use std::{
    collections::BTreeMap,
    io::ErrorKind,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use bytes::Bytes;
use clap::Parser;
use futures::{StreamExt, TryStreamExt};
use indicatif::ProgressIterator;
use object_store::{DynObjectStore, ObjectStoreExt, PutPayload, path::Path as ObjectStorePath};
use rand::{RngExt, seq::IndexedRandom};
use serde::Serialize;
use serde_with::{DurationSecondsWithFrac, serde_as};
use uuid::Uuid;

#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Perform object store disk test, comparing multiple implementations.
#[derive(Debug, Parser)]
struct Args {
    /// Path to test directory.
    #[clap(long)]
    path: PathBuf,

    /// Wipe target dir.
    #[clap(long)]
    wipe_target_dir: bool,

    /// Output file
    #[clap(long)]
    out: Option<PathBuf>,

    /// Config
    #[clap(flatten)]
    config: Config,
}

#[derive(Debug, Parser, Serialize)]
struct Config {
    /// Number of files.
    #[clap(long, default_value_t = 100)]
    n_files: usize,

    /// File size in bytes.
    #[clap(long, default_value_t = 96 * 1024 * 1024)]
    file_size: usize,

    /// Block size in bytes.
    #[clap(long, default_value_t = 16 * 1024 * 1024)]
    block_size: usize,

    /// Number of random files to be read, with potential repetition.
    #[clap(long, default_value_t = 10)]
    read_n_files: usize,

    /// Number of times we read data.
    #[clap(long, default_value_t = 1)]
    read_rounds: usize,
}

#[serde_as]
#[derive(Debug, Serialize)]
struct OutWrite {
    #[serde_as(as = "DurationSecondsWithFrac<f64>")]
    duration: Duration,
}

#[derive(Debug, Serialize)]
struct OutState {
    dir_size: u64,
}

#[serde_as]
#[derive(Debug, Serialize)]
struct OutReadRound {
    #[serde_as(as = "DurationSecondsWithFrac<f64>")]
    duration: Duration,
}

#[derive(Debug, Serialize)]
struct Output {
    config: Config,
    write: BTreeMap<&'static str, OutWrite>,
    state: BTreeMap<&'static str, OutState>,
    read: BTreeMap<&'static str, Vec<OutReadRound>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(
        args.config.file_size % args.config.block_size == 0,
        "file size must be multiple of block size"
    );

    println!("config:");
    for line in serde_yaml::to_string(&args.config)
        .context("serialize config")?
        .lines()
    {
        println!("  {line}");
    }

    if args.wipe_target_dir {
        tokio::fs::remove_dir_all(&args.path)
            .await
            .context("wipe target dir")?;
    } else {
        match tokio::fs::read_dir(&args.path).await {
            Ok(mut read_dir) => {
                let is_empty = read_dir
                    .next_entry()
                    .await
                    .context("read target dir")?
                    .is_none();
                ensure!(is_empty, "target dir not empty");
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) => {
                return Err(e).context("read target dir");
            }
        }
    }
    tokio::fs::create_dir_all(&args.path)
        .await
        .context("create target dir")?;

    let implementations = implementations();

    println!("set up stores...");
    let stores = futures::stream::iter(implementations)
        .then(async |(name, implementation)| {
            println!("  {name}...");
            let path = args.path.join(name);
            tokio::fs::create_dir(&path)
                .await
                .context("create store dir")?;
            let store = implementation
                .setup(path.clone())
                .await
                .with_context(|| format!("set up {name}"))?;
            let store = Store { store, path };

            Result::<_, anyhow::Error>::Ok((name, store))
        })
        .try_collect::<BTreeMap<_, _>>()
        .await
        .context("set up stores")?;

    println!("prime stores...");
    let n_blocks = args.config.file_size / args.config.block_size;
    let mut paths = Vec::with_capacity(args.config.n_files);
    let mut times = stores
        .keys()
        .map(|k| {
            (
                *k,
                OutWrite {
                    duration: Duration::ZERO,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    for _ in (0..args.config.n_files).progress() {
        let path = gen_path();
        let payload = (0..n_blocks)
            .map(|_| random_bytes(args.config.block_size))
            .collect::<PutPayload>();

        for (name, store) in &stores {
            let t_start = Instant::now();
            store
                .store
                .put(&path, payload.clone())
                .await
                .with_context(|| format!("PUT file in {name}"))?;
            let elapsed = t_start.elapsed();
            times.get_mut(name).unwrap().duration += elapsed;
        }

        paths.push(path);
    }
    for (name, out) in &times {
        println!("  {name}: {}s", out.duration.as_secs_f32());
    }

    println!("store sizes:");
    let mut sizes = BTreeMap::new();
    for (name, store) in &stores {
        let path = store.path.clone();
        let size = tokio::task::spawn_blocking(move || dir_size::get_size_in_bytes(&path))
            .await
            .context("spawn blocking")?
            .context("get dir size")?;
        println!("  {name}: {size}");
        sizes.insert(*name, OutState { dir_size: size });
    }

    println!("read files...");
    let mut read_rounds = BTreeMap::<_, Vec<_>>::new();
    for round in 1..=args.config.read_rounds {
        println!("  round {}/{}", round, args.config.read_rounds);
        let random_paths = choose_non_unique(&paths, args.config.read_n_files);
        for (name, store) in &stores {
            let t_start = Instant::now();
            for path in random_paths.iter().progress() {
                let get_res = store
                    .store
                    .get(path)
                    .await
                    .with_context(|| format!("read from store {name}"))?;

                if matches!(get_res.payload, object_store::GetResultPayload::File(_, _)) {
                    // read in one big chunk
                    get_res
                        .bytes()
                        .await
                        .with_context(|| format!("read from store {name}"))?;
                } else {
                    // use streaming
                    let mut stream = get_res.into_stream();

                    while stream
                        .try_next()
                        .await
                        .with_context(|| format!("read from store {name}"))?
                        .is_some()
                    {}
                }
            }
            let duration = t_start.elapsed();
            println!("    {name}: {}s", duration.as_secs_f32());
            read_rounds
                .entry(*name)
                .or_default()
                .push(OutReadRound { duration });
        }
    }

    let out = Output {
        config: args.config,
        write: times,
        state: sizes,
        read: read_rounds,
    };
    if let Some(f) = args.out {
        tokio::fs::write(f, serde_yaml::to_string(&out).context("serialize output")?)
            .await
            .context("write output")?;
    }

    Ok(())
}

fn implementations() -> BTreeMap<&'static str, Box<dyn Implementation>> {
    let mut map = BTreeMap::new();

    let mut register = |implementation: Box<dyn Implementation>| {
        let existed = map.insert(implementation.name(), implementation).is_some();
        assert!(!existed);
    };

    register(Box::new(LocalImpl));
    register(Box::new(FjallImpl));

    map
}

#[async_trait]
trait Implementation {
    fn name(&self) -> &'static str;

    async fn setup(&self, path: PathBuf) -> Result<Arc<DynObjectStore>>;
}

struct LocalImpl;

#[async_trait]
impl Implementation for LocalImpl {
    fn name(&self) -> &'static str {
        "local"
    }

    async fn setup(&self, path: PathBuf) -> Result<Arc<DynObjectStore>> {
        Ok(Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(path)
                .context("create local file system store")?,
        ))
    }
}

struct FjallImpl;

#[async_trait]
impl Implementation for FjallImpl {
    fn name(&self) -> &'static str {
        "fjall"
    }

    async fn setup(&self, path: PathBuf) -> Result<Arc<DynObjectStore>> {
        Ok(Arc::new(
            object_store_fjall::FjallStore::open(path)
                .await
                .context("create local file system store")?,
        ))
    }
}

fn gen_path() -> ObjectStorePath {
    let mut rng = rand::rng();

    let ns = rng.random_range(0..3u64);
    let table = rng.random_range(0..3u64);
    let part = "ebd1041daa7c644c99967b817ae607bdcb754c663f2c415f270d6df720280f7a";
    let id = Uuid::now_v7();

    ObjectStorePath::parse(format!("{ns}/{table}/{part}/{id}.parquet")).expect("path is valid")
}

struct Store {
    store: Arc<DynObjectStore>,
    path: PathBuf,
}

fn random_bytes(n: usize) -> Bytes {
    rand::random_iter::<u8>().take(n).collect()
}

fn choose_non_unique<T>(a: &[T], n: usize) -> Vec<&T> {
    let mut rng = rand::rng();
    (0..n)
        .map(|_| a.choose(&mut rng).expect("slice must not be empty"))
        .collect()
}
