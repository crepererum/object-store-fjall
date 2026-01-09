use std::path::Path;

use assert_cmd::{Command, cargo::cargo_bin_cmd};
use predicates::str::contains;
use tempfile::TempDir;

#[test]
fn help() {
    let mut cmd = cargo_bin_cmd!();
    cmd.arg("--help").assert().success();
}

#[test]
fn smoke() {
    let test_dir = TempDir::new().unwrap();
    small_config(test_dir.path()).assert().success();
}

#[test]
fn auto_create_target_dir() {
    let test_dir = TempDir::new().unwrap();
    small_config(&test_dir.path().join("sub"))
        .assert()
        .success();
}

#[test]
fn wipe_target_dir() {
    let test_dir = TempDir::new().unwrap();
    small_config(test_dir.path()).assert().success();
    small_config(test_dir.path())
        .arg("--wipe-target-dir")
        .assert()
        .success();
}

#[test]
fn reuse_target_dir_without_wipe_fails() {
    let test_dir = TempDir::new().unwrap();
    small_config(test_dir.path()).assert().success();
    small_config(test_dir.path())
        .assert()
        .failure()
        .stderr(contains("target dir not empty"));
}

#[test]
fn block_size_file_size_out_of_sync_fails() {
    let test_dir = TempDir::new().unwrap();
    let mut cmd = cargo_bin_cmd!();
    cmd.arg("--file-size")
        .arg(10.to_string())
        .arg("--block-size")
        .arg(3.to_string())
        .arg("--path")
        .arg(test_dir.path().display().to_string())
        .assert()
        .failure()
        .stderr(contains("file size must be multiple of block size"));
}

fn small_config(test_dir: &Path) -> Command {
    let mut cmd = cargo_bin_cmd!();
    cmd.arg("--block-size")
        .arg(100.to_string())
        .arg("--file-size")
        .arg(100.to_string())
        .arg("--n-files")
        .arg(1.to_string())
        .arg("--path")
        .arg(test_dir.display().to_string());
    cmd
}
