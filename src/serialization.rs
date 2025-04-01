use bincode::{Decode, Encode};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use fjall::Slice;
use object_store::{ObjectMeta, Result, path::Path};
use uuid::Uuid;

use crate::error::string_err;

const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard()
    .with_little_endian()
    .with_variable_int_encoding()
    .with_no_limit();

pub(crate) struct Head {
    pub(crate) last_modified: DateTime<Utc>,
    pub(crate) size: u64,
    pub(crate) id: Uuid,
}

impl Head {
    pub(crate) fn from_slice(s: &[u8]) -> Result<Self> {
        let (this, read) = bincode::decode_from_slice(s, BINCODE_CONFIG).map_err(|e| {
            // why do they not implement std::error::Error?!
            string_err(e.to_string())
        })?;
        if read != s.len() {
            return Err(string_err(format!("trailing bytes: {}", s.len() - read)));
        }
        Ok(this)
    }

    pub(crate) fn to_slice(&self) -> Result<Slice> {
        let data = Bytes::from(bincode::encode_to_vec(self, BINCODE_CONFIG).map_err(|e| {
            // why do they not implement std::error::Error?!
            string_err(e.to_string())
        })?);
        Ok(data.into())
    }

    pub(crate) fn into_meta(&self, path: Path) -> ObjectMeta {
        let Head {
            last_modified,
            size,
            id,
        } = self;

        ObjectMeta {
            location: path,
            last_modified: *last_modified,
            size: *size,
            e_tag: Some(id.to_string()),
            version: None,
        }
    }
}

impl Encode for Head {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> std::result::Result<(), bincode::error::EncodeError> {
        self.last_modified.timestamp().encode(encoder)?;
        self.last_modified
            .timestamp_subsec_nanos()
            .encode(encoder)?;

        self.size.encode(encoder)?;

        self.id.as_u128().encode(encoder)?;

        Ok(())
    }
}

impl<Context> Decode<Context> for Head {
    fn decode<D: bincode::de::Decoder<Context = Context>>(
        decoder: &mut D,
    ) -> std::result::Result<Self, bincode::error::DecodeError> {
        Ok(Self {
            last_modified: DateTime::from_timestamp(
                Decode::decode(decoder)?,
                Decode::decode(decoder)?,
            )
            .ok_or(bincode::error::DecodeError::Other("invalid timestamp"))?,
            size: Decode::decode(decoder)?,
            id: Uuid::from_u128(Decode::decode(decoder)?),
        })
    }
}
