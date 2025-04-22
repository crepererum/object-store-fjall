use bincode::{Decode, Encode};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use fjall::Slice;
use object_store::{Attribute, AttributeValue, Attributes, ObjectMeta, Result, path::Path};
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

    pub(crate) fn object_meta(&self, path: Path) -> ObjectMeta {
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

pub(crate) struct WrappedAttributes(Attributes);

impl WrappedAttributes {
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

    fn encode_key<E: bincode::enc::Encoder>(
        key: &Attribute,
        encoder: &mut E,
    ) -> std::result::Result<(), bincode::error::EncodeError> {
        match key {
            Attribute::ContentDisposition => 0u8.encode(encoder),
            Attribute::ContentEncoding => 1u8.encode(encoder),
            Attribute::ContentLanguage => 2u8.encode(encoder),
            Attribute::ContentType => 3u8.encode(encoder),
            Attribute::CacheControl => 4u8.encode(encoder),
            Attribute::Metadata(cow) => {
                5u8.encode(encoder)?;
                cow.as_bytes().encode(encoder)
            }
            _ => Err(bincode::error::EncodeError::OtherString(format!(
                "cannot encode attribute: {key:?}"
            ))),
        }
    }

    fn decode_key<Context, D: bincode::de::Decoder<Context = Context>>(
        decoder: &mut D,
    ) -> std::result::Result<Attribute, bincode::error::DecodeError> {
        match u8::decode(decoder)? {
            0 => Ok(Attribute::ContentDisposition),
            1 => Ok(Attribute::ContentEncoding),
            2 => Ok(Attribute::ContentLanguage),
            3 => Ok(Attribute::ContentType),
            4 => Ok(Attribute::CacheControl),
            5 => Ok(Attribute::Metadata(String::decode(decoder)?.into())),
            o => Err(bincode::error::DecodeError::OtherString(format!(
                "cannot decode attribute: {o:?}"
            ))),
        }
    }

    fn encode_value<E: bincode::enc::Encoder>(
        value: &AttributeValue,
        encoder: &mut E,
    ) -> std::result::Result<(), bincode::error::EncodeError> {
        value.as_bytes().encode(encoder)
    }

    fn decode_value<Context, D: bincode::de::Decoder<Context = Context>>(
        decoder: &mut D,
    ) -> std::result::Result<AttributeValue, bincode::error::DecodeError> {
        Ok(String::decode(decoder)?.into())
    }
}

impl From<Attributes> for WrappedAttributes {
    fn from(attrs: Attributes) -> Self {
        Self(attrs)
    }
}

impl From<WrappedAttributes> for Attributes {
    fn from(attrs: WrappedAttributes) -> Self {
        attrs.0
    }
}

impl Encode for WrappedAttributes {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> std::result::Result<(), bincode::error::EncodeError> {
        (self.0.len() as u64).encode(encoder)?;

        for (k, v) in &self.0 {
            Self::encode_key(k, encoder)?;
            Self::encode_value(v, encoder)?;
        }

        Ok(())
    }
}

impl<Context> Decode<Context> for WrappedAttributes {
    fn decode<D: bincode::de::Decoder<Context = Context>>(
        decoder: &mut D,
    ) -> std::result::Result<Self, bincode::error::DecodeError> {
        let len = u64::decode(decoder)? as usize;
        let mut attrs = Attributes::with_capacity(len);

        for _ in 0..len {
            let k = Self::decode_key(decoder)?;
            let v = Self::decode_value(decoder)?;
            attrs.insert(k, v);
        }

        Ok(Self(attrs))
    }
}
