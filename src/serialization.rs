use std::{borrow::Cow, marker::PhantomData};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use fjall::Slice;
use object_store::{Attribute, AttributeValue, Attributes, ObjectMeta, Result, path::Path};
use serde::{
    Deserialize, Serialize,
    de::{MapAccess, Visitor},
    ser::{Error, SerializeMap},
};
use uuid::Uuid;

use crate::error::{GenericResultExt, string_err};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Head {
    pub(crate) last_modified: DateTime<Utc>,
    pub(crate) size: u64,
    pub(crate) id: Uuid,
}

impl Head {
    pub(crate) fn from_slice(s: &[u8]) -> Result<Self> {
        let (this, remaining) = postcard::take_from_bytes(s).generic_err()?;
        if !remaining.is_empty() {
            return Err(string_err(format!("trailing bytes: {}", remaining.len())));
        }
        Ok(this)
    }

    pub(crate) fn to_slice(&self) -> Result<Slice> {
        let data = Bytes::from(postcard::to_stdvec(self).generic_err()?);
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

pub(crate) struct WrappedAttributes(Attributes);

impl WrappedAttributes {
    pub(crate) fn from_slice(s: &[u8]) -> Result<Self> {
        let (this, remaining) = postcard::take_from_bytes(s).generic_err()?;
        if !remaining.is_empty() {
            return Err(string_err(format!("trailing bytes: {}", remaining.len())));
        }
        Ok(this)
    }

    pub(crate) fn to_slice(&self) -> Result<Slice> {
        let data = Bytes::from(postcard::to_stdvec(self).generic_err()?);
        Ok(data.into())
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

impl Serialize for WrappedAttributes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;

        for (k, v) in &self.0 {
            map.serialize_entry(
                &WrappedAttribute::try_from(k).map_err(S::Error::custom)?,
                &WrappedAttributeValue::from(v),
            )?;
        }

        map.end()
    }
}

impl<'de> Deserialize<'de> for WrappedAttributes {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(AttributesVisitor::new())
    }
}

struct AttributesVisitor {
    marker: PhantomData<fn() -> Attributes>,
}

impl AttributesVisitor {
    fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<'de> Visitor<'de> for AttributesVisitor {
    type Value = WrappedAttributes;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("Attributes")
    }

    fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut attrs = Attributes::with_capacity(access.size_hint().unwrap_or(0));

        while let Some((key, value)) =
            access.next_entry::<WrappedAttribute, WrappedAttributeValue>()?
        {
            attrs.insert(key.into(), value.into());
        }

        Ok(WrappedAttributes(attrs))
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum WrappedAttribute<'a> {
    ContentDisposition,
    ContentEncoding,
    ContentLanguage,
    ContentType,
    CacheControl,
    Metadata(Cow<'a, str>),
}

impl<'a> TryFrom<&'a Attribute> for WrappedAttribute<'a> {
    type Error = String;

    fn try_from(attr: &'a Attribute) -> Result<Self, Self::Error> {
        match attr {
            Attribute::ContentDisposition => Ok(Self::ContentDisposition),
            Attribute::ContentEncoding => Ok(Self::ContentEncoding),
            Attribute::ContentLanguage => Ok(Self::ContentLanguage),
            Attribute::ContentType => Ok(Self::ContentType),
            Attribute::CacheControl => Ok(Self::CacheControl),
            Attribute::Metadata(md) => Ok(Self::Metadata(md.as_ref().into())),
            other => Err(format!("cannot encode attribute {other:?}")),
        }
    }
}

impl<'a> From<WrappedAttribute<'a>> for Attribute {
    fn from(attr: WrappedAttribute<'a>) -> Self {
        match attr {
            WrappedAttribute::ContentDisposition => Self::ContentDisposition,
            WrappedAttribute::ContentEncoding => Self::ContentEncoding,
            WrappedAttribute::ContentLanguage => Self::ContentLanguage,
            WrappedAttribute::ContentType => Self::ContentType,
            WrappedAttribute::CacheControl => Self::CacheControl,
            WrappedAttribute::Metadata(md) => Self::Metadata(md.into_owned().into()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct WrappedAttributeValue<'a>(Cow<'a, str>);

impl<'a> From<&'a AttributeValue> for WrappedAttributeValue<'a> {
    fn from(value: &'a AttributeValue) -> Self {
        Self(value.as_ref().into())
    }
}

impl<'a> From<WrappedAttributeValue<'a>> for AttributeValue {
    fn from(value: WrappedAttributeValue<'a>) -> Self {
        value.0.into_owned().into()
    }
}
