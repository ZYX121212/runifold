use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};

use crate::McpError;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Collection {
    Tools,
    Resources,
    ResourceTemplates,
    Prompts,
}

#[derive(Debug, Deserialize, Serialize)]
struct Cursor {
    session: String,
    collection: Collection,
    offset: usize,
}

pub(crate) fn page<T>(
    values: Vec<T>,
    requested: Option<&str>,
    session: &str,
    collection: Collection,
    page_size: usize,
) -> Result<(Vec<T>, Option<String>), McpError> {
    let offset = match requested {
        Some(cursor) => decode(cursor, session, collection)?,
        None => 0,
    };
    if offset > values.len() {
        return Err(McpError::protocol("pagination cursor is out of range"));
    }
    let end = offset.saturating_add(page_size).min(values.len());
    let next = (end < values.len()).then(|| {
        encode(&Cursor {
            session: session.to_owned(),
            collection,
            offset: end,
        })
    });
    Ok((
        values.into_iter().skip(offset).take(page_size).collect(),
        next,
    ))
}

fn encode(cursor: &Cursor) -> String {
    URL_SAFE_NO_PAD.encode(
        serde_json::to_vec(cursor).expect("internal pagination cursor must always serialize"),
    )
}

fn decode(encoded: &str, session: &str, collection: Collection) -> Result<usize, McpError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| McpError::protocol("pagination cursor is invalid"))?;
    let cursor: Cursor = serde_json::from_slice(&bytes)
        .map_err(|_| McpError::protocol("pagination cursor is invalid"))?;
    if cursor.session != session || cursor.collection != collection {
        return Err(McpError::protocol(
            "pagination cursor does not belong to this session and collection",
        ));
    }
    Ok(cursor.offset)
}
