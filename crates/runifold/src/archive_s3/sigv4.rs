//! Native AWS Signature Version 4 pre-signing for S3-compatible endpoints.

use std::{
    collections::BTreeMap,
    fmt::Write as _,
    num::NonZeroU32,
    time::{SystemTime, UNIX_EPOCH},
};

use hmac::{Hmac, Mac};
use reqwest::{Url, header::HeaderMap};
use secrecy::{ExposeSecret, SecretString};
use sha2::{Digest, Sha256};
use time::{OffsetDateTime, format_description};

use super::{
    S3ArchivePresignFuture, S3ArchivePresignRequest, S3ArchivePresignedObject, S3ArchivePresigner,
    archive_error, config_error,
};

type HmacSha256 = Hmac<Sha256>;

/// Temporary or static `SigV4` credentials.
#[derive(Clone)]
pub struct S3SigV4Credentials {
    access_key_id: String,
    secret_access_key: SecretString,
    session_token: Option<SecretString>,
}

impl S3SigV4Credentials {
    /// Creates validated credentials, including an optional temporary token.
    ///
    /// # Errors
    ///
    /// Rejects blank or control-character-bearing credential fields.
    pub fn new(
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        session_token: Option<String>,
    ) -> Result<Self, runifold_workflow::WorkflowTaskTombstoneArchiveError> {
        let access_key_id = access_key_id.into();
        let secret_access_key = secret_access_key.into();
        if !credential_field_is_valid(&access_key_id, 256)
            || !credential_field_is_valid(&secret_access_key, 4_096)
            || session_token
                .as_deref()
                .is_some_and(|value| !credential_field_is_valid(value, 8_192))
        {
            return Err(config_error("S3 SigV4 credentials are invalid"));
        }
        Ok(Self {
            access_key_id,
            secret_access_key: SecretString::from(secret_access_key),
            session_token: session_token.map(SecretString::from),
        })
    }
}

impl std::fmt::Debug for S3SigV4Credentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("S3SigV4Credentials")
            .field("access_key_id", &"<redacted>")
            .field("secret_access_key", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// Validated `SigV4` endpoint and authority duration.
#[derive(Clone, Debug)]
pub struct S3SigV4PresignerConfig {
    endpoint: Url,
    region: String,
    expires_seconds: NonZeroU32,
    path_style: bool,
}

impl S3SigV4PresignerConfig {
    /// Creates a signer configuration.
    ///
    /// `path_style` should be enabled for most `MinIO` and custom endpoints.
    ///
    /// # Errors
    ///
    /// Rejects endpoints with credentials/query/fragment, blank regions, or
    /// expiration outside S3's `1..=604800` second bound.
    pub fn new(
        endpoint: Url,
        region: impl Into<String>,
        expires_seconds: u32,
        path_style: bool,
    ) -> Result<Self, runifold_workflow::WorkflowTaskTombstoneArchiveError> {
        let region = region.into();
        let expires_seconds = NonZeroU32::new(expires_seconds)
            .filter(|value| value.get() <= 604_800)
            .ok_or_else(|| config_error("S3 presign expiration must be in 1..=604800 seconds"))?;
        if !matches!(endpoint.scheme(), "http" | "https")
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || region.trim().is_empty()
            || region.len() > 128
            || region.chars().any(char::is_control)
        {
            return Err(config_error("S3 SigV4 endpoint or region is invalid"));
        }
        Ok(Self {
            endpoint,
            region,
            expires_seconds,
            path_style,
        })
    }
}

/// Native `SigV4` pre-signer for AWS S3 and compatible object stores.
#[derive(Clone, Debug)]
pub struct S3SigV4Presigner {
    config: S3SigV4PresignerConfig,
    credentials: S3SigV4Credentials,
}

impl S3SigV4Presigner {
    /// Creates a signer from validated endpoint policy and credentials.
    pub const fn new(config: S3SigV4PresignerConfig, credentials: S3SigV4Credentials) -> Self {
        Self {
            config,
            credentials,
        }
    }

    fn sign(
        &self,
        request: &S3ArchivePresignRequest,
    ) -> Result<S3ArchivePresignedObject, runifold_workflow::WorkflowTaskTombstoneArchiveError>
    {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| archive_error("system clock predates Unix epoch"))?
            .as_secs();
        let now = i64::try_from(now).map_err(|_| archive_error("system clock overflowed"))?;
        let now = OffsetDateTime::from_unix_timestamp(now)
            .map_err(|_| archive_error("system clock is outside SigV4 range"))?;
        Ok(S3ArchivePresignedObject {
            put_url: self.presign_at("PUT", request, &request.required_put_headers, now)?,
            head_url: self.presign_at("HEAD", request, &HeaderMap::new(), now)?,
            put_headers: HeaderMap::new(),
            head_headers: HeaderMap::new(),
        })
    }

    fn presign_at(
        &self,
        method: &str,
        request: &S3ArchivePresignRequest,
        headers: &HeaderMap,
        now: OffsetDateTime,
    ) -> Result<Url, runifold_workflow::WorkflowTaskTombstoneArchiveError> {
        let date = now
            .format(
                &format_description::parse("[year][month][day]")
                    .map_err(|_| archive_error("SigV4 date format is invalid"))?,
            )
            .map_err(|_| archive_error("SigV4 date formatting failed"))?;
        let timestamp = now
            .format(
                &format_description::parse("[year][month][day]T[hour][minute][second]Z")
                    .map_err(|_| archive_error("SigV4 timestamp format is invalid"))?,
            )
            .map_err(|_| archive_error("SigV4 timestamp formatting failed"))?;
        let url = object_url(
            &self.config.endpoint,
            &request.bucket,
            &request.key,
            self.config.path_style,
        )?;
        let host = host_header(&url)?;
        let (canonical_headers, signed_headers) = canonical_headers(headers, &host)?;
        let scope = format!("{date}/{}/s3/aws4_request", self.config.region);
        let mut query = vec![
            ("X-Amz-Algorithm".to_owned(), "AWS4-HMAC-SHA256".to_owned()),
            (
                "X-Amz-Credential".to_owned(),
                format!("{}/{}", self.credentials.access_key_id, scope),
            ),
            ("X-Amz-Date".to_owned(), timestamp.clone()),
            (
                "X-Amz-Expires".to_owned(),
                self.config.expires_seconds.get().to_string(),
            ),
            ("X-Amz-SignedHeaders".to_owned(), signed_headers.clone()),
        ];
        if let Some(token) = &self.credentials.session_token {
            query.push((
                "X-Amz-Security-Token".to_owned(),
                token.expose_secret().to_owned(),
            ));
        }
        let unsigned_query = canonical_query(&query);
        let canonical_request = format!(
            "{method}\n{}\n{unsigned_query}\n{canonical_headers}\n{signed_headers}\nUNSIGNED-PAYLOAD",
            url.path()
        );
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{}",
            hex_digest(canonical_request.as_bytes())
        );
        let signing_key = signing_key(
            self.credentials.secret_access_key.expose_secret(),
            &date,
            &self.config.region,
        );
        let signature = hex_bytes(&hmac(&signing_key, string_to_sign.as_bytes()));
        query.push(("X-Amz-Signature".to_owned(), signature));
        let mut signed = url;
        signed.set_query(Some(&canonical_query(&query)));
        Ok(signed)
    }
}

impl S3ArchivePresigner for S3SigV4Presigner {
    fn presign(&self, request: S3ArchivePresignRequest) -> S3ArchivePresignFuture<'_> {
        let result = self.sign(&request);
        Box::pin(async move { result })
    }
}

fn object_url(
    endpoint: &Url,
    bucket: &str,
    key: &str,
    path_style: bool,
) -> Result<Url, runifold_workflow::WorkflowTaskTombstoneArchiveError> {
    let encoded_key = key.split('/').map(aws_encode).collect::<Vec<_>>().join("/");
    let mut url = endpoint.clone();
    let base_path = endpoint.path().trim_end_matches('/');
    if path_style {
        url.set_path(&format!("{base_path}/{}/{encoded_key}", aws_encode(bucket)));
    } else {
        let host = endpoint
            .host_str()
            .ok_or_else(|| config_error("S3 endpoint host is missing"))?;
        url.set_host(Some(&format!("{bucket}.{host}")))
            .map_err(|_| config_error("S3 virtual-host endpoint is invalid"))?;
        url.set_path(&format!("{base_path}/{encoded_key}"));
    }
    Ok(url)
}

fn host_header(url: &Url) -> Result<String, runifold_workflow::WorkflowTaskTombstoneArchiveError> {
    let host = url
        .host_str()
        .ok_or_else(|| config_error("S3 endpoint host is missing"))?;
    Ok(match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_owned(),
    })
}

fn canonical_headers(
    headers: &HeaderMap,
    host: &str,
) -> Result<(String, String), runifold_workflow::WorkflowTaskTombstoneArchiveError> {
    let mut values = BTreeMap::from([("host".to_owned(), host.to_owned())]);
    for (name, value) in headers {
        let value = value
            .to_str()
            .map_err(|_| config_error("S3 signed header is not visible ASCII"))?
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        values.insert(name.as_str().to_ascii_lowercase(), value);
    }
    let signed = values.keys().cloned().collect::<Vec<_>>().join(";");
    let canonical = values
        .into_iter()
        .fold(String::new(), |mut output, (name, value)| {
            writeln!(output, "{name}:{value}").expect("writing to String cannot fail");
            output
        });
    Ok((canonical, signed))
}

fn canonical_query(values: &[(String, String)]) -> String {
    let mut values = values
        .iter()
        .map(|(name, value)| (aws_encode(name), aws_encode(value)))
        .collect::<Vec<_>>();
    values.sort();
    values
        .into_iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn aws_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(*byte));
        } else {
            write!(encoded, "%{byte:02X}").expect("writing to String cannot fail");
        }
    }
    encoded
}

fn signing_key(secret: &str, date: &str, region: &str) -> Vec<u8> {
    let date_key = hmac(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let region_key = hmac(&date_key, region.as_bytes());
    let service_key = hmac(&region_key, b"s3");
    hmac(&service_key, b"aws4_request")
}

fn hmac(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key)
        .expect("HMAC-SHA256 accepts every key length by construction");
    mac.update(value);
    mac.finalize().into_bytes().to_vec()
}

fn hex_digest(value: &[u8]) -> String {
    hex_bytes(&Sha256::digest(value))
}

fn hex_bytes(value: &[u8]) -> String {
    value.iter().fold(String::new(), |mut output, byte| {
        write!(output, "{byte:02x}").expect("writing to String cannot fail");
        output
    })
}

fn credential_field_is_valid(value: &str, maximum: usize) -> bool {
    !value.trim().is_empty() && value.len() <= maximum && !value.chars().any(char::is_control)
}

#[cfg(test)]
mod tests {
    use reqwest::header::HeaderValue;

    use super::*;

    #[test]
    fn signs_path_style_put_and_head_without_debug_secret_disclosure() {
        let credentials =
            S3SigV4Credentials::new("ACCESS", "SECRET", Some("TOKEN".into())).unwrap();
        assert!(!format!("{credentials:?}").contains("SECRET"));
        let signer = S3SigV4Presigner::new(
            S3SigV4PresignerConfig::new(
                Url::parse("http://127.0.0.1:9000").unwrap(),
                "us-east-1",
                60,
                true,
            )
            .unwrap(),
            credentials,
        );
        let request = S3ArchivePresignRequest {
            bucket: "archive-bucket".into(),
            key: "tenant/a b.json".into(),
            required_put_headers: HeaderMap::from_iter([(
                "if-none-match".parse().unwrap(),
                HeaderValue::from_static("*"),
            )]),
        };
        let now = OffsetDateTime::from_unix_timestamp(1_704_067_200).unwrap();
        let put = signer
            .presign_at("PUT", &request, &request.required_put_headers, now)
            .unwrap();
        assert_eq!(put.path(), "/archive-bucket/tenant/a%20b.json");
        assert!(put.query().unwrap().contains("X-Amz-Signature="));
        assert!(put.query().unwrap().contains("host%3Bif-none-match"));
        assert!(
            !format!(
                "{:?}",
                S3ArchivePresignedObject {
                    put_url: put.clone(),
                    head_url: put,
                    put_headers: HeaderMap::new(),
                    head_headers: HeaderMap::new(),
                }
            )
            .contains("TOKEN")
        );
    }
}
