use secrecy::{ExposeSecret, SecretString};

/// Supplies a bearer token for an outbound MCP HTTP request.
///
/// Implementations may return a fresh token on every call.
pub trait HttpAuthProvider: Send + Sync {
    /// Returns the current bearer token, or `None` for an anonymous request.
    fn bearer_token(&self) -> Option<SecretString>;
}

/// Authorizes one inbound MCP HTTP bearer token.
pub trait HttpAuthorizer: Send + Sync {
    /// Returns whether the supplied bearer token is accepted.
    fn authorize(&self, bearer_token: Option<&str>) -> bool;
}

/// Static bearer credentials usable by both clients and servers.
pub struct StaticBearerAuth {
    token: SecretString,
}

impl StaticBearerAuth {
    /// Creates static bearer credentials.
    pub fn new(token: SecretString) -> Self {
        Self { token }
    }
}

impl HttpAuthProvider for StaticBearerAuth {
    fn bearer_token(&self) -> Option<SecretString> {
        Some(self.token.clone())
    }
}

impl HttpAuthorizer for StaticBearerAuth {
    fn authorize(&self, bearer_token: Option<&str>) -> bool {
        bearer_token.is_some_and(|candidate| {
            constant_time_equal(candidate.as_bytes(), self.token.expose_secret().as_bytes())
        })
    }
}

impl std::fmt::Debug for StaticBearerAuth {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StaticBearerAuth")
            .field("token", &"[REDACTED]")
            .finish()
    }
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    let maximum = left.len().max(right.len());
    for index in 0..maximum {
        let left_byte = left.get(index).copied().unwrap_or_default();
        let right_byte = right.get(index).copied().unwrap_or_default();
        difference |= usize::from(left_byte ^ right_byte);
    }
    difference == 0
}

#[cfg(test)]
mod tests {
    use secrecy::SecretString;

    use super::{HttpAuthorizer, StaticBearerAuth};

    #[test]
    fn static_token_is_redacted_and_exact() {
        let auth = StaticBearerAuth::new(SecretString::from("sensitive".to_owned()));
        assert!(auth.authorize(Some("sensitive")));
        assert!(!auth.authorize(Some("sensitive2")));
        assert!(!format!("{auth:?}").contains("sensitive"));
    }
}
