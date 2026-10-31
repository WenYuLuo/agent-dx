//! E2B SDK v1 file signatures at the gateway trust boundary.
use super::e2b_route::{constant_time_eq, E2bAuthorizationError};
use base64::{engine::general_purpose::STANDARD_NO_PAD, Engine};
use http::{Method, Request};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, time::SystemTime};

pub(super) fn authorize_file_url<B>(
    request: &Request<B>,
    signing_key: &str,
) -> Result<(), E2bAuthorizationError> {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| E2bAuthorizationError::Unavailable)?
        .as_secs();
    verify(request, signing_key, now)
}

fn verify<B>(request: &Request<B>, key: &str, now: u64) -> Result<(), E2bAuthorizationError> {
    let denied = E2bAuthorizationError::Unauthorized;
    if request.uri().path() != "/files" {
        return Err(denied);
    }
    let operation = match *request.method() {
        Method::GET => "read",
        Method::POST => "write",
        _ => return Err(denied),
    };
    let mut query = BTreeMap::new();
    for (name, value) in url::form_urlencoded::parse(request.uri().query().unwrap_or("").as_bytes())
    {
        if !matches!(
            name.as_ref(),
            "path" | "username" | "signature" | "signature_expiration"
        ) || query
            .insert(name.into_owned(), value.into_owned())
            .is_some()
        {
            return Err(denied);
        }
    }
    let path = query.get("path").ok_or(denied)?;
    let signature = query.get("signature").ok_or(denied)?;
    let username = query.get("username").map(String::as_str).unwrap_or("");
    let mut message = format!("{path}:{operation}:{username}:{key}");
    if let Some(expiration) = query.get("signature_expiration") {
        let expiration = expiration.parse::<u64>().map_err(|_| denied)?;
        if expiration < now || i64::try_from(expiration).is_err() {
            return Err(denied);
        }
        message.push(':');
        message.push_str(&expiration.to_string());
    }
    let expected = format!(
        "v1_{}",
        STANDARD_NO_PAD.encode(Sha256::digest(message.as_bytes()))
    );
    if !constant_time_eq(signature.as_bytes(), expected.as_bytes()) {
        return Err(denied);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "sdk-signing-key-0123456789abcdef0123456789";

    fn signed(method: Method, path: &str, user: &str, expiry: Option<&str>) -> Request<()> {
        let operation = if method == Method::GET {
            "read"
        } else {
            "write"
        };
        let mut message = format!("{path}:{operation}:{user}:{KEY}");
        if let Some(expiry) = expiry {
            message.push(':');
            message.push_str(expiry);
        }
        let signature = format!(
            "v1_{}",
            STANDARD_NO_PAD.encode(Sha256::digest(message.as_bytes()))
        );
        let mut query = url::form_urlencoded::Serializer::new(String::new());
        query.append_pair("path", path);
        query.append_pair("username", user);
        query.append_pair("signature", &signature);
        if let Some(expiry) = expiry {
            query.append_pair("signature_expiration", expiry);
        }
        Request::builder()
            .method(method)
            .uri(format!("/files?{}", query.finish()))
            .body(())
            .unwrap()
    }

    #[test]
    fn sdk_read_and_write_signatures_bind_path_user_and_expiration() {
        for method in [Method::GET, Method::POST] {
            for expiry in [None, Some("100")] {
                let request = signed(method.clone(), "/tmp/文件 a+b.txt", "root", expiry);
                assert!(verify(&request, KEY, 100).is_ok());
                assert_eq!(
                    verify(&request, "another-sandbox-token", 100),
                    Err(E2bAuthorizationError::Unauthorized)
                );
                if expiry.is_some() {
                    assert!(verify(&request, KEY, 101).is_err());
                }
            }
        }
    }

    #[test]
    fn signature_rejects_tampering_duplicates_and_other_endpoints() {
        let request = signed(Method::GET, "/tmp/original", "root", Some("100"));
        for uri in [
            request.uri().to_string().replace("original", "other"),
            request
                .uri()
                .to_string()
                .replace("username=root", "username=user"),
            format!("{}&path=/tmp/other", request.uri()),
            format!("{}&signature=another", request.uri()),
            request
                .uri()
                .to_string()
                .replace("signature_expiration=100", "signature_expiration=101"),
            request.uri().to_string().replace("/files?", "/init?"),
        ] {
            let altered = Request::builder().uri(uri).body(()).unwrap();
            assert!(verify(&altered, KEY, 100).is_err());
        }
        let upload = Request::builder()
            .method(Method::POST)
            .uri(request.uri())
            .body(())
            .unwrap();
        assert!(verify(&upload, KEY, 100).is_err());
    }
}
