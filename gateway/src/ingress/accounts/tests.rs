use super::*;
use http::{HeaderMap, HeaderValue};

#[test]
fn bearer_credentials_are_unambiguous_and_not_query_parameters() {
    let mut headers = HeaderMap::new();
    assert!(bearer(&headers).is_err());
    headers.insert("authorization", HeaderValue::from_static("Bearer abc"));
    assert_eq!(bearer(&headers).unwrap(), "abc");
    headers.append("authorization", HeaderValue::from_static("Bearer def"));
    assert!(bearer(&headers).is_err());
    headers.remove("authorization");
    headers.insert("authorization", HeaderValue::from_static("Basic abc"));
    assert!(bearer(&headers).is_err());
}

#[test]
fn login_request_does_not_accept_client_identity_or_secrets() {
    let request: LoginRequest =
        serde_json::from_str(r#"{"authorizationCode":"code","agreementVersion":"1"}"#).unwrap();
    assert_eq!(request.authorization_code, "code");
    for field in [
        "userId",
        "clientSecret",
        "clientId",
        "apiKey",
        "tenant",
        "displayName",
        "nickname",
    ] {
        let value = serde_json::json!({"authorizationCode":"code", field:"forged"});
        assert!(serde_json::from_value::<LoginRequest>(value).is_err());
    }
}

#[test]
fn agc_login_request_accepts_only_the_user_access_token_and_agreement() {
    let request: AgcLoginRequest = serde_json::from_str(
        r#"{"agcAccessToken":"header.payload.signature","agreementVersion":"1"}"#,
    )
    .unwrap();
    assert_eq!(request.agc_access_token, "header.payload.signature");
    for field in ["uid", "userId", "phone", "clientId", "projectId", "tenant"] {
        let value = serde_json::json!({
            "agcAccessToken": "header.payload.signature",
            "agreementVersion": "1",
            field: "forged",
        });
        assert!(serde_json::from_value::<AgcLoginRequest>(value).is_err());
    }
}

#[test]
fn agc_error_has_the_client_contract_code() {
    assert_eq!(
        Error::InvalidAgcAccessToken.status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        Error::InvalidAgcAccessToken.code(),
        "INVALID_AGC_ACCESS_TOKEN"
    );
    assert_eq!(Error::AccountDisabled.status(), StatusCode::FORBIDDEN);
    assert_eq!(Error::AccountDisabled.code(), "ACCOUNT_DISABLED");
}

#[test]
fn agc_user_token_has_an_independent_bounded_ascii_contract() {
    assert!(valid_agc_access_token("header.payload.signature"));
    for token in ["", "contains space", "line\nbreak", "令牌"] {
        assert!(!valid_agc_access_token(token), "{token:?}");
    }
    assert!(valid_agc_access_token(&"a".repeat(8192)));
    assert!(!valid_agc_access_token(&"a".repeat(8193)));
}

#[test]
fn key_alias_uses_nickname_and_stable_user_suffix_without_credential_data() {
    let user = "1234abcd-5678-4000-8000-123456789abc";
    assert_eq!(
        model_key_alias("huawei", user, Some(" 小明 ")),
        "huawei-小明-1234abcd"
    );
    assert_eq!(model_key_alias("agc", user, None), "agc-1234abcd");
    assert_eq!(
        model_key_alias("huawei", user, Some("../\\")),
        "huawei-1234abcd"
    );
    let alias = model_key_alias("huawei", user, Some(&"长".repeat(500)));
    assert_eq!(alias.chars().count(), 7 + 128 + 1 + 8);
    assert_eq!(
        model_key_alias("huawei", user, Some("<昵称>\n")),
        "huawei-昵称-1234abcd"
    );
}
