use anyhow::Result;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use sabissbackend::{
    app::{AppState, build_router},
    config::{db::create_pool, environment::Environment},
    module::auth::{
        crud::{self, ManagedGoogleWalletUpsert},
        model::VerifiedGoogleToken,
        schema::{WalletChallengeRequest, WalletChallengeResponse},
    },
    service::{crypto::create_managed_owner_key, stellar::deploy_wallet_contract},
};
use serde_json::{Value, json};
use std::sync::Once;
use tower::util::ServiceExt;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};
use reqwest::Client;
use uuid::Uuid;

static TEST_TRACING: Once = Once::new();

fn init_test_tracing() {
    TEST_TRACING.call_once(|| {
        tracing_subscriber::registry()
            .with(
                EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| EnvFilter::new("sabissbackend=debug")),
            )
            .with(tracing_subscriber::fmt::layer().with_test_writer())
            .init();
    });
}

async fn build_test_state() -> Result<AppState> {
    init_test_tracing();
    let env = Environment::load()?;
    let db = create_pool(&env).await?;
    sqlx::migrate!("./migrations").run(&db).await?;

    Ok(AppState {
        db,
        env,
        http_client: Client::new(),
    })
}

#[tokio::test]
async fn test_health_check() -> Result<()> {
    let state = build_test_state().await?;
    let app = build_router(state)?;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    
    let body = axum::body::to_bytes(response.into_body(), 1024).await?;
    let json: Value = serde_json::from_slice(&body)?;
    assert_eq!(json["status"], "ok");

    Ok(())
}

#[tokio::test]
async fn test_wallet_challenge_flow() -> Result<()> {
    let state = build_test_state().await?;
    let app = build_router(state)?;

    // 1. Request a challenge
    let wallet_address = "GC6UJTOU4SL2VU3EQ5S4P6W32MEAJAZL4TI6Y7AQZUTZXYFA6FICBWQ3";
    let challenge_req = WalletChallengeRequest {
        wallet_address: wallet_address.to_string(),
    };

    let response = app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/auth/wallet/challenge")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&challenge_req)?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    
    let body = axum::body::to_bytes(response.into_body(), 2048).await?;
    let challenge_res: WalletChallengeResponse = serde_json::from_slice(&body)?;
    
    assert!(!challenge_res.message.is_empty());
    assert!(challenge_res.message.contains(wallet_address));
    
    Ok(())
}

#[tokio::test]
async fn test_google_sign_in_config() -> Result<()> {
    let env = Environment::load()?;
    
    // Verify that the environment variables are correctly loaded
    assert!(env.google_client_id.is_some(), "GOOGLE_CLIENT_ID should be set in .env");
    assert_eq!(env.google_client_id.as_ref().unwrap(), "153387979068-g9sg813uuih831nsd1a3480trjlqnn7a.apps.googleusercontent.com");
    
    let db = create_pool(&env).await?;
    let state = AppState {
        db,
        env: env.clone(),
        http_client: Client::new(),
    };
    let app = build_router(state)?;

    // Test with an invalid credential to see if it reaches the verification logic
    let google_req = json!({
        "credential": "invalid-token",
        "g_csrf_token": "test-csrf",
        "client_id": "153387979068-g9sg813uuih831nsd1a3480trjlqnn7a.apps.googleusercontent.com"
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/auth/google/sign-in")
                .header("content-type", "application/json")
                .header("cookie", "g_csrf_token=test-csrf")
                .body(Body::from(serde_json::to_vec(&google_req)?))?,
        )
        .await?;

    // It should fail because the token is invalid, but it confirms the route and config are working
    // 401 Unauthorized or 400 Bad Request depending on implementation
    assert!(response.status() == StatusCode::UNAUTHORIZED || response.status() == StatusCode::BAD_REQUEST);
    
    Ok(())
}

#[tokio::test]
async fn test_google_user_sign_in_creates_smart_wallet_profile() -> Result<()> {
    let state = build_test_state().await?;
    let google_sub = format!("google-sub-{}", Uuid::new_v4());
    let email = format!("{}@example.com", google_sub);
    let verified = VerifiedGoogleToken {
        google_sub: google_sub.clone(),
        email: Some(email.clone()),
        email_verified: true,
        display_name: Some("Test User".to_owned()),
        avatar_url: Some("https://example.com/avatar.png".to_owned()),
    };

    let user = crud::upsert_google_user(&state.db, &verified).await?;
    let owner = create_managed_owner_key(&state.env)?;
    let deployed_wallet = deploy_wallet_contract(&state.env, &owner.owner_public_key_hex).await?;
    let _wallet = crud::upsert_google_managed_wallet(
        &state.db,
        &state.env,
        user.id,
        &verified.google_sub,
        &ManagedGoogleWalletUpsert {
            wallet_address: &deployed_wallet.contract_id,
            owner_address: &owner.owner_address,
            owner_ref: &verified.google_sub,
            owner_encrypted_private_key: &owner.encrypted_private_key,
            owner_encryption_nonce: &owner.encryption_nonce,
            owner_key_version: owner.key_version,
        },
    )
    .await?;

    let profile = crud::get_user_profile_by_id(&state.db, user.id)
        .await?
        .expect("google user profile should exist");
    let (user, wallet) = profile.into_parts();
    let wallet = wallet.expect("google sign-in should create a smart-wallet profile");

    assert_eq!(user.email.as_deref(), Some(email.as_str()));
    let wallet = sabissbackend::module::auth::schema::WalletResponse::from(wallet);
    assert_eq!(wallet.account_kind, "smart_account");
    assert_eq!(wallet.chain_id, 10143);
    assert!(!wallet.wallet_address.is_empty());
    assert_eq!(wallet.owner_address.as_deref(), Some(owner.owner_address.as_str()));
    assert_eq!(wallet.owner_provider.as_deref(), Some("google_oidc"));
    assert_eq!(
        wallet.factory_address.as_deref(),
        state.env.sabi_wallet_factory_id.as_deref()
    );
    assert_eq!(wallet.entry_point_address, None);

    Ok(())
}
