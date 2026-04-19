use crate::{
    app::AppState,
    module::auth::{
        crud::{self, ManagedGoogleWalletUpsert},
        error::AuthError,
        model::{ACCOUNT_KIND_STELLAR_SMART_WALLET, UserRecord, VerifiedGoogleToken},
    },
    service::{
        crypto::create_managed_owner_key,
        stellar::deploy_wallet_contract,
    },
};

pub async fn ensure_google_user_smart_wallet(
    state: &AppState,
    user: &UserRecord,
    verified: &VerifiedGoogleToken,
) -> Result<(), AuthError> {
    if let Some(existing_wallet) = crud::get_wallet_for_user(&state.db, user.id).await? {
        if existing_wallet.account_kind == ACCOUNT_KIND_STELLAR_SMART_WALLET
            && existing_wallet.wallet_address.is_some()
        {
            return Ok(());
        }
    }

    let owner = create_managed_owner_key(&state.env)
        .map_err(|error| AuthError::internal("failed to create smart-wallet owner key", error))?;
    let deployed_wallet = deploy_wallet_contract(&state.env, &owner.owner_public_key_hex)
        .await
        .map_err(|error| AuthError::internal("failed to deploy smart wallet", error))?;

    crud::upsert_google_managed_wallet(
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

    Ok(())
}
