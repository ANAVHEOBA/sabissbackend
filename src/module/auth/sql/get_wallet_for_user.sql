SELECT
    wallet_address,
    network,
    account_kind,
    wallet_status,
    wallet_standard,
    owner_address,
    owner_provider,
    owner_ref,
    sponsor_address,
    relayer_kind,
    relayer_url,
    factory_contract_id,
    web_auth_contract_id,
    web_auth_domain,
    owner_encrypted_private_key,
    owner_encryption_nonce,
    owner_key_version,
    deployed_at,
    last_authenticated_at,
    created_at
FROM wallet_accounts
WHERE user_id = $1
