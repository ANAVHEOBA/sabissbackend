INSERT INTO wallet_accounts (id, user_id, wallet_address, chain_id, network)
VALUES (
    $1,
    $2,
    $3,
    CASE
        WHEN LOWER($4) = 'testnet' THEN 10143
        ELSE 1
    END,
    $4
)
RETURNING
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
