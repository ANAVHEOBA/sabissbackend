INSERT INTO wallet_challenges (
    id,
    wallet_address,
    chain_id,
    network,
    nonce,
    message,
    expires_at
)
VALUES (
    $1,
    $2,
    CASE
        WHEN LOWER($3) = 'testnet' THEN 10143
        ELSE 1
    END,
    $3,
    $4,
    $5,
    $6
)
RETURNING id, wallet_address, network, nonce, message, expires_at, consumed_at, created_at
