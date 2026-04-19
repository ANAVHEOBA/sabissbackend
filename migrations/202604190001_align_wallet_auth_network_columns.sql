ALTER TABLE wallet_accounts
ADD COLUMN IF NOT EXISTS network TEXT;

UPDATE wallet_accounts
SET network = CASE
    WHEN network IS NOT NULL THEN network
    WHEN chain_id = 10143 THEN 'testnet'
    WHEN chain_id IS NOT NULL THEN 'mainnet'
    ELSE 'testnet'
END
WHERE network IS NULL;

ALTER TABLE wallet_accounts
ALTER COLUMN network SET DEFAULT 'testnet';

ALTER TABLE wallet_accounts
ALTER COLUMN network SET NOT NULL;

ALTER TABLE wallet_challenges
ADD COLUMN IF NOT EXISTS network TEXT;

UPDATE wallet_challenges
SET network = CASE
    WHEN network IS NOT NULL THEN network
    WHEN chain_id = 10143 THEN 'testnet'
    WHEN chain_id IS NOT NULL THEN 'mainnet'
    ELSE 'testnet'
END
WHERE network IS NULL;

ALTER TABLE wallet_challenges
ALTER COLUMN network SET DEFAULT 'testnet';

ALTER TABLE wallet_challenges
ALTER COLUMN network SET NOT NULL;
