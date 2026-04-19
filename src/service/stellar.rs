use std::process::Stdio;

use anyhow::{Context, Result, anyhow};
use serde_json::Value;
use tokio::process::Command;

use crate::config::environment::Environment;

#[derive(Debug, Clone)]
pub struct NegRiskRegistrationTxResult {
    pub tx_hash: String,
}

#[derive(Debug, Clone)]
pub struct PublishEventTxResult {
    pub tx_hash: String,
}

#[derive(Debug, Clone)]
pub struct PublishBinaryMarketTxResult {
    pub tx_hash: String,
    pub condition_id: String,
}

#[derive(Debug, Clone)]
pub struct ContractTxResult {
    pub tx_hash: String,
}

#[derive(Debug, Clone)]
pub struct DeployWalletContractResult {
    pub contract_id: String,
}

#[derive(Debug, Clone)]
pub struct ProposeResolutionTxResult {
    pub tx_hash: String,
    pub dispute_window_seconds: i64,
}

#[derive(Debug, Clone)]
pub struct SetMarketPricesTxResult {
    pub yes_price_tx_hash: String,
    pub no_price_tx_hash: String,
}

#[derive(Debug, Clone)]
pub struct BootstrapMarketLiquidityTxResult {
    pub yes_price_tx_hash: String,
    pub no_price_tx_hash: String,
    pub split_and_add_liquidity_tx_hash: String,
    pub deposit_collateral_tx_hash: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MarketLiquidityReadResult {
    pub yes_available: String,
    pub no_available: String,
    pub idle_yes_total: String,
    pub idle_no_total: String,
    pub posted_yes_total: String,
    pub posted_no_total: String,
    pub claimable_collateral_total: String,
}

#[derive(Debug, Clone)]
pub struct MarketPricesReadResult {
    pub yes_bps: u32,
    pub no_bps: u32,
}

const DEFAULT_RESOLUTION_DISPUTE_WINDOW_SECONDS: i64 = 86_400;
const STELLAR_PLACEHOLDER_TX_HASH: &str = "stellar-cli-submitted";

pub async fn deploy_wallet_contract(
    env: &Environment,
    owner_public_key_hex: &str,
) -> Result<DeployWalletContractResult> {
    let factory_id = env
        .sabi_wallet_factory_id
        .as_deref()
        .ok_or_else(|| anyhow!("missing SABI_WALLET_FACTORY_ID for managed wallet provisioning"))?;
    let contract_id = invoke_contract(
        env,
        factory_id,
        true,
        &["create_wallet", "--owner", owner_public_key_hex],
    )
    .await
    .context("failed to create user wallet through Soroban wallet factory")?;

    Ok(DeployWalletContractResult { contract_id })
}

pub async fn register_neg_risk_event(
    env: &Environment,
    event_id: &str,
    other_market_condition_id: Option<&str>,
) -> Result<NegRiskRegistrationTxResult> {
    let event_id = bytes32_cli_arg(event_id)?;
    let other_market_condition_id = other_market_condition_id.unwrap_or(
        "0000000000000000000000000000000000000000000000000000000000000000",
    );
    let other_market_condition_id = bytes32_cli_arg(other_market_condition_id)?;

    invoke_contract(
        env,
        &env.sabi_neg_risk_id,
        true,
        &[
            "register_neg_risk_event",
            "--event-id",
            event_id.as_str(),
            "--other-market",
            other_market_condition_id.as_str(),
        ],
    )
    .await
    .context("failed to register neg-risk event on Soroban")?;

    Ok(NegRiskRegistrationTxResult {
        tx_hash: STELLAR_PLACEHOLDER_TX_HASH.to_owned(),
    })
}

pub async fn publish_event(
    env: &Environment,
    event_id: &str,
    group_id: &str,
    series_id: &str,
    neg_risk: bool,
) -> Result<PublishEventTxResult> {
    let event_id = bytes32_cli_arg(event_id)?;
    let group_id = bytes32_cli_arg(group_id)?;
    let series_id = bytes32_cli_arg(series_id)?;

    invoke_contract(
        env,
        &env.sabi_market_id,
        true,
        &[
            "create_event",
            "--event-id",
            event_id.as_str(),
            "--group-id",
            group_id.as_str(),
            "--series-id",
            series_id.as_str(),
            "--neg-risk",
            bool_arg(neg_risk),
        ],
    )
    .await
    .context("failed to publish event on Soroban")?;

    Ok(PublishEventTxResult {
        tx_hash: STELLAR_PLACEHOLDER_TX_HASH.to_owned(),
    })
}

pub async fn publish_standalone_binary_market(
    env: &Environment,
    event_id: &str,
    group_id: &str,
    series_id: &str,
    neg_risk: bool,
    question_id: &str,
    end_time: u64,
    oracle: &str,
) -> Result<PublishBinaryMarketTxResult> {
    publish_event(env, event_id, group_id, series_id, neg_risk).await?;

    publish_event_market(env, event_id, question_id, end_time, oracle).await
}

pub async fn publish_event_market(
    env: &Environment,
    event_id: &str,
    question_id: &str,
    end_time: u64,
    oracle: &str,
) -> Result<PublishBinaryMarketTxResult> {
    let event_id = bytes32_cli_arg(event_id)?;
    let question_id = bytes32_cli_arg(question_id)?;
    let condition_id = invoke_contract(
        env,
        &env.sabi_market_id,
        true,
        &[
            "create_binary_market_for_event",
            "--event-id",
            event_id.as_str(),
            "--question-id",
            question_id.as_str(),
            "--end-time",
            &end_time.to_string(),
            "--oracle",
            oracle,
        ],
    )
    .await
    .context("failed to publish event market on Soroban")?;

    Ok(PublishBinaryMarketTxResult {
        tx_hash: STELLAR_PLACEHOLDER_TX_HASH.to_owned(),
        condition_id,
    })
}

pub async fn find_existing_event_binary_market(
    _env: &Environment,
    _event_id: &str,
    _question_id: &str,
) -> Result<Option<String>> {
    Ok(None)
}

pub async fn pause_market(env: &Environment, condition_id: &str) -> Result<ContractTxResult> {
    let condition_id = bytes32_cli_arg(condition_id)?;
    invoke_contract(
        env,
        &env.sabi_market_id,
        true,
        &["pause_market", "--condition-id", condition_id.as_str()],
    )
    .await
    .context("failed to pause market on Soroban")?;

    Ok(ContractTxResult {
        tx_hash: STELLAR_PLACEHOLDER_TX_HASH.to_owned(),
    })
}

pub async fn unpause_market(env: &Environment, condition_id: &str) -> Result<ContractTxResult> {
    let condition_id = bytes32_cli_arg(condition_id)?;
    invoke_contract(
        env,
        &env.sabi_market_id,
        true,
        &["unpause_market", "--condition-id", condition_id.as_str()],
    )
    .await
    .context("failed to unpause market on Soroban")?;

    Ok(ContractTxResult {
        tx_hash: STELLAR_PLACEHOLDER_TX_HASH.to_owned(),
    })
}

pub async fn propose_resolution(
    env: &Environment,
    condition_id: &str,
    winning_outcome: u64,
    _oracle_address: &str,
) -> Result<ProposeResolutionTxResult> {
    let condition_id = bytes32_cli_arg(condition_id)?;
    invoke_contract(
        env,
        &env.sabi_market_id,
        true,
        &[
            "propose_resolution",
            "--resolver",
            &env.admin,
            "--condition-id",
            condition_id.as_str(),
            "--winning-outcome",
            &winning_outcome.to_string(),
        ],
    )
    .await
    .context("failed to propose market resolution on Soroban")?;

    let dispute_window_seconds = invoke_contract(
        env,
        &env.sabi_market_id,
        false,
        &["get_resolution_dispute_window"],
    )
    .await
    .ok()
    .and_then(|value| value.parse::<i64>().ok())
    .unwrap_or(DEFAULT_RESOLUTION_DISPUTE_WINDOW_SECONDS);

    Ok(ProposeResolutionTxResult {
        tx_hash: STELLAR_PLACEHOLDER_TX_HASH.to_owned(),
        dispute_window_seconds,
    })
}

pub async fn dispute_resolution(
    env: &Environment,
    condition_id: &str,
) -> Result<ContractTxResult> {
    let condition_id = bytes32_cli_arg(condition_id)?;
    invoke_contract(
        env,
        &env.sabi_market_id,
        true,
        &[
            "dispute_resolution",
            "--disputer",
            &env.admin,
            "--condition-id",
            condition_id.as_str(),
        ],
    )
    .await
    .context("failed to dispute market resolution on Soroban")?;

    Ok(ContractTxResult {
        tx_hash: STELLAR_PLACEHOLDER_TX_HASH.to_owned(),
    })
}

pub async fn finalize_resolution(
    env: &Environment,
    condition_id: &str,
    _oracle_address: &str,
    _winning_outcome: u64,
) -> Result<ContractTxResult> {
    let condition_id = bytes32_cli_arg(condition_id)?;
    invoke_contract(
        env,
        &env.sabi_market_id,
        true,
        &["finalize_resolution", "--condition-id", condition_id.as_str()],
    )
    .await
    .context("failed to finalize market resolution on Soroban")?;

    Ok(ContractTxResult {
        tx_hash: STELLAR_PLACEHOLDER_TX_HASH.to_owned(),
    })
}

pub async fn emergency_resolve_market(
    env: &Environment,
    condition_id: &str,
    oracle_address: &str,
    winning_outcome: u64,
) -> Result<ContractTxResult> {
    finalize_resolution(env, condition_id, oracle_address, winning_outcome).await
}

pub async fn buy_market_outcome(
    env: &Environment,
    source_account: &str,
    buyer: &str,
    condition_id: &str,
    outcome_index: u32,
    usdc_amount: &str,
) -> Result<ContractTxResult> {
    let condition_id = bytes32_cli_arg(condition_id)?;
    invoke_contract_as_source(
        env,
        source_account,
        &env.sabi_exchange_id,
        true,
        &[
            "buy_outcome",
            "--buyer",
            buyer,
            "--condition-id",
            condition_id.as_str(),
            "--outcome-index",
            &outcome_index.to_string(),
            "--usdc-amount",
            usdc_amount,
        ],
    )
    .await
    .context("failed to buy outcome on Soroban exchange")?;

    Ok(ContractTxResult {
        tx_hash: STELLAR_PLACEHOLDER_TX_HASH.to_owned(),
    })
}

pub async fn sell_market_outcome(
    env: &Environment,
    source_account: &str,
    seller: &str,
    condition_id: &str,
    outcome_index: u32,
    token_amount: &str,
) -> Result<ContractTxResult> {
    let condition_id = bytes32_cli_arg(condition_id)?;
    invoke_contract_as_source(
        env,
        source_account,
        &env.sabi_exchange_id,
        true,
        &[
            "sell_outcome",
            "--seller",
            seller,
            "--condition-id",
            condition_id.as_str(),
            "--outcome-index",
            &outcome_index.to_string(),
            "--token-amount",
            token_amount,
        ],
    )
    .await
    .context("failed to sell outcome on Soroban exchange")?;

    Ok(ContractTxResult {
        tx_hash: STELLAR_PLACEHOLDER_TX_HASH.to_owned(),
    })
}

pub async fn split_market_position(
    env: &Environment,
    source_account: &str,
    user: &str,
    condition_id: &str,
    collateral_amount: &str,
) -> Result<ContractTxResult> {
    let condition_id = bytes32_cli_arg(condition_id)?;
    invoke_contract_as_source(
        env,
        source_account,
        &env.sabi_ctf_id,
        true,
        &[
            "split_position",
            "--user",
            user,
            "--collateral-token",
            &env.mock_usdc_id,
            "--parent-collection-id",
            "0000000000000000000000000000000000000000000000000000000000000000",
            "--condition-id",
            condition_id.as_str(),
            "--partition",
            "[1,2]",
            "--amount",
            collateral_amount,
        ],
    )
    .await
    .context("failed to split market position on Soroban CTF")?;

    Ok(ContractTxResult {
        tx_hash: STELLAR_PLACEHOLDER_TX_HASH.to_owned(),
    })
}

pub async fn merge_market_positions(
    env: &Environment,
    source_account: &str,
    user: &str,
    condition_id: &str,
    pair_token_amount: &str,
) -> Result<ContractTxResult> {
    let condition_id = bytes32_cli_arg(condition_id)?;
    invoke_contract_as_source(
        env,
        source_account,
        &env.sabi_ctf_id,
        true,
        &[
            "merge_positions",
            "--user",
            user,
            "--collateral-token",
            &env.mock_usdc_id,
            "--parent-collection-id",
            "0000000000000000000000000000000000000000000000000000000000000000",
            "--condition-id",
            condition_id.as_str(),
            "--partition",
            "[1,2]",
            "--amount",
            pair_token_amount,
        ],
    )
    .await
    .context("failed to merge market positions on Soroban CTF")?;

    Ok(ContractTxResult {
        tx_hash: STELLAR_PLACEHOLDER_TX_HASH.to_owned(),
    })
}

pub async fn set_market_prices(
    env: &Environment,
    condition_id: &str,
    yes_bps: u32,
    no_bps: u32,
) -> Result<SetMarketPricesTxResult> {
    let condition_id = bytes32_cli_arg(condition_id)?;
    invoke_contract(
        env,
        &env.sabi_exchange_id,
        true,
        &[
            "set_price",
            "--condition-id",
            condition_id.as_str(),
            "--outcome-index",
            "0",
            "--price-bps",
            &yes_bps.to_string(),
        ],
    )
    .await
    .context("failed to set YES market price on Soroban")?;
    invoke_contract(
        env,
        &env.sabi_exchange_id,
        true,
        &[
            "set_price",
            "--condition-id",
            condition_id.as_str(),
            "--outcome-index",
            "1",
            "--price-bps",
            &no_bps.to_string(),
        ],
    )
    .await
    .context("failed to set NO market price on Soroban")?;

    Ok(SetMarketPricesTxResult {
        yes_price_tx_hash: STELLAR_PLACEHOLDER_TX_HASH.to_owned(),
        no_price_tx_hash: STELLAR_PLACEHOLDER_TX_HASH.to_owned(),
    })
}

pub async fn bootstrap_market_liquidity(
    env: &Environment,
    condition_id: &str,
    yes_bps: u32,
    no_bps: u32,
    inventory_usdc_amount: &str,
    exit_collateral_usdc_amount: &str,
) -> Result<BootstrapMarketLiquidityTxResult> {
    let prices = set_market_prices(env, condition_id, yes_bps, no_bps).await?;
    let condition_id = bytes32_cli_arg(condition_id)?;

    invoke_contract(
        env,
        &env.sabi_liquidity_manager_id,
        true,
        &[
            "split_and_add_liquidity",
            "--provider",
            &env.admin,
            "--condition-id",
            condition_id.as_str(),
            "--amount",
            inventory_usdc_amount,
        ],
    )
    .await
    .context("failed to split and add liquidity on Soroban")?;

    let deposit_collateral_tx_hash = if exit_collateral_usdc_amount != "0" {
        invoke_contract(
            env,
            &env.sabi_liquidity_manager_id,
            true,
            &[
                "deposit_collateral",
                "--provider",
                &env.admin,
                "--condition-id",
                condition_id.as_str(),
                "--amount",
                exit_collateral_usdc_amount,
            ],
        )
        .await
        .context("failed to deposit exit collateral through Soroban liquidity manager")?;
        Some(STELLAR_PLACEHOLDER_TX_HASH.to_owned())
    } else {
        None
    };

    Ok(BootstrapMarketLiquidityTxResult {
        yes_price_tx_hash: prices.yes_price_tx_hash,
        no_price_tx_hash: prices.no_price_tx_hash,
        split_and_add_liquidity_tx_hash: STELLAR_PLACEHOLDER_TX_HASH.to_owned(),
        deposit_collateral_tx_hash,
    })
}

pub async fn get_market_liquidity(
    env: &Environment,
    condition_id: &str,
) -> Result<MarketLiquidityReadResult> {
    let condition_id = bytes32_cli_arg(condition_id)?;
    let yes_available = invoke_contract(
        env,
        &env.sabi_exchange_id,
        false,
        &[
            "get_available_liquidity",
            "--condition-id",
            condition_id.as_str(),
            "--outcome-index",
            "0",
        ],
    )
    .await
    .context("failed to read YES liquidity on Soroban exchange")?;
    let no_available = invoke_contract(
        env,
        &env.sabi_exchange_id,
        false,
        &[
            "get_available_liquidity",
            "--condition-id",
            condition_id.as_str(),
            "--outcome-index",
            "1",
        ],
    )
    .await
    .context("failed to read NO liquidity on Soroban exchange")?;
    let totals = invoke_contract(
        env,
        &env.sabi_liquidity_manager_id,
        false,
        &["get_market_liquidity", "--condition-id", condition_id.as_str()],
    )
    .await
    .context("failed to read liquidity totals on Soroban liquidity manager")?;
    let mut totals = parse_liquidity_totals(&totals)?;
    if totals.posted_yes_total == "0" && yes_available != "0" {
        totals.posted_yes_total = yes_available.clone();
    }
    if totals.posted_no_total == "0" && no_available != "0" {
        totals.posted_no_total = no_available.clone();
    }

    Ok(MarketLiquidityReadResult {
        yes_available,
        no_available,
        idle_yes_total: "0".to_owned(),
        idle_no_total: "0".to_owned(),
        posted_yes_total: totals.posted_yes_total,
        posted_no_total: totals.posted_no_total,
        claimable_collateral_total: totals.claimable_collateral_total,
    })
}

pub async fn get_market_prices_batch_best_effort(
    env: &Environment,
    condition_ids: &[String],
) -> Result<std::collections::HashMap<String, MarketPricesReadResult>> {
    let mut prices = std::collections::HashMap::new();
    for condition_id in condition_ids {
        let Ok(normalized) = bytes32_cli_arg(condition_id) else {
            continue;
        };

        let yes_bps = invoke_contract(
            env,
            &env.sabi_exchange_id,
            false,
            &[
                "get_price",
                "--condition-id",
                normalized.as_str(),
                "--outcome-index",
                "0",
            ],
        )
        .await;
        let no_bps = invoke_contract(
            env,
            &env.sabi_exchange_id,
            false,
            &[
                "get_price",
                "--condition-id",
                normalized.as_str(),
                "--outcome-index",
                "1",
            ],
        )
        .await;

        let (Ok(yes_bps), Ok(no_bps)) = (yes_bps, no_bps) else {
            continue;
        };
        let (Ok(yes_bps), Ok(no_bps)) = (yes_bps.parse::<u32>(), no_bps.parse::<u32>()) else {
            continue;
        };
        prices.insert(
            normalized,
            MarketPricesReadResult { yes_bps, no_bps },
        );
    }
    Ok(prices)
}

fn bool_arg(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}

fn bytes32_cli_arg(value: &str) -> Result<String> {
    let normalized = value.trim();
    let normalized = normalized.trim_matches('"');
    let normalized = normalized.strip_prefix("0x").unwrap_or(normalized);
    let normalized = normalized.strip_prefix("0X").unwrap_or(normalized);

    if normalized.len() != 64 || !normalized.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err(anyhow!("invalid bytes32 argument `{value}`"));
    }

    Ok(normalized.to_ascii_lowercase())
}

async fn invoke_contract(
    env: &Environment,
    contract_id: &str,
    send: bool,
    contract_args: &[&str],
) -> Result<String> {
    let source_account = env.private_key.as_deref().unwrap_or(&env.source);
    invoke_contract_as_source(env, source_account, contract_id, send, contract_args).await
}

async fn invoke_contract_as_source(
    env: &Environment,
    source_account: &str,
    contract_id: &str,
    send: bool,
    contract_args: &[&str],
) -> Result<String> {
    let mut command = Command::new("stellar");
    command
        .arg("contract")
        .arg("invoke")
        .arg("--network")
        .arg(&env.network)
        .arg("--source-account")
        .arg(source_account)
        .arg("--id")
        .arg(contract_id)
        .arg("--send")
        .arg(if send { "yes" } else { "no" })
        .arg("--")
        .args(contract_args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let output = command
        .output()
        .await
        .with_context(|| format!("failed to execute `stellar contract invoke` for `{contract_id}`"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        let detail = if !stderr.is_empty() { stderr } else { stdout };
        return Err(anyhow!(
            "stellar contract invoke failed for `{contract_id}`: {detail}"
        ));
    }

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    Ok(parse_invoke_output(stdout))
}

fn parse_invoke_output(stdout: String) -> String {
    match serde_json::from_str::<Value>(&stdout) {
        Ok(Value::String(value)) => value,
        _ => stdout,
    }
}

struct LiquidityTotalsParsed {
    posted_yes_total: String,
    posted_no_total: String,
    claimable_collateral_total: String,
}

fn parse_liquidity_totals(raw: &str) -> Result<LiquidityTotalsParsed> {
    let value: Value = serde_json::from_str(raw)
        .with_context(|| format!("failed to decode liquidity totals output: {raw}"))?;
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("liquidity totals output was not an object"))?;

    Ok(LiquidityTotalsParsed {
        posted_yes_total: json_string_field(object, "posted_yes_total")?,
        posted_no_total: json_string_field(object, "posted_no_total")?,
        claimable_collateral_total: json_string_field(object, "claimable_collateral_total")?,
    })
}

fn json_string_field(
    object: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<String> {
    let value = object
        .get(field)
        .ok_or_else(|| anyhow!("missing field `{field}` in contract output"))?;
    match value {
        Value::String(value) => Ok(value.clone()),
        Value::Number(value) => Ok(value.to_string()),
        _ => Err(anyhow!("unexpected value type for contract field `{field}`")),
    }
}
