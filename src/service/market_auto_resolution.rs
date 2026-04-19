use uuid::Uuid;

use crate::{
    app::AppState,
    module::{
        auth::error::AuthError,
        market::{
            crud,
            model::NewMarketAutoResolutionConfigRecord,
            schema::{ConfigureMarketAutoResolveRequest, MarketAutoResolveConfigResponse},
        },
    },
};

pub async fn configure_market_auto_resolve_coinbase(
    state: &AppState,
    market_id: Uuid,
    payload: ConfigureMarketAutoResolveRequest,
) -> Result<MarketAutoResolveConfigResponse, AuthError> {
    let market = crud::get_market_by_id(&state.db, market_id)
        .await?
        .ok_or_else(|| AuthError::not_found("market not found"))?;
    let event = crud::get_market_event_by_id(&state.db, market.event_db_id)
        .await?
        .ok_or_else(|| AuthError::not_found("event not found"))?;

    let config = NewMarketAutoResolutionConfigRecord {
        market_id,
        provider: "coinbase".to_owned(),
        product_id: payload.auto_resolve.product_id.trim().to_owned(),
        start_time: payload.auto_resolve.start_time.unwrap_or(market.end_time),
        start_price: None,
        start_price_captured_at: None,
        end_price: None,
        end_price_captured_at: None,
        up_outcome_index: payload.auto_resolve.up_outcome_index,
        down_outcome_index: payload.auto_resolve.down_outcome_index,
        tie_outcome_index: payload.auto_resolve.tie_outcome_index,
        last_error: None,
    };

    let config = crud::upsert_market_auto_resolution_config(&state.db, &config).await?;
    Ok(MarketAutoResolveConfigResponse::from_records(
        &event, &market, &config,
    ))
}
