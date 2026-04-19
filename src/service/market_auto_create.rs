use crate::{
    app::AppState,
    module::{
        auth::error::AuthError,
        market::{
            crud,
            model::NewMarketAutoCreateSeriesRecord,
            schema::{ConfigureMarketAutoCreateSeriesRequest, MarketAutoCreateSeriesResponse},
        },
    },
    service::jwt::AuthenticatedUser,
};
use uuid::Uuid;

pub async fn configure_market_auto_create_series(
    state: &AppState,
    authenticated_user: AuthenticatedUser,
    payload: ConfigureMarketAutoCreateSeriesRequest,
) -> Result<MarketAutoCreateSeriesResponse, AuthError> {
    let series = payload.series;
    let record = NewMarketAutoCreateSeriesRecord {
        id: Uuid::new_v4(),
        provider: "coinbase".to_owned(),
        product_id: series.product_id.trim().to_owned(),
        title_prefix: series.title_prefix.trim().to_owned(),
        slug_prefix: series.slug_prefix.trim().to_owned(),
        category_slug: series.category_slug.trim().to_owned(),
        subcategory_slug: series.subcategory_slug,
        tag_slugs: series.tag_slugs,
        image_url: series.image_url,
        summary_text: series.summary,
        rules_text: series.rules,
        context_text: series.context,
        additional_context: series.additional_context,
        resolution_sources: series.resolution_sources,
        resolution_timezone: series.resolution_timezone,
        start_time: series.start_time,
        cadence_seconds: series.cadence_seconds,
        market_duration_seconds: series.market_duration_seconds,
        oracle_address: series.oracle_address.trim().to_owned(),
        outcomes: series.outcomes,
        up_outcome_index: series.up_outcome_index,
        down_outcome_index: series.down_outcome_index,
        tie_outcome_index: series.tie_outcome_index,
        featured: series.featured,
        breaking: series.breaking,
        searchable: series.searchable,
        visible: series.visible,
        hide_resolved_by_default: series.hide_resolved_by_default,
        active: series.active,
        last_created_slot_start: None,
        created_by_user_id: authenticated_user.user_id,
    };

    let record = crud::upsert_market_auto_create_series(&state.db, &record).await?;
    Ok(MarketAutoCreateSeriesResponse::from_record(&record))
}
