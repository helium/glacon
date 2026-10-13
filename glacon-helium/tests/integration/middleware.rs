//! The middleware picks outputs per record, or drops it.

use crate::common;
use glacon_helium::{FileStoreInput, IcebergOutput, MemoryState, Route, metrics as m};

#[tokio::test]
async fn routes_each_record_to_its_outputs_or_drops_it() -> anyhow::Result<()> {
    common::metrics::install()?;
    let mut h = common::Harness::new().await?;
    let b = h.add_bucket().await?;
    h.put_file(
        b,
        common::V1_PREFIX,
        common::minutes_ago(30),
        vec![
            common::v1("only_events", "a", 1),
            common::v1("only_values", "b", 2),
            common::v1("both", "both", 3),
            common::v1("nowhere", "drop", 4),
        ],
    )
    .await?;

    let route = Route::builder("mw_routes")
        .input(
            "v1",
            FileStoreInput::new(
                common::V1Input,
                h.input_settings(b, common::start_after()),
                MemoryState::new(),
            ),
        )
        .output(
            common::EVENTS,
            IcebergOutput::new(common::EventsTable("events"), h.output_settings()),
        )
        .output(
            common::VALUES,
            IcebergOutput::new(common::ValuesTable("event_values"), h.output_settings()),
        )
        .middleware_with(|outputs| {
            Ok(common::ByKindMiddleware {
                events: outputs.id(common::EVENTS)?,
                values: outputs.id(common::VALUES)?,
            })
        })
        .build()?;

    common::run(route).await?;

    assert_eq!(
        common::ids(&common::events(&h, "events").await?),
        ["both", "only_events"]
    );
    let value_ids: Vec<_> = common::values(&h, "event_values")
        .await?
        .into_iter()
        .map(|v| v.event_id)
        .collect();
    assert_eq!(value_ids, ["both", "only_values"]);

    let route = [("route", "mw_routes")];
    assert_eq!(common::metrics::counter(m::MIDDLEWARE_DROPPED, &route)?, 1);
    assert_eq!(
        common::metrics::counter(
            m::OUTPUT_RECORDS,
            &[("route", "mw_routes"), ("output", common::EVENTS)]
        )?,
        2
    );
    assert_eq!(
        common::metrics::counter(
            m::OUTPUT_RECORDS,
            &[("route", "mw_routes"), ("output", common::VALUES)]
        )?,
        2
    );
    h.cleanup().await?;
    Ok(())
}
