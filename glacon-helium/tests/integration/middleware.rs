//! The middleware drops records; each output keeps the kinds it wants.

use crate::common::{self, Kind, Only};
use glacon_helium::{FileStoreInput, IcebergOutput, Route, metrics as m};

#[tokio::test]
async fn outputs_keep_what_they_want_of_what_the_middleware_left() -> anyhow::Result<()> {
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

    let route = Route::builder("mw_routes", h.route_settings("mw_routes"))
        .input(
            "v1",
            FileStoreInput::new(common::V1Input, h.input_settings(b, common::start_after())),
        )
        .middleware(common::DropKind)
        .output(
            common::EVENTS,
            IcebergOutput::new(
                Only(common::EventsTable("events"), &[Kind::A, Kind::Both]),
                h.output_settings(),
            ),
        )
        .output(
            common::VALUES,
            IcebergOutput::new(
                Only(common::ValuesTable("event_values"), &[Kind::B, Kind::Both]),
                h.output_settings(),
            ),
        )
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
    for output in [common::EVENTS, common::VALUES] {
        let labels = [("route", "mw_routes"), ("output", output)];
        // Every output is handed what the middleware left...
        assert_eq!(
            common::metrics::counter(m::OUTPUT_RECORDS, &labels)?,
            3,
            "{output}"
        );
        // ...and keeps two of them.
        assert_eq!(
            common::metrics::counter(m::OUTPUT_ROWS, &labels)?,
            2,
            "{output}"
        );
    }
    h.cleanup().await?;
    Ok(())
}
