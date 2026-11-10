//! The middleware rejects records; each output keeps the kinds it wants,
//! and a rejects table keeps the errors.

use crate::common::{self, Kind, Only};
use glacon_helium::{FileStoreInput, IcebergOutput, Route, metrics as m};

#[tokio::test]
async fn outputs_keep_what_they_want_of_what_the_middleware_made() -> anyhow::Result<()> {
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
        .middleware(common::RejectDrop)
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
        .output(
            common::REJECTS,
            IcebergOutput::new(common::RejectsTable("rejects"), h.output_settings()),
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
    assert_eq!(common::rejects(&h, "rejects").await?, ["nowhere: rejected"]);

    let route = [("route", "mw_routes")];
    assert_eq!(common::metrics::counter(m::MIDDLEWARE_ERRORS, &route)?, 1);
    let rejected = [("route", "mw_routes"), ("reason", "rejected")];
    assert_eq!(
        common::metrics::counter(m::MIDDLEWARE_ERRORS, &rejected)?,
        1
    );
    for (output, ok_rows, err_rows) in [
        (common::EVENTS, 2, 0),
        (common::VALUES, 2, 0),
        (common::REJECTS, 0, 1),
    ] {
        let labels = [("route", "mw_routes"), ("output", output)];
        // Every output is handed every result, the error included...
        assert_eq!(
            common::metrics::counter(m::OUTPUT_RECORDS, &labels)?,
            4,
            "{output}"
        );
        for (kind, n) in [("ok", 3), ("err", 1)] {
            let by_kind = [("route", "mw_routes"), ("output", output), ("kind", kind)];
            assert_eq!(
                common::metrics::counter(m::OUTPUT_RECORDS, &by_kind)?,
                n,
                "{output} {kind}"
            );
        }
        // ...and keeps some of them, as rows made from records or errors.
        for (kind, n) in [("ok", ok_rows), ("err", err_rows)] {
            let by_kind = [("route", "mw_routes"), ("output", output), ("kind", kind)];
            assert_eq!(
                common::metrics::counter(m::OUTPUT_ROWS, &by_kind)?,
                n,
                "{output} {kind}"
            );
        }
        assert_eq!(common::metrics::counter(m::OUTPUT_BATCHES, &labels)?, 1);
    }
    h.cleanup().await?;
    Ok(())
}
