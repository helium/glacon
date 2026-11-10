use glacon_core::GlaconRecordError;
use serde::Serialize;
use std::convert::Infallible;

/// Turns route records into rows of one Iceberg table.
///
/// The table is created from [`GlaconOutput::table`] when it does not exist yet.
/// Schema changes to an existing table are not handled.
///
/// The table must be partitioned: helium-iceberg's writer cannot write to an
/// unpartitioned table.
///
/// `E` is the error type of the route's middleware ([`Infallible`] without
/// one). A table that keeps only records can implement this for any
/// `E: GlaconRecordError`.
pub trait GlaconOutput<R, E: GlaconRecordError = Infallible>: Send + Sync + 'static {
    /// Row written to the table; serialized to Arrow through serde, so field
    /// names must match the table's columns.
    type Row: Serialize + Send + Sync + 'static;

    /// Definition (namespace, name, schema, partitioning) of the table.
    fn table(&self) -> helium_iceberg::Result<helium_iceberg::TableDefinition>;

    /// Append the rows for `record`. Zero, one or many rows are all fine.
    ///
    /// The output gets every record of every batch: append none for the
    /// records this table doesn't keep. Decide from the record alone, so a
    /// replayed batch gives the same rows.
    fn to_rows(&self, record: &R, rows: &mut Vec<Self::Row>);

    /// Append the rows for a record the middleware could not make, given
    /// why: the middleware's own error, to match on. Default: none, so the
    /// table ignores the middleware's errors; a rejects table appends a row
    /// for them. Decide from the error alone, so a replayed batch gives the
    /// same rows.
    fn to_error_rows(&self, _error: &E, _rows: &mut Vec<Self::Row>) {}
}
