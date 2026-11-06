use serde::Serialize;

/// Turns route records into rows of one Iceberg table.
///
/// The table is created from [`GlaconOutput::table`] when it does not exist yet.
/// Schema changes to an existing table are not handled.
///
/// The table must be partitioned: helium-iceberg's writer cannot write to an
/// unpartitioned table.
pub trait GlaconOutput<R>: Send + Sync + 'static {
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
}
