use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow_array::BooleanArray;
use arrow_array::Int64Array;
use arrow_array::RecordBatch;
use arrow_array::UInt64Array;
use arrow_schema::ArrowError;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::Schema;
use parquet::arrow::ArrowWriter;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ArrowPredicateFn;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_reader::RowFilter;
use parquet::basic::Compression;
use parquet::basic::ZstdLevel;
use parquet::file::metadata::PageIndexPolicy;
use parquet::file::properties::EnabledStatistics;
use parquet::file::properties::WriterProperties;

use crate::EventEntry;
use crate::IndexError;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("captured_at_ms", DataType::Int64, false),
        Field::new("record", DataType::UInt64, false),
    ]))
}

pub(crate) fn write_part(
    path: &Path,
    entries: &[EventEntry],
    row_group_entries: usize,
) -> Result<(), IndexError> {
    let captured_at =
        Int64Array::from_iter_values(entries.iter().map(|entry| entry.captured_at_ms));
    let records = UInt64Array::from_iter_values(entries.iter().map(|entry| entry.record));
    let batch = RecordBatch::try_new(schema(), vec![Arc::new(captured_at), Arc::new(records)])?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_max_row_group_row_count(Some(row_group_entries))
        .build();
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema(), Some(properties))?;
    writer.write(&batch)?;
    let _metadata = writer.close()?;
    File::open(path)?.sync_all()?;
    Ok(())
}

pub(crate) fn read_part_range(
    path: &Path,
    from_ms: i64,
    until_ms: i64,
) -> Result<Vec<EventEntry>, IndexError> {
    let file = File::open(path)?;
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)?;
    let descriptor = builder.metadata().file_metadata().schema_descr_ptr();
    let projection = ProjectionMask::leaves(&descriptor, [0]);
    let predicate = ArrowPredicateFn::new(projection, move |batch| {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| ArrowError::CastError("captured_at_ms is not int64".to_owned()))?;
        Ok(BooleanArray::from_iter(values.iter().map(|value| {
            value.map(|value| value >= from_ms && value < until_ms)
        })))
    });
    let filter = RowFilter::new(vec![Box::new(predicate)]);
    let reader = builder.with_row_filter(filter).build()?;
    let mut entries = Vec::new();
    for batch in reader {
        let batch = batch?;
        let captured_at = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| ArrowError::CastError("captured_at_ms is not int64".to_owned()))?;
        let records = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| ArrowError::CastError("record is not uint64".to_owned()))?;
        entries.extend(captured_at.values().iter().zip(records.values()).map(
            |(&captured_at_ms, &record)| EventEntry {
                captured_at_ms,
                record,
            },
        ));
    }
    Ok(entries)
}

pub(crate) fn read_all(path: &Path) -> Result<Vec<EventEntry>, IndexError> {
    let file = File::open(path)?;
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
    let reader = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)?.build()?;
    let mut entries = Vec::new();
    for batch in reader {
        append_batch(&mut entries, &batch?)?;
    }
    Ok(entries)
}

pub(crate) fn validate(path: &Path) -> Result<(), IndexError> {
    let file = File::open(path)?;
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)?;
    let fields = builder.schema().fields();
    let valid = match (fields.first(), fields.get(1)) {
        (Some(captured_at), Some(record)) if fields.len() == 2 => {
            captured_at.name() == "captured_at_ms"
                && captured_at.data_type() == &DataType::Int64
                && record.name() == "record"
                && record.data_type() == &DataType::UInt64
        }
        _ => false,
    };
    if !valid {
        return Err(IndexError::InvalidPartSchema);
    }
    Ok(())
}

fn append_batch(entries: &mut Vec<EventEntry>, batch: &RecordBatch) -> Result<(), IndexError> {
    let captured_at = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| ArrowError::CastError("captured_at_ms is not int64".to_owned()))?;
    let records = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| ArrowError::CastError("record is not uint64".to_owned()))?;
    entries.extend(captured_at.values().iter().zip(records.values()).map(
        |(&captured_at_ms, &record)| EventEntry {
            captured_at_ms,
            record,
        },
    ));
    Ok(())
}
