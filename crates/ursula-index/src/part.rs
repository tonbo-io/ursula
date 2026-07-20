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
use futures_util::TryStreamExt;
use parquet::arrow::ArrowWriter;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ArrowPredicateFn;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_reader::RowFilter;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::basic::Compression;
use parquet::basic::ZstdLevel;
use parquet::file::metadata::PageIndexPolicy;
use parquet::file::properties::EnabledStatistics;
use parquet::file::properties::WriterProperties;
use serde::Deserialize;
use serde::Serialize;

use crate::EventEntry;
use crate::IndexError;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct PartUnit {
    pub(crate) start: u64,
    pub(crate) end: u64,
    pub(crate) hash: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct PartLayout {
    pub(crate) version: u32,
    pub(crate) part_key: String,
    pub(crate) bytes: u64,
    pub(crate) units: Vec<PartUnit>,
}

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

pub(crate) async fn read_part_range_async<T>(
    reader: T,
    from_ms: i64,
    until_ms: i64,
) -> Result<Vec<EventEntry>, IndexError>
where
    T: AsyncFileReader + Send + Unpin + 'static,
{
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, options).await?;
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
    let batches = builder
        .with_row_filter(filter)
        .build()?
        .try_collect::<Vec<_>>()
        .await?;
    let mut entries = Vec::new();
    for batch in &batches {
        append_batch(&mut entries, batch)?;
    }
    Ok(entries)
}

pub(crate) fn build_layout(
    path: &Path,
    part_key: String,
    bytes: &[u8],
) -> Result<PartLayout, IndexError> {
    let file = File::open(path)?;
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)?;
    let file_bytes = u64::try_from(bytes.len())
        .map_err(|_error| IndexError::InvalidConfig("part is too large"))?;
    let metadata = builder.metadata();
    let offset_index = metadata
        .offset_index()
        .ok_or_else(|| IndexError::InvalidPartLayout(part_key.clone()))?;
    let mut native_units = Vec::new();
    for (row_group_index, group) in metadata.row_groups().iter().enumerate() {
        for (column_index, column) in group.columns().iter().enumerate() {
            let (chunk_start, chunk_length) = column.byte_range();
            let chunk_end = chunk_start
                .checked_add(chunk_length)
                .ok_or_else(|| IndexError::InvalidPartLayout(part_key.clone()))?;
            let pages = offset_index
                .get(row_group_index)
                .and_then(|indexes| indexes.get(column_index))
                .filter(|pages| !pages.page_locations().is_empty())
                .ok_or_else(|| IndexError::InvalidPartLayout(part_key.clone()))?;
            let mut cursor = chunk_start;
            for page in pages.page_locations() {
                let page_start = u64::try_from(page.offset)
                    .map_err(|_error| IndexError::InvalidPartLayout(part_key.clone()))?;
                let page_size = u64::try_from(page.compressed_page_size)
                    .map_err(|_error| IndexError::InvalidPartLayout(part_key.clone()))?;
                let page_end = page_start
                    .checked_add(page_size)
                    .ok_or_else(|| IndexError::InvalidPartLayout(part_key.clone()))?;
                if page_start < cursor || page_end > chunk_end || page_start >= page_end {
                    return Err(IndexError::InvalidPartLayout(part_key));
                }
                if cursor < page_start {
                    native_units.push(cursor..page_start);
                }
                native_units.push(page_start..page_end);
                cursor = page_end;
            }
            if cursor < chunk_end {
                native_units.push(cursor..chunk_end);
            }
        }
    }
    native_units.sort_unstable_by_key(|range| (range.start, range.end));
    let mut boundaries = Vec::new();
    let mut cursor = 0_u64;
    for unit in native_units {
        if unit.start < cursor || unit.start >= unit.end || unit.end > file_bytes {
            return Err(IndexError::InvalidPartLayout(part_key));
        }
        if cursor < unit.start {
            boundaries.push(cursor..unit.start);
        }
        boundaries.push(unit.clone());
        cursor = unit.end;
    }
    if cursor < file_bytes {
        boundaries.push(cursor..file_bytes);
    }
    if boundaries.is_empty() {
        return Err(IndexError::InvalidPartLayout(part_key));
    }
    let units = boundaries
        .into_iter()
        .map(|range| {
            let start = usize::try_from(range.start)
                .map_err(|_error| IndexError::InvalidPartLayout(part_key.clone()))?;
            let end = usize::try_from(range.end)
                .map_err(|_error| IndexError::InvalidPartLayout(part_key.clone()))?;
            let unit = bytes
                .get(start..end)
                .ok_or_else(|| IndexError::InvalidPartLayout(part_key.clone()))?;
            Ok(PartUnit {
                start: range.start,
                end: range.end,
                hash: crate::object_store::digest(unit),
            })
        })
        .collect::<Result<Vec<_>, IndexError>>()?;
    Ok(PartLayout {
        version: 1,
        part_key,
        bytes: file_bytes,
        units,
    })
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
