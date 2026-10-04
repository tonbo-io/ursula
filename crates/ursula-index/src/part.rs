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

const T_MS: &str = "t_ms";
const T_END_MS: &str = "t_end_ms";
const OFFSET: &str = "offset";
const LEN: &str = "len";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(T_MS, DataType::Int64, false),
        Field::new(T_END_MS, DataType::Int64, false),
        Field::new(OFFSET, DataType::UInt64, false),
        Field::new(LEN, DataType::UInt64, false),
    ]))
}

/// Row predicate shared by part pruning and the Parquet row filter.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PartFilter {
    pub(crate) from_ms: i64,
    pub(crate) until_ms: i64,
    pub(crate) overlap: bool,
    /// Entries before this offset are below retention.
    pub(crate) floor: u64,
    /// Entries at or after this offset are past the pinned watermark.
    pub(crate) through: u64,
    /// Only entries after this `(t_ms, offset)` cursor.
    pub(crate) after: Option<(i64, u64)>,
}

impl PartFilter {
    pub(crate) fn matches(&self, t_ms: i64, t_end_ms: i64, offset: u64) -> bool {
        let in_window = if self.overlap {
            t_ms < self.until_ms && t_end_ms >= self.from_ms
        } else {
            t_ms >= self.from_ms && t_ms < self.until_ms
        };
        in_window
            && offset >= self.floor
            && offset < self.through
            && self.after.is_none_or(|after| (t_ms, offset) > after)
    }
}

pub(crate) fn write_part(
    path: &Path,
    entries: &[EventEntry],
    row_group_entries: usize,
) -> Result<(), IndexError> {
    let t_ms = Int64Array::from_iter_values(entries.iter().map(|entry| entry.t_ms));
    let t_end_ms = Int64Array::from_iter_values(entries.iter().map(|entry| entry.t_end_ms));
    let offsets = UInt64Array::from_iter_values(entries.iter().map(|entry| entry.offset));
    let lens = UInt64Array::from_iter_values(entries.iter().map(|entry| entry.len));
    let batch = RecordBatch::try_new(schema(), vec![
        Arc::new(t_ms),
        Arc::new(t_end_ms),
        Arc::new(offsets),
        Arc::new(lens),
    ])?;
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
    filter: PartFilter,
) -> Result<Vec<EventEntry>, IndexError>
where
    T: AsyncFileReader + Send + Unpin + 'static,
{
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, options).await?;
    let descriptor = builder.metadata().file_metadata().schema_descr_ptr();
    let leaves = descriptor
        .columns()
        .iter()
        .enumerate()
        .filter(|(_, column)| matches!(column.name(), T_MS | T_END_MS | OFFSET))
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    let projection = ProjectionMask::leaves(&descriptor, leaves);
    let predicate = ArrowPredicateFn::new(projection, move |batch| {
        let t_ms = int64_column(&batch, T_MS)?
            .ok_or_else(|| ArrowError::SchemaError("t_ms column is missing".to_owned()))?;
        let offsets = uint64_column(&batch, OFFSET)?
            .ok_or_else(|| ArrowError::SchemaError("offset column is missing".to_owned()))?;
        let t_end_ms = int64_column(&batch, T_END_MS)?;
        let ends = t_end_ms.map(|column| column.values());
        Ok(BooleanArray::from_iter(
            t_ms.values()
                .iter()
                .zip(offsets.values().iter())
                .enumerate()
                .map(|(row, (t_ms, offset))| {
                    let t_end_ms = ends
                        .and_then(|ends| ends.get(row))
                        .copied()
                        .unwrap_or(*t_ms);
                    Some(filter.matches(*t_ms, t_end_ms, *offset))
                }),
        ))
    });
    let row_filter = RowFilter::new(vec![Box::new(predicate)]);
    let batches = builder
        .with_row_filter(row_filter)
        .build()?
        .try_collect::<Vec<_>>()
        .await?;
    let mut entries = Vec::new();
    for batch in &batches {
        append_record_batch(&mut entries, batch)?;
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
        append_record_batch(&mut entries, &batch?)?;
    }
    Ok(entries)
}

/// Columns are found by name. `t_ms`, `offset` and `len` are required;
/// `t_end_ms` defaults to `t_ms`, and unknown columns are ignored, so later
/// columns are additive.
pub(crate) fn validate(path: &Path) -> Result<(), IndexError> {
    let file = File::open(path)?;
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)?;
    let schema = builder.schema();
    let has = |name: &str, data_type: &DataType| {
        schema
            .field_with_name(name)
            .is_ok_and(|field| field.data_type() == data_type)
    };
    let valid = has(T_MS, &DataType::Int64)
        && has(OFFSET, &DataType::UInt64)
        && has(LEN, &DataType::UInt64)
        && (schema.field_with_name(T_END_MS).is_err() || has(T_END_MS, &DataType::Int64));
    if !valid {
        return Err(IndexError::InvalidPartSchema);
    }
    Ok(())
}

fn int64_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<Option<&'a Int64Array>, ArrowError> {
    batch
        .column_by_name(name)
        .map(|column| {
            column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| ArrowError::CastError(format!("{name} is not int64")))
        })
        .transpose()
}

fn uint64_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<Option<&'a UInt64Array>, ArrowError> {
    batch
        .column_by_name(name)
        .map(|column| {
            column
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| ArrowError::CastError(format!("{name} is not uint64")))
        })
        .transpose()
}

fn append_record_batch(
    entries: &mut Vec<EventEntry>,
    batch: &RecordBatch,
) -> Result<(), IndexError> {
    let missing = |name: &str| ArrowError::SchemaError(format!("{name} column is missing"));
    let t_ms = int64_column(batch, T_MS)?.ok_or_else(|| missing(T_MS))?;
    let offsets = uint64_column(batch, OFFSET)?.ok_or_else(|| missing(OFFSET))?;
    let lens = uint64_column(batch, LEN)?.ok_or_else(|| missing(LEN))?;
    let ends = int64_column(batch, T_END_MS)?.map(|column| column.values());
    entries.extend(
        t_ms.values()
            .iter()
            .zip(offsets.values().iter())
            .zip(lens.values().iter())
            .enumerate()
            .map(|(row, ((&t_ms, &offset), &len))| EventEntry {
                t_ms,
                t_end_ms: ends.and_then(|ends| ends.get(row)).copied().unwrap_or(t_ms),
                offset,
                len,
            }),
    );
    Ok(())
}
