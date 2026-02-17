use byteorder::{LittleEndian, ReadBytesExt};
use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use std::{
    error::Error,
    fs::File,
    io::{self, BufReader, Read, Seek},
};

mod clickhouse_scan;

#[allow(dead_code)]
#[derive(Debug)]
enum ColumnType {
    String,
    UInt8,
    UInt64,
    Int,
    Enum8(EnumType),
    Unsupported(String),
}

#[derive(Debug)]
enum ColumnData {
    String(String),
    UInt8(u8),
    UInt64(u64),
    Int(i32),
    Enum8(String),
}

#[derive(Debug)]
struct EnumValue {
    name: String,
    value: i8,
}

#[derive(Debug)]
struct EnumType {
    values: Vec<EnumValue>,
}

#[derive(Debug)]
struct Column {
    name: String,
    type_: ColumnType,
    data: Vec<ColumnData>,
}

#[derive(Debug)]
struct ClickHouseBindData {
    filepath: String,
}

#[derive(Debug)]
struct ClickHouseInitData {
    columns: Vec<Column>,
    current_row: std::sync::atomic::AtomicUsize,
    total_rows: usize,
    done: std::sync::atomic::AtomicBool,
}

fn read_string(reader: &mut impl Read) -> io::Result<String> {
    let len = read_var_u64(reader)? as usize;
    let mut buffer = vec![0; len];
    reader.read_exact(&mut buffer)?;
    Ok(String::from_utf8_lossy(&buffer)
        .replace('\0', "")
        .replace('\u{FFFD}', "")
        .to_string())
}

fn parse_enum_values(params: &str) -> Option<EnumType> {
    let inner = params.trim_matches(|c| c == '(' || c == ')').trim();

    if inner.is_empty() {
        return None;
    }

    let mut values = Vec::new();
    for pair in inner.split(',') {
        let parts: Vec<&str> = pair.split('=').collect();
        if parts.len() != 2 {
            continue;
        }

        let name = parts[0].trim().trim_matches('\'').to_string();

        if let Ok(value) = parts[1].trim().parse::<i8>() {
            values.push(EnumValue { name, value });
        }
    }

    if values.is_empty() {
        None
    } else {
        Some(EnumType { values })
    }
}

fn parse_column_type(type_str: &str) -> (ColumnType, Option<String>) {
    let params_start = type_str.find('(');
    let base_type = match params_start {
        Some(idx) => &type_str[..idx],
        None => type_str,
    };

    let params = params_start.map(|idx| {
        if type_str.ends_with(')') {
            type_str[idx..].to_string()
        } else {
            String::new()
        }
    });

    let column_type = match base_type {
        "String" => ColumnType::String,
        "UInt8" => ColumnType::UInt8,
        "UInt64" => ColumnType::UInt64,
        "Int" => ColumnType::Int,
        "Enum8" => {
            if let Some(ref p) = params {
                if let Some(enum_type) = parse_enum_values(p) {
                    ColumnType::Enum8(enum_type)
                } else {
                    ColumnType::Unsupported("Invalid Enum8".to_string())
                }
            } else {
                ColumnType::Unsupported("Invalid Enum8".to_string())
            }
        }
        other => ColumnType::Unsupported(other.to_string()),
    };

    (column_type, params)
}

fn read_column_data(
    reader: &mut impl Read,
    column_type: &ColumnType,
    rows: u64,
) -> io::Result<Vec<ColumnData>> {
    let mut data = Vec::with_capacity(rows as usize);
    for _ in 0..rows {
        let value = match column_type {
            ColumnType::UInt64 => {
                let val = reader.read_u64::<LittleEndian>()?;
                ColumnData::UInt64(val)
            }
            ColumnType::String => ColumnData::String(read_string(reader)?),
            ColumnType::UInt8 => ColumnData::UInt8(reader.read_u8()?),
            ColumnType::Enum8(enum_type) => {
                let val = reader.read_u8()?;
                let enum_str = enum_type
                    .values
                    .iter()
                    .find(|ev| ev.value == val as i8)
                    .map(|ev| ev.name.clone())
                    .unwrap_or_else(|| format!("Unknown({})", val));
                ColumnData::Enum8(enum_str)
            }
            ColumnType::Int => ColumnData::Int(reader.read_i32::<LittleEndian>()?),
            ColumnType::Unsupported(type_name) => {
                ColumnData::String(format!("<unsupported:{}>", type_name))
            }
        };
        data.push(value);
    }
    Ok(data)
}

fn read_var_u64(reader: &mut impl Read) -> io::Result<u64> {
    let mut x = 0u64;
    let mut shift = 0;

    for _ in 0..10 {
        let byte = reader.read_u8()?;
        x |= ((byte & 0x7F) as u64) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            return Ok(x);
        }
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "Invalid VarUInt",
    ))
}

fn read_native_format(reader: &mut BufReader<File>) -> io::Result<Vec<Column>> {
    let num_columns = read_var_u64(reader)?;
    let mut columns = Vec::new();
    let num_rows = read_var_u64(reader)?;

    for _ in 0..num_columns {
        let name = read_string(reader)?;
        let type_str = read_string(reader)?;
        let (column_type, _type_params) = parse_column_type(&type_str);
        let data = read_column_data(reader, &column_type, num_rows)?;
        columns.push(Column {
            name,
            type_: column_type,
            data,
        });
    }

    loop {
        let _pos = reader.stream_position()?;
        let block_columns = match read_var_u64(reader) {
            Ok(cols) => cols,
            Err(_) => break,
        };

        let block_rows = read_var_u64(reader)?;

        if block_rows == 0 {
            break;
        }

        for _ in 0..block_columns {
            let _ = read_string(reader)?;
            let _ = read_string(reader)?;
        }

        for col in &mut columns {
            let mut new_data = read_column_data(reader, &col.type_, block_rows)?;
            col.data.append(&mut new_data);
        }
    }

    Ok(columns)
}

struct ClickHouseVTab;

impl VTab for ClickHouseVTab {
    type InitData = ClickHouseInitData;
    type BindData = ClickHouseBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let filepath = bind.get_parameter(0).to_string();

        let file = File::open(&filepath)?;
        let mut reader = BufReader::with_capacity(64 * 1024, file);
        let columns = read_native_format(&mut reader)?;

        for column in &columns {
            let logical_type = match &column.type_ {
                ColumnType::String => LogicalTypeId::Varchar,
                ColumnType::UInt8 => LogicalTypeId::Integer,
                ColumnType::UInt64 => LogicalTypeId::Integer,
                ColumnType::Int => LogicalTypeId::Integer,
                ColumnType::Enum8(_) => LogicalTypeId::Varchar,
                ColumnType::Unsupported(_) => LogicalTypeId::Varchar,
            };
            bind.add_result_column(&column.name, LogicalTypeHandle::from(logical_type));
        }

        Ok(ClickHouseBindData { filepath })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let bind_data = info.get_bind_data::<ClickHouseBindData>();
        let filepath = unsafe { &(*bind_data).filepath };
        let file = File::open(filepath)?;
        let mut reader = BufReader::with_capacity(64 * 1024, file);

        let columns = read_native_format(&mut reader)?;
        let total_rows = if columns.is_empty() {
            0
        } else {
            columns[0].data.len()
        };

        Ok(ClickHouseInitData {
            columns,
            current_row: std::sync::atomic::AtomicUsize::new(0),
            total_rows,
            done: std::sync::atomic::AtomicBool::new(false),
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data();
        let current_row = init_data.current_row.load(std::sync::atomic::Ordering::Relaxed);

        if current_row >= init_data.total_rows || init_data.done.load(std::sync::atomic::Ordering::Relaxed) {
            output.set_len(0);
            init_data.done.store(true, std::sync::atomic::Ordering::Relaxed);
            return Ok(());
        }

        let batch_size = 1024.min(init_data.total_rows - current_row);

        for col_idx in 0..init_data.columns.len() {
            let column = &init_data.columns[col_idx];
            let mut vector = output.flat_vector(col_idx);

            match &column.type_ {
                ColumnType::String | ColumnType::Unsupported(_) => {
                    for row in 0..batch_size {
                        let data_idx = current_row + row;
                        match &column.data[data_idx] {
                            ColumnData::String(s) => {
                                let cleaned = s.replace('\0', "").replace('\u{FFFD}', "");
                                vector.insert(row, cleaned.as_str())
                            }
                            _ => vector.insert(row, "<invalid>"),
                        }
                    }
                }
                ColumnType::UInt8 => {
                    let slice = vector.as_mut_slice::<i32>();
                    for row in 0..batch_size {
                        let data_idx = current_row + row;
                        if let ColumnData::UInt8(v) = column.data[data_idx] {
                            slice[row] = v as i32;
                        }
                    }
                }
                ColumnType::Enum8(_) => {
                    for row in 0..batch_size {
                        let data_idx = current_row + row;
                        if let ColumnData::Enum8(ref s) = column.data[data_idx] {
                            vector.insert(row, s.as_str());
                        }
                    }
                }

                ColumnType::UInt64 => {
                    let slice = vector.as_mut_slice::<i32>();
                    for row in 0..batch_size {
                        let data_idx = current_row + row;
                        if let ColumnData::UInt64(v) = column.data[data_idx] {
                            slice[row] = v as i32;
                        }
                    }
                }
                ColumnType::Int => {
                    let slice = vector.as_mut_slice::<i32>();
                    for row in 0..batch_size {
                        let data_idx = current_row + row;
                        if let ColumnData::Int(v) = column.data[data_idx] {
                            slice[row] = v;
                        }
                    }
                }
            }
        }
        
        init_data.current_row.fetch_add(batch_size, std::sync::atomic::Ordering::Relaxed);
        output.set_len(batch_size);
        
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<ClickHouseVTab>("clickhouse_native")?;
    clickhouse_scan::register_clickhouse_scan(&con)?;
    Ok(())
}
