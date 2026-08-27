use clickhouse_rs::{
    types::{ColumnType, SqlType},
    Pool,
};
use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use std::{
    error::Error,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::runtime::Runtime;

struct ClickHouseScanBindData {
    url: String,
    query: String,
    column_types: Vec<LogicalTypeId>,
}

struct ClickHouseScanInitData {
    block_data: Vec<Vec<String>>,
    column_types: Vec<LogicalTypeId>,
    current_row: AtomicUsize,
    total_rows: usize,
    done: AtomicBool,
}

fn map_clickhouse_type(sql_type: SqlType) -> LogicalTypeId {
    match sql_type {
        SqlType::Int8 | SqlType::Int16 | SqlType::Int32 => LogicalTypeId::Integer,
        SqlType::Int64 => LogicalTypeId::Bigint,
        SqlType::UInt8 | SqlType::UInt16 | SqlType::UInt32 => LogicalTypeId::UInteger,
        SqlType::UInt64 => LogicalTypeId::UBigint,
        SqlType::Float32 => LogicalTypeId::Float,
        SqlType::Float64 => LogicalTypeId::Double,
        SqlType::String | SqlType::FixedString(_) => LogicalTypeId::Varchar,
        SqlType::Date => LogicalTypeId::Date,
        SqlType::DateTime(_) => LogicalTypeId::Timestamp,
        SqlType::Bool => LogicalTypeId::Boolean,
        _ => LogicalTypeId::Integer,
    }
}

fn cell_to_string<'a, K: ColumnType>(
    sql_type: SqlType,
    row: &'a clickhouse_rs::types::Row<'a, K>,
    name: &str,
) -> String {
    match sql_type {
        SqlType::UInt8 => row.get::<u8, _>(name).map(|v| v.to_string()).unwrap_or_else(|_| "0".into()),
        SqlType::UInt16 => row.get::<u16, _>(name).map(|v| v.to_string()).unwrap_or_else(|_| "0".into()),
        SqlType::UInt32 => row.get::<u32, _>(name).map(|v| v.to_string()).unwrap_or_else(|_| "0".into()),
        SqlType::UInt64 => row.get::<u64, _>(name).map(|v| v.to_string()).unwrap_or_else(|_| "0".into()),
        SqlType::Int8 => row.get::<i8, _>(name).map(|v| v.to_string()).unwrap_or_else(|_| "0".into()),
        SqlType::Int16 => row.get::<i16, _>(name).map(|v| v.to_string()).unwrap_or_else(|_| "0".into()),
        SqlType::Int32 => row.get::<i32, _>(name).map(|v| v.to_string()).unwrap_or_else(|_| "0".into()),
        SqlType::Int64 => row.get::<i64, _>(name).map(|v| v.to_string()).unwrap_or_else(|_| "0".into()),
        SqlType::Float32 => row.get::<f32, _>(name).map(|v| v.to_string()).unwrap_or_else(|_| "0.0".into()),
        SqlType::Float64 => row.get::<f64, _>(name).map(|v| v.to_string()).unwrap_or_else(|_| "0.0".into()),
        SqlType::String | SqlType::FixedString(_) => row.get::<String, _>(name).unwrap_or_default(),
        SqlType::Bool => row.get::<bool, _>(name).map(|v| v.to_string()).unwrap_or_else(|_| "false".into()),
        SqlType::Date => row.get::<String, _>(name).unwrap_or_else(|_| "1970-01-01".into()),
        SqlType::DateTime(_) => row.get::<String, _>(name).unwrap_or_else(|_| "1970-01-01 00:00:00".into()),
        _ => row.get::<String, _>(name).unwrap_or_else(|_| "0".into()),
    }
}

fn fetch_schema(url: &str, query: &str) -> Result<(Vec<String>, Vec<LogicalTypeId>), Box<dyn Error>> {
    let runtime = Runtime::new().map_err(|e| format!("Failed to create runtime: {e}"))?;
    runtime.block_on(async {
        let pool = Pool::new(url.to_string());
        let mut client = pool.get_handle().await?;
        let block = client.query(query).fetch_all().await?;

        let mut names = Vec::new();
        let mut types = Vec::new();
        for col in block.columns() {
            names.push(col.name().to_string());
            types.push(map_clickhouse_type(col.sql_type()));
        }
        Ok::<_, Box<dyn Error>>((names, types))
    })
}

fn fetch_block(url: &str, query: &str) -> Result<(Vec<Vec<String>>, usize), Box<dyn Error>> {
    let runtime = Runtime::new().map_err(|e| format!("Failed to create runtime: {e}"))?;
    runtime.block_on(async {
        let pool = Pool::new(url.to_string());
        let mut client = pool.get_handle().await?;
        let block = client.query(query).fetch_all().await?;

        let columns = block.columns();
        let mut data: Vec<Vec<String>> = vec![Vec::new(); columns.len()];
        let mut row_count = 0;

        for row in block.rows() {
            for (col_idx, col) in columns.iter().enumerate() {
                data[col_idx].push(cell_to_string(col.sql_type(), &row, col.name()));
            }
            row_count += 1;
        }

        Ok::<_, Box<dyn Error>>((data, row_count))
    })
}

struct ClickHouseScanVTab;

impl VTab for ClickHouseScanVTab {
    type InitData = ClickHouseScanInitData;
    type BindData = ClickHouseScanBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let query = bind.get_parameter(0).to_string();
        let url = bind
            .get_named_parameter("url")
            .map(|v| v.to_string())
            .unwrap_or_else(|| {
                std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "tcp://localhost:9000".to_string())
            });
        let _user = bind
            .get_named_parameter("user")
            .map(|v| v.to_string())
            .unwrap_or_else(|| {
                std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_string())
            });
        let _password = bind
            .get_named_parameter("password")
            .map(|v| v.to_string())
            .unwrap_or_else(|| std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_default());

        let (names, types) = fetch_schema(&url, &query)?;

        for (name, type_id) in names.iter().zip(types.iter()) {
            bind.add_result_column(name, LogicalTypeHandle::from(*type_id));
        }

        Ok(ClickHouseScanBindData {
            url,
            query,
            column_types: types,
        })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let bind_data = unsafe { info.get_bind_data::<ClickHouseScanBindData>().as_ref() }
            .ok_or("ClickHouse bind data is missing")?;

        let (block_data, total_rows) = fetch_block(&bind_data.url, &bind_data.query)?;

        Ok(ClickHouseScanInitData {
            block_data,
            column_types: bind_data.column_types.clone(),
            current_row: AtomicUsize::new(0),
            total_rows,
            done: AtomicBool::new(false),
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data();
        let current_row = init_data.current_row.load(Ordering::Relaxed);

        if current_row >= init_data.total_rows || init_data.done.load(Ordering::Relaxed) {
            output.set_len(0);
            init_data.done.store(true, Ordering::Relaxed);
            return Ok(());
        }

        let batch_size = 1024.min(init_data.total_rows - current_row);

        for (col_idx, type_id) in init_data.column_types.iter().enumerate() {
            let mut vector = output.flat_vector(col_idx);

            match type_id {
                LogicalTypeId::Integer | LogicalTypeId::UInteger => {
                    let slice = unsafe { vector.as_mut_slice::<i32>() };
                    for row_offset in 0..batch_size {
                        let val_str = &init_data.block_data[col_idx][current_row + row_offset];
                        slice[row_offset] = val_str.parse::<i32>().or_else(|_| val_str.parse::<u32>().map(|v| v as i32)).unwrap_or(0);
                    }
                }
                LogicalTypeId::Bigint | LogicalTypeId::UBigint => {
                    let slice = unsafe { vector.as_mut_slice::<i64>() };
                    for row_offset in 0..batch_size {
                        let val_str = &init_data.block_data[col_idx][current_row + row_offset];
                        slice[row_offset] = val_str.parse::<i64>().or_else(|_| val_str.parse::<u64>().map(|v| v as i64)).unwrap_or(0);
                    }
                }
                _ => {
                    for row_offset in 0..batch_size {
                        vector.insert(row_offset, init_data.block_data[col_idx][current_row + row_offset].as_str());
                    }
                }
            }
        }

        init_data.current_row.fetch_add(batch_size, Ordering::Relaxed);
        output.set_len(batch_size);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(vec![
            ("url".to_string(), LogicalTypeHandle::from(LogicalTypeId::Varchar)),
            ("user".to_string(), LogicalTypeHandle::from(LogicalTypeId::Varchar)),
            ("password".to_string(), LogicalTypeHandle::from(LogicalTypeId::Varchar)),
        ])
    }
}

pub fn register_clickhouse_scan(con: &Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<ClickHouseScanVTab>("clickhouse_scan")?;
    Ok(())
}
