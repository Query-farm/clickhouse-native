use clickhouse_rs::{types::SqlType, Pool};
use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use std::{error::Error, sync::Arc};
use tokio::runtime::Runtime;

#[repr(C)]
struct ClickHouseScanBindData {
    url: String,
    user: String,
    password: String,
    query: String,
    column_names: Vec<String>,
    column_types: Vec<LogicalTypeId>,
}

#[repr(C)]
struct ClickHouseScanInitData {
    runtime: Option<Arc<Runtime>>,
    block_data: Option<Vec<Vec<String>>>,
    column_types: Vec<LogicalTypeId>,
    column_names: Vec<String>,
    current_row: usize,
    total_rows: usize,
    done: bool,
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

struct ClickHouseScanVTab;

impl VTab for ClickHouseScanVTab {
    type InitData = ClickHouseScanInitData;
    type BindData = ClickHouseScanBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let query = bind.get_parameter(0).to_string();
        
        // Check if a URL is provided directly
        let mut url = bind
            .get_named_parameter("url")
            .map(|v| v.to_string());
        
        // If no URL, try to build one from individual parameters
        if url.is_none() {
            let host = bind
                .get_named_parameter("host")
                .map(|v| v.to_string())
                .or_else(|| std::env::var("CLICKHOUSE_HOST").ok());
                
            let port = bind
                .get_named_parameter("port")
                .map(|v| v.to_string())
                .or_else(|| std::env::var("CLICKHOUSE_PORT").ok())
                .unwrap_or_else(|| "9000".to_string());
                
            let user = bind
                .get_named_parameter("user")
                .map(|v| v.to_string())
                .or_else(|| std::env::var("CLICKHOUSE_USER").ok())
                .unwrap_or_else(|| "default".to_string());
                
            let password = bind
                .get_named_parameter("password")
                .map(|v| v.to_string())
                .or_else(|| std::env::var("CLICKHOUSE_PASSWORD").ok())
                .unwrap_or_default();
                
            let database = bind
                .get_named_parameter("database")
                .map(|v| v.to_string())
                .or_else(|| std::env::var("CLICKHOUSE_DATABASE").ok());
            
            // Build URL from components if host is provided
            if let Some(ref h) = host {
                let auth = if !password.is_empty() {
                    format!("{}:{}@", user, password)
                } else if user != "default" {
                    format!("{}@", user)
                } else {
                    String::new()
                };
                
                let mut built_url = format!("tcp://{}{}:{}", auth, h, port);
                
                // Add database if provided
                if let Some(ref db) = database {
                    built_url = format!("{}/{}", built_url, db);
                }
                
                // Add query parameters like secure=true if provided
                let secure = bind
                    .get_named_parameter("secure")
                    .map(|v| v.to_string())
                    .or_else(|| std::env::var("CLICKHOUSE_SECURE").ok());
                    
                if let Some(ref s) = secure {
                    if s == "true" || s == "1" {
                        built_url = format!("{}?secure=true", built_url);
                    }
                }
                
                url = Some(built_url);
            }
        }
        
        // Fall back to environment variable or default
        let url = url.unwrap_or_else(|| {
            std::env::var("CLICKHOUSE_URL")
                .unwrap_or_else(|_| "tcp://localhost:9000".to_string())
        });
        
        let user = bind
            .get_named_parameter("user")
            .map(|v| v.to_string())
            .unwrap_or_else(|| {
                std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_string())
            });
        let password = bind
            .get_named_parameter("password")
            .map(|v| v.to_string())
            .unwrap_or_else(|| std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_default());

        let runtime = Arc::new(Runtime::new().map_err(|e| format!("Failed to create runtime: {}", e))?);

        let result = runtime.block_on(async {
            let pool = Pool::new(url.clone());
            let mut client = pool.get_handle().await?;
            let block = client.query(&query).fetch_all().await?;

            let columns = block.columns();
            let mut names = Vec::new();
            let mut types = Vec::new();

            for col in columns {
                names.push(col.name().to_string());
                types.push(map_clickhouse_type(col.sql_type()));
            }

            Ok::<(Vec<String>, Vec<LogicalTypeId>), Box<dyn Error>>((names, types))
        })?;

        let (names, types) = result;

        for (name, type_id) in names.iter().zip(types.iter()) {
            let logical_type = match type_id {
                LogicalTypeId::Integer => LogicalTypeId::Integer,
                LogicalTypeId::Bigint => LogicalTypeId::Bigint,
                LogicalTypeId::UInteger => LogicalTypeId::UInteger,
                LogicalTypeId::UBigint => LogicalTypeId::UBigint,
                LogicalTypeId::Float => LogicalTypeId::Float,
                LogicalTypeId::Double => LogicalTypeId::Double,
                LogicalTypeId::Varchar => LogicalTypeId::Varchar,
                LogicalTypeId::Date => LogicalTypeId::Date,
                LogicalTypeId::Timestamp => LogicalTypeId::Timestamp,
                LogicalTypeId::Boolean => LogicalTypeId::Boolean,
                _ => LogicalTypeId::Varchar,
            };
            let type_handle = LogicalTypeHandle::from(logical_type);
            bind.add_result_column(name, type_handle);
        }

        Ok(ClickHouseScanBindData {
            url,
            user,
            password,
            query,
            column_names: names,
            column_types: types,
        })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let bind_data = info.get_bind_data::<ClickHouseScanBindData>();
        let bind_data = unsafe { &*bind_data };

        let runtime = Arc::new(Runtime::new().map_err(|e| format!("Failed to create runtime: {}", e))?);

        let result = runtime.block_on(async {
            let pool = Pool::new(bind_data.url.clone());
            let mut client = pool.get_handle().await?;
            let block = client.query(&bind_data.query).fetch_all().await?;

            let columns = block.columns();
            let mut data: Vec<Vec<String>> = Vec::new();

            for _ in columns {
                data.push(Vec::new());
            }

            let mut row_count = 0;
            for row in block.rows() {
                for (col_idx, col) in columns.iter().enumerate() {
                    let value = match col.sql_type() {
                        SqlType::UInt8 => match row.get::<u8, &str>(col.name()) {
                            Ok(val) => val.to_string(),
                            Err(_) => "0".to_string(),
                        },
                        // ... rest of type handling ...
                        _ => match row.get::<String, &str>(col.name()) {
                            Ok(val) => val,
                            Err(_) => "0".to_string(),
                        },
                    };
                    data[col_idx].push(value);
                }
                row_count += 1;
            }

            Ok::<(Vec<Vec<String>>, usize), Box<dyn Error>>((data, row_count))
        })?;

        let (block_data, total_rows) = result;

        let column_types = bind_data.column_types.iter().map(|t| match t {
            LogicalTypeId::Integer => LogicalTypeId::Integer,
            LogicalTypeId::Bigint => LogicalTypeId::Bigint,
            LogicalTypeId::UInteger => LogicalTypeId::UInteger,
            LogicalTypeId::UBigint => LogicalTypeId::UBigint,
            LogicalTypeId::Float => LogicalTypeId::Float,
            LogicalTypeId::Double => LogicalTypeId::Double,
            LogicalTypeId::Varchar => LogicalTypeId::Varchar,
            LogicalTypeId::Date => LogicalTypeId::Date,
            LogicalTypeId::Timestamp => LogicalTypeId::Timestamp,
            LogicalTypeId::Boolean => LogicalTypeId::Boolean,
            _ => LogicalTypeId::Varchar,
        }).collect();
        let column_names = bind_data.column_names.iter().cloned().collect();

        Ok(ClickHouseScanInitData {
            runtime: Some(runtime),
            block_data: Some(block_data),
            column_types,
            column_names,
            current_row: 0,
            total_rows,
            done: false,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data() as *const ClickHouseScanInitData as *mut ClickHouseScanInitData;
        
        unsafe {
            if (*init_data).done || (*init_data).current_row >= (*init_data).total_rows {
                output.set_len(0);
                (*init_data).done = true;
                return Ok(());
            }

            let block_data = match (*init_data).block_data.as_ref() {
                Some(data) => data,
                None => return Err("Block data is not available".into()),
            };

            let batch_size = 1024.min((*init_data).total_rows - (*init_data).current_row);

            for col_idx in 0..(*init_data).column_types.len() {
                let mut vector = output.flat_vector(col_idx);
                let type_id = &(&(*init_data).column_types)[col_idx];

                match type_id {
                    LogicalTypeId::Integer | LogicalTypeId::UInteger => {
                        let slice = vector.as_mut_slice::<i32>();
                        for row_offset in 0..batch_size {
                            let row_idx = (*init_data).current_row + row_offset;
                            let val_str = &block_data[col_idx][row_idx];

                            let val = if let Ok(v) = val_str.parse::<i32>() {
                                v
                            } else if let Ok(v) = val_str.parse::<u32>() {
                                v as i32
                            } else if let Ok(v) = i32::from_str_radix(val_str.trim(), 10) {
                                v
                            } else {
                                0
                            };
                            slice[row_offset] = val;
                        }
                    }
                    LogicalTypeId::Bigint => {
                        let slice = vector.as_mut_slice::<i64>();
                        for row_offset in 0..batch_size {
                            let row_idx = (*init_data).current_row + row_offset;
                            if let Ok(val) = block_data[col_idx][row_idx].parse::<i64>() {
                                slice[row_offset] = val;
                            } else {
                                slice[row_offset] = 0;
                            }
                        }
                    }
                    _ => {
                        for row_offset in 0..batch_size {
                            let row_idx = (*init_data).current_row + row_offset;
                            let val = block_data[col_idx][row_idx].as_str();
                            vector.insert(row_offset, val);
                        }
                    }
                }
            }

            (*init_data).current_row += batch_size;
            output.set_len(batch_size);
        }
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

pub fn register_clickhouse_scan(con: &Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<ClickHouseScanVTab>("clickhouse_scan")?;
    Ok(())
}
