<a href="https://community-extensions.duckdb.org/extensions/chsql.html" target="_blank">
<img src="https://github.com/user-attachments/assets/9003897d-db6f-4a79-9443-9b72766b511b" width=200>
</a>

# DuckDB Clickhouse Native Extension for [chsql](https://github.com/quackscience/duckdb-extension-clickhouse-sql)
Experimental ClickHouse Native Client and Native file reader for DuckDB chsql


### 📦 Installation
```sql
INSTALL chsql_native FROM community;
LOAD chsql_native;
```

## 🤖 Native Client
The extension provides an experimental clickhouse native client: `clickhouse_scan`
### 🏁 Settings
```bash
# Local Setup, Insecure
export CLICKHOUSE_URL="tcp://localhost:9000"
# Remote Setup, Secure
export CLICKHOUSE_URL="tcp://user:pass@remote:9440/?secure=true&skip_verify=true"
```
### ✏️ Usage
```sql
D SELECT * FROM clickhouse_scan("SELECT version(), 'hello', 123");
┌────────────┬─────────┬────────┐
│ version()  │ 'hello' │  123   │
│  varchar   │ varchar │ uint32 │
├────────────┼─────────┼────────┤
│ 24.10.2.80 │ hello   │    123 │
└────────────┴─────────┴────────┘
```

## 🤖 Native Reader
The extension provides an experimental clickhouse native file reader: `clickhouse_native`

### 🏁 Input
Generate some native files with `clickhouse-local` or `clickhouse-server`

```sql
--- simple w/ one row, two columns
SELECT version(), number FROM numbers(1) INTO OUTFILE '/tmp/numbers.clickhouse' FORMAT Native;
--- simple w/ one column, 100000 rows
SELECT number FROM numbers(100000) INTO OUTFILE '/tmp/100000.clickhouse' FORMAT Native;
--- complex w/ multiple types
SELECT * FROM system.functions LIMIT 10 INTO OUTFILE '/tmp/functions.clickhouse' FORMAT Native;
```

### ✏️ Usage
Read ClickHouse Native files with DuckDB. Reads are full-scans at this time.

```sql
D SELECT * FROM clickhouse_native('/tmp/numbers.clickhouse');
┌──────────────┬─────────┐
│  version()   │ number  │
│   varchar    │  int32  │
├──────────────┼─────────┤
│ 24.12.1.1273 │ 0       │
└──────────────┴─────────┘
```
```sql
D SELECT count(*), max(number) FROM clickhouse_native('/tmp/100000.clickhouse');
┌──────────────┬─────────────┐
│ count_star() │ max(number) │
│    int64     │    int32    │
├──────────────┼─────────────┤
│       100000 │       99999 │
└──────────────┴─────────────┘
```
```sql
D SELECT * FROM clickhouse_native('/tmp/functions.clickhouse') WHERE alias_to != '' LIMIT 10;
┌────────────────────┬──────────────┬──────────────────┬──────────────────────┬──────────────┬─────────┬───┬─────────┬───────────┬────────────────┬──────────┬────────────┐
│        name        │ is_aggregate │ case_insensitive │       alias_to       │ create_query │ origin  │ … │ syntax  │ arguments │ returned_value │ examples │ categories │
│      varchar       │    int32     │      int32       │       varchar        │   varchar    │ varchar │   │ varchar │  varchar  │    varchar     │ varchar  │  varchar   │
├────────────────────┼──────────────┼──────────────────┼──────────────────────┼──────────────┼─────────┼───┼─────────┼───────────┼────────────────┼──────────┼────────────┤
│ connection_id      │            0 │                1 │ connectionID         │              │ System  │ … │         │           │                │          │            │
│ rand32             │            0 │                0 │ rand                 │              │ System  │ … │         │           │                │          │            │
│ INET6_ATON         │            0 │                1 │ IPv6StringToNum      │              │ System  │ … │         │           │                │          │            │
│ INET_ATON          │            0 │                1 │ IPv4StringToNum      │              │ System  │ … │         │           │                │          │            │
│ truncate           │            0 │                1 │ trunc                │              │ System  │ … │         │           │                │          │            │
│ ceiling            │            0 │                1 │ ceil                 │              │ System  │ … │         │           │                │          │            │
│ replace            │            0 │                1 │ replaceAll           │              │ System  │ … │         │           │                │          │            │
│ from_utc_timestamp │            0 │                1 │ fromUTCTimestamp     │              │ System  │ … │         │           │                │          │            │
│ mapFromString      │            0 │                0 │ extractKeyValuePairs │              │ System  │ … │         │           │                │          │            │
│ str_to_map         │            0 │                1 │ extractKeyValuePairs │              │ System  │ … │         │           │                │          │            │
├────────────────────┴──────────────┴──────────────────┴──────────────────────┴──────────────┴─────────┴───┴─────────┴───────────┴────────────────┴──────────┴────────────┤
│ 10 rows                                                                                                                                           12 columns (11 shown) │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

#### Notes

> The reader is a clear room implementation of the ClickHouse Native file format using no code or libraries from ClickHouse Inc. As such it is potentially incomplete, imperfect and might not be compatible with all files. USE AT YOUR OWN RISK!

### 🐎 Performance
Simple CLI _cold start_ count() test using `duckdb` vs. `clickhouse-local` and 1M rows
#### DuckDB
```sql
# time duckdb -c "LOAD chsql_native; SELECT count(*) FROM clickhouse_native('/tmp/1M.clickhouse');"
┌──────────────┐
│ count_star() │
│    int64     │
├──────────────┤
│      1000000 │
└──────────────┘

real	0m0.095s
user	0m0.077s
sys	0m0.029s
```
#### clickhouse-local
```sql
# time clickhouse local "SELECT count(*) FROM '/tmp/1M.clickhouse'";
1000000

real	0m0.141s
user	0m0.086s
sys	0m0.043s
```

<br>

### ⛑️ Extension Status
- [x] Basic Fomat Reading
  - [x] Column Extraction
  - [x] Blocks Parser & Iterator
  - [x] Type Mapping WIP
    - [x] Strings
    - [x] Integers
    - [x] Enums
    - [ ] ??? as String
  - [ ] Compression support
- [x] Basic Native Client
  - [x] clickhouse-rs binding
  - [x] TLS Support
  - [x] Type Mapping WIP
    - [x] Strings
    - [x] Integers
    - [ ] Everything Else

<br>


### ⚙️ Dev Build
You can easily modify the code and build a local extension for testing and development.

#### Requirements
- Rust

1) Clone and Compile the extension on your system

```bash
cd /usr/src
git clone --recurse-submodules https://github.com/quackscience/duckdb-extension-clickhouse-native
cd duckdb-extension-clickhouse-native
make configure && make
```

2) Download and Run DuckDB with -unsigned
```
wget https://github.com/duckdb/duckdb/releases/download/v1.1.3/duckdb_cli-linux-amd64.zip && unzip duckdb_cli-linux-amd64.zip
./duckdb -unsigned
```

3) Load your local extension build
```sql
D LOAD '/usr/src/duckdb-extension-clickhouse-native/build/debug/clickhouse_native.duckdb_extension';
```


----

###### Disclaimer
> DuckDB ® is a trademark of DuckDB Foundation.
> ClickHouse® is a trademark of ClickHouse Inc. All trademarks, service marks, and logos mentioned or depicted are the property of their respective owners. The use of any third-party trademarks, brand names, product names, and company names is purely informative or intended as parody and does not imply endorsement, affiliation, or association with the respective owners.
