# Hive

> Hive source connector

## Description

Read data from Hive.

:::tip

In order to use this connector, You must ensure your spark/flink cluster already integrated hive. The tested hive version is 2.3.9.

If you use SeaTunnel Engine, You need put seatunnel-hadoop3-3.1.4-uber.jar and hive-exec-2.3.9.jar in $SEATUNNEL_HOME/lib/ dir.
:::

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)

Read all the data in a split in a pollNext call. What splits are read will be saved in snapshot.

- [x] [schema projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)
- [x] file format
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json

## Options

| name                 | type   | required | default value |
|----------------------|--------|----------|---------------|
| table_name           | string | yes      | -             |
| metastore_uri        | string | yes      | -             |
| kerberos_principal   | string | no       | -             |
| kerberos_keytab_path | string | no       | -             | 
| common-options       |        | no       | -             |

### table_name [string]

Target Hive table name eg: db1.table1

### metastore_uri [string]

Hive metastore uri

### common options 

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details

## Example

```bash

  Hive {
    table_name = "default.seatunnel_orc"
    metastore_uri = "thrift://namenode001:9083"
  }

```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Hive Source Connector

### Next version

- [Improve] Support kerberos authentication ([3840](https://github.com/apache/incubator-seatunnel/pull/3840))
