"""
Process:
========
    1. Create the databricks workspace
    2. create the storage account
    3. create cluster in databricks workspace
    4. Do mount with storage account

Now:
---
    df = spark.read.csv("/mnt/retaildb/orders.csv", header=True)
    df.show()

write in parquet:

    df.write.mode('overwrite').partitionBy('order_status').format('parquet').save("/mnt/retaildb/parquet/orders_parquet/")

write in delta:

    df.write.mode('overwrite').partitionBy('order_status').format('delta').save('/mnt/retaildb/delta/orders_delta')
    -> here:'_delta_log' folder will be create along with all partition folders.


create tables on parquet and delta data:
========================================

parquet:
---------
        %sql
        create database if not exists retaildb

        %sql
        create table retaildb.ordersparquet using parquet location "/mnt/retaildb/parquet/orders_parquet/*"
            "Here do not need to mention schema because parquet files are already having the metadata"

        %sql
        select * from retaildb.ordersparquet limit 10;

delta:
------
        %sql
        create table retaildb.ordersdelta using delta location '/mnt/retaildb/delta/orders_delta/*'

        describe table extended retaildb.ordersdelta

- creating table along with writing df

        df.write.mode('overwrite').partitionBy('order_status').format('delta').\
        option('path', '/mnt/retaildb/delta_latest/orders_delta').\
        .saveAsTable('retaildb.orders_delta_table')

- Inserting data into delta table:

        > describe history retaildb.orders_delta
            - now we can see only one version with '0'
        > insert into retaildb.orders_delta values('111111111','2024-04-25 00:00:00.0','2222222','CLOSED')
        > describe history retaildb.orders_delta
            - now we can see two versions which are with '0' and '1'



"""