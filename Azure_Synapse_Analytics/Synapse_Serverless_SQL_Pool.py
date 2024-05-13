"""

Serverless SQL Pool:
====================

    - In synapse work space there is a "Data" tab.
    - In this tab there are "Workspace" and "Linked" tabs are available.
    - In this Workspace, we can see the databases here.
    - In this Linked tab, we can see the linked storage account which is ADLS GEN2 containers.
    - We can do upload a file from here and query that data as well.
    - we can directly query the file from here without create any table like below.
            - right-click on the uploaded file
                -> New SQL script
                -> Select Top 100 rows.
                -> select script will generate
                -> click on "Run".

Normal Table:
=============   DWH stores the metadata and Actual data.


External Table: (Metadata 'DWH' + Data 'Datalake')
===============
    - Metadata stored in DWH
    - Actual data stored in ADLS GEN2
    - In case of serverless SQL pool, we can only have an external table.
    - We can never have normal table.

Now go to manage tab on home page of synapse workspace for SQL pool.
 - There is a Built-in sql pool available here this is serverless SQL pool.
 - we don't need to create separately
 - Charge $5 per 1TB data.

Now go to Data tab on workspace.
    - Now, i am not having any databases in Workspace tab.
    - So, we can create external tables...lets see

Create External Table:
----------------------

Template:
    create external table orders (order_id int, ...etc)
    with (
    Location= "/xyz/orders.csv"
    DATA_SOURCE = ""
    FILE_FORMAT =
    )

-> Now go to "Develop" tab which is below the "Data" tab.
-> click on +
-> select SQL Script
-> Check the "Connect to" on top right. which should have "Built-in"

        > CREATE DATABASE salesdb_dev

-> Now change the database name in "Use database" dropdown available on next to "Connect to".
-> Now, we have to create a datasource, file format for create external table.

creating external datasource:

    CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Hai@4433#12'

    CREATE DATABASE SCOPED CREDENTIAL Sasoken
    WITH IDENTITY = 'SHARED ASSESS SIGNATURE',
    SECRET = '<copy sas Key>'

    CREATE EXTERNAL DATA SOURCE ExtDataSrc
    WITH (LOCATION = 'https://trendytechsa101.dfs.core.windows.net/data',
            CREDENTIAL= SasToken)

Creating file format:

    CREATE EXTERNAL FILE FORMAT <NAME> WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS(
    FIELD_TERMINATOR = ','
    FIRST_ROW = 2
    ))

Now create the external table:

    create external table orders (order_id int, order_date varchar(30), customer_id bigint, order_status varchar(30))
    with (
    Location= "orders.csv"
    DATA_SOURCE = "ExtDataSrc"
    FILE_FORMAT = <Name given in above>
    )
    GO

Another way to create external table:

        go to linked tab on data panel of home page.
        -> select data
        -> right click on dataset.
            -> select "New SQL Script"
            -> Create External Table
            -> fill the respected details
            -> click on generate script


"""