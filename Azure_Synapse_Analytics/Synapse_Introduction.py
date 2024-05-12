"""

Database --> - Day to day transactional data
             - Not for analysing historical data.
             - Perform analysis of data can overburden to database.
             - So, A data warehouse is specially designed for the analytics.
             - Keeping historical data in database is big challenge.

Azure synapse analytics:
-----------------------
        - It is much more than a data warehouse
        - Every synapse workspace should be associate with one storage account and root container.
        - It is creating managed resource group to itself provision some services.

What is Azure synapse analytics:
--------------------------------
    - It is unified analytics service which bring togather many capabilities like below.
                -> Data integration.
                -> Enterprise level data-warehousing
                -> Big data analytics.

    External source --> INGEST --> ADLS GEN2 --> COMPUTE

    INGESTION - synapse pipeline, mapping dataflow
    COMPUTATION - Dedicated sql pool, serverless sql pool, apache spark pool, mapping data flows

    Dedicated sql pool is more like redshift in aws.
    Serverless SQL pool is more like athena in aws (It charges $5 for 1 TB data scanned).
    Spark pool - it will process on spark cluster
    connected services power BI.


"""