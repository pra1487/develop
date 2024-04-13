"""

Partition Skew:
===============

    - The name itself having meaning like partition is taking long time to process complete.
    - while doing joins, groupby like operation in some times, one of the key is having huge data needs to collect.
        in that time, all the respective data is keep coming to one partition.
    - it may occure out of memory issue also.

    -for example: if we are doing groupBy on order_status than COMPLETE key is having huge data
    than needs to convert the COMPLETE key into 10 different keys like COMPLETE1, COMPLETE2...
    so, spark will covert one COMPLETE partition into 10 different partitions.



AQE (Adaptive Query Execution):
===============================

    - Dynamically coalesing the number of shuffle partitions.
    - Dynamically handling the partition skew
    - Dynamically switching the join strategies

- During the shuffle it calculates the runtime stats.

To check the AQE enabled or not:

    > spark.conf.get("spark.sql.adaptive.enabled")
    - by default it is in False

    > spark.conf.set("spark.sql.adaptive.enabled", True)





"""