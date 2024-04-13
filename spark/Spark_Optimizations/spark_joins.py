"""
Internals of groupby():
======================

orders_df = spark.read.format('csv').schema(orders_schema).load("file:///D://data/orders_1gb.csv")
orders_df.rdd.getNumPartitions()
>9

orders_df.groupBy('orders_status').count().write.format('csv').mode('overwrite').save('file:///D://data/output')

- whenever we call a wide transformation / whenever shuffle happens we get 200 shuffle partitions.
- since we have 9 different order_Satus keys, will have at the max of 9 shuffle partitions only which will have some data.
- remaining 191 partitions will be empty.
- if there will be 400 different keys in the order_status, then all the 200 partitions will be having some data.

- local aggregation will be happen before shuffle the data.
- which means

    p1: closed-2000
        pending-1500
        inprogress- 2000

    p2: pending - 3000
        closed - 4000
    .
    .
    .
    p9

after the above local aggregation, we have 9 different keys, so each key count will go to one partition.
So, 9 partitions will consume the data, remaining 191 would be empty.





"""