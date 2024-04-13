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

-  it is putting unnecessary overburden on task scheduler to process the empty partitions.
-


Normal join vs Broadcast Join:
==============================

To check the autoBroadcastJoinThreshold size with below conf.

    > spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

By default it is 10mb

which means, if any of the dataframe or table size is lesser than 10mb then spark will go and do the broadcast join.

To disable the broadcast join:

    > spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '-1')
    > orders_df.join(customer_df, orders_df.customer_id == customer_df.customer_id, 'inner')

- Here, the shuffle sort sortMergeJoin happens.
- because, here three stages will create, stage-1 is for read csv(orders), stage-2 is for read customers and then stage-3 is for join.
- in this join we have disabled the broadcast join config, so in the stage-1 most of the required data shuffle will be happen.
- then state-2 will have the next shuffle happen. then the stage-3 will be having the joining process.

TO enable to broadcast join:

    > spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '10485760b')
    > orders_df.join(customer_df, orders_df.customer_id == customer_df.customer_id, 'inner')






"""