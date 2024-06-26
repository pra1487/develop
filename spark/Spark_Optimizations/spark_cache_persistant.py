"""
cache:
------
which means: keep something in memory(RAM) to make further usage without hitting the disk for the same operation.

ex:

df1 = read data from disk

df1.cache()

df2 = df1.transformations

df3 = df2.transformations

df4 = df3.trnasformations

df4.show()
or
df4.count()

here for the first time spark will hit the disk for the initial df1 creation
then process the remaining in memory(RAM).

but after cache the df1, that df1 will save in memory(RAM)
so, spark will take the im memory df1 for the next action calling processes.

cache is lazy, so, it will wait for one action than only cache will happen.

if we will reuse the df3 then better to make cache the df3 to cut down the previous process.
then for the df4 execution, spark will use the cached df3 to make it faster.

Notes:
    - we should cache the data frame which can be reuse
    - we should not cache the large dataframe because it may not be fit in memory, so we can cache the medioum level data frames which can easily fit into memory
    - cache is lazy, so cache will happen after call an action only.

cache default levels:
                         rdd - memory
                         dataframes - memory and disk (means first it will try to cache the data in memory(RAM), if it will not fit into memory then cache the remaining in disk)
                                    if in RDD the remaining data will be skipped what ever is not fit in memory.


persist: it will give little more flexible with respective to memory and disk locations to do cache/persist.
-------
        - default caching level is memory and disk
        - this storage level can be change by setting a perameter in persistant
        - when we will do the cache or persist then we can able to see the results on "storage" tab on spark UI (when is the application is running)
        - we can not able to see this results after complete the job in history server.

spark-UI:
---------
        - we can able to see the cache results in storage tab of the spark UI.
        - cache is lazy, so waiting for the action call.

                order_df_cached = orders_df.cache()

        - show is an action to display the first 20 rows of the df, so, spark will cache that 20 records of the dataframe.
        - if will do count action, then all the partitions will be cached.
        - if we will not assign to another variable then it will works like a .show() then it will cached the first partition.

            df.select('order_id', 'order_status').filter("order_status == 'closed'").cache()

        - if we need to cache the total filtered dataframe then needs to execute a count() action.

        - this is cached in storage level Memory Deserialized 1x Replicated.

        - if the Analyzed Logical plans are not same then that will not consider the cached data.

        serialized:
                    Binary format (which will be more optiized in terms of space, will take less space to store)
                    in takes extra cpu cycles for the format conversion.

        Deserialized:
                    Keeping it in object form, takes slightly more space
                    in terms of computation it is fast

        On disk - the data is always kept in serialized format
        on RAM - the data is kept in deserialized format.

cache spark table :

    spark.sql("cache table db.table_name")

"""