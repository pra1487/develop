"""
How many initial number of partitions in the dataframe:
-------------------------------------------------------

Let's consider a scenario, we have a 1.1gb of file:

    - Basically, if the partition size is big we will get less number of partitions and will get more number of partitions with less size.
    - So, spark will decide the initial number of partitions while creating the dataframe to process the data.
    - So, the partition size should be min (128mb, file-size/total cores)

let's consider scenario-1:
==========================

        - we have 1.1gb of file
        - we have 2 executors with 1gb RAM and 1 CPU core.
    Now partition size will be min(128mb, 1.1gb/2) = min(9, 2)
    So, now spark will create 9 number of initial partitions while creating the dataframe on 1.1gb dof data file

spark.sql.files.maxPartitionsBytes = 128m

we can change this as per our requirement.

                The generalized formula for this calculation is min(maxPartitionBytes, file_size/defaultParallelism)
                ====================================================================================================

let's consider scenario-2:
==========================

            - we have a same 1.1gb of file
            - we have 8 executors with 2gb RAM and 2 CPU cores
            - total number of cores are 16, so, default parallelism is 16

    Now partition size will be min (128mb, 1.1gb/16) = min(128mb, 75mb) = min(9, 16)
    Here, the minimum is 75mb, so spark will create 16 number of initial number partitions while creating the dataframe on 1.1gb of data file.
    So, spark can able to process all the 16 partitions in parallel.


let's consider scenario-3:
==========================

        - single Non-Splittable file.
        - if some time csv will compressed with snappy or gzip then that file will be not splittable.
        - parquest is splittable with snappy.
        - So, if the csv file comes with the above compression then that will not be splittable
            then spark will be considered as a single partition only.
            - If the execution memory is not sufficient to process the above file, will throw the OOM issue.

orders_df.rdd.getNumPartitions()
new_orders_df = orders_df.repartition(1)
new_orders_df.rdd.getNumPartitions()

new_orders_df.write \
.format('csv') \
.mode('overwrire') \
.option('codec', 'org.apache.hadoop.io.compress.GzipCodec') \
.save('orders_gz')


new_orders_df.write \
.format('csv') \
.mode('overwrire') \
.option('codec', 'snappy') \
.save('orders_snappy')


let's consider scenario-3:
==========================

        - we are having multiple files in input location.
        - how the spark will calculate initial number of partitions.
        - if the input location is having 100 files with each 50mb then spark will create 50 partitions.
            due to not exceeds the partition size of 128mb
        - in this process, spark will merge two two files into one partition.




"""