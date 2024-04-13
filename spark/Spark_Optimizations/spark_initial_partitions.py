"""
How many initial number of partitions in the dataframe:
-------------------------------------------------------

Let's consider a scenario, we have a 1.1gb of file:

    - Basically, if the partition size is big we will get less number of partitions and will get more number of partitions with less size.
    - So, spark will decide the initial number of partitions while creating the dataframe to process the data.
    - So, the partition size should be min (128mb, file-size/total cores)

lets consider:
        - we have 1.1gb of file
        - we have 2 executors with 1gb RAM and 1 CPU core.
    Now partition size will be min(128mb, 1.1gb/2) = min(9, 2)
    So, now spaek will create 9 number of initial partitions while creating the dataframe on 1.1gb dof data file

spark.sql.files.maxPartitionsBytes = 128m

we can change this as per our requirement.



"""