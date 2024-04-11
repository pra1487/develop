"""
Spark-Optimizations:
--------------------

Resources - Memory(RAM), CPU cores (Compute)

Our intention is to make sure our spark job will get the right number/amount of resources

Executor/instance/container: combination of memory and computation which means RAM and cpu cores.

for example,
   - we have 10 worker nodes, each node is having 16 CPU cores and 64gb RAM.
   - here 1 node can hold morethan one executor.

There are two possibilities to create executors:
------------------------------------------------
    1. Thin executor
        - intention is to create more number of executors with minimul resources.
        - we can create 15 executors from each node in above example.
        - So, each executor will get 1 cpu core and 4gb of ram approximately
        - In this case, we will be lose multi-threading.
        - A lot of copies of broadcast variable required.

    2. Fat executor
        - intention is to create only one executor with maximum resources.
        - We can get 15 CPU cores and 63gb ram.
        - observed that if the executor holds more than 5 cores then the HDFS throughput will suffer.
        - If the executor is holding huge memory, then the garbage collections will takes time.
         (Garbage collection means removing unused objects from the memory)
        -

    3. Best approach
        - 1 Core is allocated for the internal process.
        - 1gb RAM also allocated for the operating system.
        - So, now each node is having 15 cores and 63gb RAM only.
        - 5 is the right choice of the number of CPU cores in each instance
        - So, we have 3 executors from each node with the above approach.
        - So, each executor should get 21gb of RAM.
        - Out of this 21gb memory, some of it will allocate fo the overhead memory(Off heap mempry)
            # it should be max(384mb, 7% of executor memory)
            # 7% means 1.5gb of overhead memory because this is greater then 384mb -- this is not part of container/executor memory
            # So, 21-1.5gb ~ 19gb is the actual executor memory
        - So, finally, each executor is having 19gb RAM and 5 CPU cores
        - finally, we are having totally 30 executors from the 10 worker node cluster.
        - 1 Executor out of these 30 executors will be given for the YARN Application manager.
        - So, Totaly we are having 29 Executors.

"""

"""
If we have a file in hdfs with 10.1gb size then start processing.

    - 81 partitions in our dataframe with default block size of 128mb

we requested 4 executors:
    - each one should have 5 cpu cores and 21gb RAM.
    - Total 20 CPU core and 84gb RAM.
    
    - here total 20 parallel task can able to run.
    - we are having 81 tasks to complete it.
    - 20 tasks will be complete in parallel in 10secs for example
    - 20 tasks will be in 10secs same as remaining as well.
    - 80 tasks will be done in 40secs
    - last 1 task can done in next 10 or less time.
    - So, total process will be done in 50decs around.
    
to check the spark default parallalisam:
    - spark.sparkContext.defaultParallelism

to check the dynamic resouce status:
--------------------------------------
    - Go to the environment tab in spark UI then search with dynamic then see the below configs.        
        spark.dynamicAllocaion.enabled = True
        spark.dynamicAllocation.maxExecutors = 10
        spark.dynamicAllocation.minexecutors = 2

to turn off the dynamic resource allocation:
--------------------------------------------
        - set the spark configuration with .config('spark.dynamicAllocation.enabled', False) in spark creation.
        - By default it is in True
        - if you turn off then request the required resourses like below
            .config('spark.executor.instances', '2')\
            .config('spark.executor.cores', '2')\
            .config('spark.executor.memory', '2g')\
      
to check the number of partitions on the dataframe:
---------------------------------------------------
        print(sc.defaultParallelism)
        print(df.rdd.getNumPartitions())

"""