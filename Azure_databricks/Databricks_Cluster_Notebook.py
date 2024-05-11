"""
Azure Databricks:
=================

The main fundamental units of the Databricks are:
    1. Cluster - we can able to create cluster from create option or compute option.
    2. Notebook - we can able to create notebook from create option.

Cluster:
--------    There are three types of clusters available
                1. All purpose cluster - Interactive purpose
                2. job cluster - During the job run the cluster will be created and once the job finishes it terminates.
                3. pool -

    All purpose cluster:
    --------------------
        -> Needs to create manually.
        -> It will run until will terminate.
        -> Suitable for interactive workloads.
        -> Shared among many users, Which means can able to use many users.
        -> Little expensive to run (we have to pay from the create time)
        -> We can able to choose the runtime version of Databricks (spark)
        -> We can able to create with terminate after time in minutes. it can help save cost.
        -> There is chance to choose worker types, driver types and min workers, max workers.
        -> Here are the worker types: generally we will go with mwmory, compute worker types.

                * memory optimized: when we need more memory to cach lot of data volumes.
                * compute optimized: when we required more faster computation. ex: streaming work loads.
                * storage optimized: High disk throughputs.
                * general optimized:


    Job Cluster:
    ------------
        -> No option to create manully.
        -> It will automatically terminate after the job complete.
        -> Suitable for automated workloads.
        -> Isolated for that particular job.
        -> cheaper in terms of cost.

Cluster Modes:
==============
    There are three cluster modes are available here.

        1. High concurrency: Optimized to run concurrent SQL, Python and R worklads. Does not support Scala.

        2. Standard:
            -> Recommended for single-user clusters. can run SQL, Python, R and scala workloads.
            -> Can have multiple workers.

        3. Single Node: Cluster with no workers. Recommanded for single-user cluster computing on small data volues.


Notebook:
--------
    - We can able to create notebook from create option and from workspace --> shared/users --> select user--> from here.
    - we can able to Enter the notebook name, select Default language, select cluster in create UI.
    - We can able to write our code in this notebook.
    - we can upload a file to file systerm (dbfs:/FileStore/orders.csv)
    - we can run the code on attached cluster.
    - we are having Detach, Restart Cluster, Detach & Re-attach, Spark UI, Driver logs options in dropdown of the cluster.
    - we can do documentation with %md
    - we can write other language with magic commands like %sql, %scala .. etc
    - %fs for dbfs (ls /FileStore).



"""