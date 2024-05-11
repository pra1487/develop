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


    Job Cluster:
    ------------
        -> No option to create manully.
        -> It will automatically terminate after the job complete.
        -> Suitable for automated workloads.
        -> Isolated for that particular job.
        -> cheaper in terms of cost.




"""