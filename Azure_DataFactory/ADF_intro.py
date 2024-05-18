"""

Azure Datafactory:
===================

    - ADF is completely manages and serverless service in Azure.
    - ADF is platform as a service
    -

1. To Transfer the data from source to sink
    RDBMS --> ADLS
    ADLS --> RDBMS
    ...etc

2. Transformations:
    we do not write any code and we just do it graphically,
    than internally spark code will create and submitted on cluster.

3. Orchestration.
    - Scheduling
    - monitoring
    - triggering ...etc.

There are three main tabs are available in Azure Dashboard.

    i) Author
    ii) Monitor
    iii) Manage

Now we are going to do Source --> Sink
Source -> Azure SQL database
Sink -> ADLS Gne2

Step 1 - Connect to both source and sink, we can do this with "Linked Service.
        -> Go to manage button
        -> click the Linked service
Step 2 - create datasets from author tab for both source and destination to store the data.
        -> Go to author tab
        -> click on Datasets
        -> create
Step 3 - add these both linked services and datasets in the copy acitivity.
        -> Run the pipeline
        -> check the Sink location to see the data.



"""