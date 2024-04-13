"""
Spark_Memory_Management:
========================

- Actually, we will not be getting more memory than what maximum possible allocation memory from YARN scheduler.
- we can able to check this value with the "yarn.scheduler.maximum-allocation-mb" in the environment tab of the spark UI.
- If 8gb is the maximum allocation memory in yarn scheduler, but we will request 8gb as an executor memory from the spark-submit
   than an error will throw why because there is no chance to get more than 8gb.
    due to overhead memory (which is 10% of executor memory or 384mb) is always adds to requested executor memory.


- in the same way there is a limitation for the below configs as well.

    * yarn.scheduler.minimum-allocation-mb
    * yarn.scheduler.maximum-allocation-vcores
    * yarn.scheduler.minimum-allocation-vcores

Now coming to calculate the execution memory from the requested executor memory:

    - If we will request 2gb as an executor memory

        > 300mb reserved for the spark engine -> remaining 1.7gb
        > in this remaining 1.7gb
            - 60% unified memory (storage + execution)
            - 40% user memory - 700mb
    - finally:
        - 384mb which is max(384mb or 10% executor memory) for overhead memory
        - 300mb reserved memory
        - 1gb storage+execution memory
            * 500mb for storage
            * 500mb for execution
        - 700mb user memory (specially dedicated for RDD related operations)

    - execution memory can take some memory from storage memory provided it is free
    - storage memory can take some memory from execution memory provided it is free
"""