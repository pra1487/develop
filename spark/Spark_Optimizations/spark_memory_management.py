"""
Spark_Memory_Management:
========================

- Actually, we will not be getting more memory than what maximum possible allocation memory from YARN.
- we can able to check this value with the "yarn.scheduler.maximum-allocation-mb" in the environment tab of the spark UI.
- If we will request 8gb as an executor memory from the spark-submit
    an error will throw because there is no chance to get more than 8gb
    due to overhead memory (which is 10% of executor memory or 384mb) is always adds to requested executor memory.




"""