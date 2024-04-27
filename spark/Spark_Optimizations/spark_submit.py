"""
spark-submit \
    --deploy-mode cluster \
    --master yarn \
    --num-executors 1 \
    --executor-cores 2 \
    --executor-memory 1G \
    --driver-memory 2G \
    --driver-cores 2 \
    --conf spark.dynamicAllocation.enabled = False \
    prog1.py

Note: - If the driver run on one of the worker node that will be the cluster mode.
      - If the driver will run on the gateway node that will be the client mode.
      - by default that will consider client mode.

      - The configurations will be given in the SparkSession as well.
      - SparkSession configs will be the first preference
      - spark-submit configs will be the second preference

"""