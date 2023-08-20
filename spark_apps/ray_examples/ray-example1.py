from pyspark.sql import SparkSession
from ray.util.spark import setup_ray_cluster, shutdown_ray_cluster
import ray

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Ray on spark example 1") \
        .config("spark.task.cpus", "4") \
        .getOrCreate()

    setup_ray_cluster(num_worker_nodes=1)

    ray.init()

    print("Hello from Aryaman!")

    shutdown_ray_cluster()

    spark.stop()


