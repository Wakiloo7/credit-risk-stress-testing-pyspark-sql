from pyspark.sql import SparkSession


def get_spark_session(app_name: str = 'CreditRiskStressTestingPipeline') -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master('local[*]')
        .config('spark.sql.shuffle.partitions', '8')
        .config('spark.sql.execution.arrow.pyspark.enabled', 'true')
        .getOrCreate()
    )
