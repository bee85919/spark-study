import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().config(LocalSparkConf.get("ContextApp")).enableHiveSupport().getOrCreate()