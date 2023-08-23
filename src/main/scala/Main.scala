import org.apache.spark.sql.SparkSession

object Main extends App {
//  val sparkHome = System.getenv("SPARK_HOME")
  val logDir = s"file:/Users/b06/spark-3.4.1-bin-hadoop3/event"
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkExample")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", logDir)
    .getOrCreate();

  println("First SparkContext:")
  println("APP Name :" + spark.sparkContext.appName);
  println("Deploy Mode :" + spark.sparkContext.deployMode);
  println("Master :" + spark.sparkContext.master);
  val df = spark.createDataFrame(
    List(("Scala", 25000), ("Spark", 35000), ("PHP", 21000)))
  df.show()
  df.count()

  val sparkSession2 = SparkSession.builder()
    .master("local[1]")
    .appName("SparkExample-2")
    .config("spark.eventLog.enabled", true)
    .config("spark.eventLog.dir", logDir)
    .getOrCreate();

  println("Second SparkContext:")
  println("APP Name :" + sparkSession2.sparkContext.appName);
  println("Deploy Mode :" + sparkSession2.sparkContext.deployMode);
  println("Master :" + sparkSession2.sparkContext.master);
}
