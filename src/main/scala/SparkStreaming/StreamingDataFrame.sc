/*
import org.apache.spark.sql.SparkSession

object Main extends App {
  val sparkHome = System.getenv("SPARK_HOME")
  val logDir = s"file:${sparkHome}/event"
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
*/


val dataPath = System.getenv("SPARK_DATA")

// Read text from socket
val socketDF = spark.
  readStream.
  format("socket").
  option("host", "localhost").
  option("port", 9999).
  load()

socketDF.isStreaming    // Returns True for DataFrames that have streaming sources

socketDF.printSchema

// Read all the csv files written atomically in a directory
val userSchema = new StructType().add("name", "string").add("age", "integer")
val csvDF = spark.
  readStream.
  option("sep", ";").
  schema(userSchema).      // Specify schema of the csv files
  csv(s"${sparkData}/streaming/output-")    // Equivalent to format("csv").load("/path/to/directory")
