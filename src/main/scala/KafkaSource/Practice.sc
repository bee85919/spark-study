// Spark-shell
spark-shell --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1


// df
val df = spark.
  readStream.
  format("kafka").
  option("kafka.bootstrap.servers", "localhost:29092,localhost:39092").
  option("subscribe", "test-topic").
  load()

df.printSchema


// streamDf
val streamDf = df.
  selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").
  as[(String, String)]


// writeStream
streamDf.writeStream.
  format(""). // choose
  options("", ""). // write option, additional options by format
  save() // actual executing


// write
streamDf.write.
  format(""). // choose
  options("", ""). // write option, additional options by format
  save() // actual executing


// file append
val dataPath = System.getenv("SPARK_DATA")
val outputPath = s"${dataPath}/streaming/output/ex-1"
val checkpointPath = s"${dataPath}/streaming/checkpoint/ex-1"
streamDf.
  writeStream.
  outputMode("append").
  format("csv").
  option("checkpointLocation", checkpointPath).
  start(outputPath).
  awaitTermination()


// Q: 왜 콤마(,) 가 붙었을까?
// A: CSV 형식이 각 필드를 콤마로 구분하기 때문이다.


// Trigger
val dataPath = System.getenv("SPARK_DATA")
val outputPath = s"${dataPath}/streaming/output/ex-3"
val checkpointPath = s"${dataPath}/streaming/checkpoint/ex-3"
import org.apache.spark.sql.streaming.Trigger
streamDf.
  writeStream.
  outputMode("append").
  format("csv").
  trigger(Trigger.ProcessingTime("10 seconds")).
  option("checkpointLocation", checkpointPath).
  start(outputPath).
  awaitTermination()
