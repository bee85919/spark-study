// Stream-static Joins
val staticDf = spark.read. ...
val streamingDf = spark.readStream. ...

streamingDf.join(staticDf, "type")          // inner equi-join with a static DF
streamingDf.join(staticDf, "type", "left_outer")  // left outer join with a static DF


// multiple watermark
inputStream1.withWatermark("eventTime1", "1 hour").
  join(
    inputStream2.withWatermark("eventTime2", "2 hours"),
    joinCondition)