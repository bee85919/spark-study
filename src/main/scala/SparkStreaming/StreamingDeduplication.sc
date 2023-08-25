val streamingDf = spark.readStream. ...  // columns: guid, eventTime, ...

// Without watermark using guid column
streamingDf.dropDuplicates("guid")

// With watermark using guid and eventTime columns
streamingDf.
  withWatermark("eventTime", "10 seconds").
  dropDuplicates("guid", "eventTime")