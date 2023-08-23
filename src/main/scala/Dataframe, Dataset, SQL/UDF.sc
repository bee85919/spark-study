// UDF 선언
val udfConvertValueNAtoNull = udf({ value: String =>
  if (value == null || value.isEmpty ||  "NA".equals(value.toUpperCase())) None
  else Some(value.toInt)
}: (String => Option[Int]) )

// UDF 사용
convertedDf2.filter(col("arrdelay") === "NA").count
convertedDf2.filter(col("arrdelay") =!= "NA").count
convertedDf2.filter(col("deptime") === "NA").count
convertedDf2.filter(col("depdelay") =!= "NA").count

val convertedDf3 = convertedDf2.withColumn("arrdelay", udfConvertValueNAtoNull(col("arrdelay"))).withColumn("depdelay",udfConvertValueNAtoNull(col("depdelay")))

convertedDf3.filter(col("arrdelay").isNull).count
convertedDf3.filter(col("arrdelay").isNotNull).count
convertedDf3.filter(col("depdelay").isNull).count
convertedDf3.filter(col("depdelay").isNotNull).count

// column 변경
val convertedDf3 = convertedDf2.
  withColumn("year", udfConvertValueNAtoNull(col("year"))).
  withColumn("month", udfConvertValueNAtoNull(col("month"))).
  withColumn("dayofmonth", udfConvertValueNAtoNull(col("dayofmonth"))).
  withColumn("dayofweek", udfConvertValueNAtoNull(col("dayofweek"))).
  withColumn("deptime", udfConvertValueNAtoNull(col("deptime"))).
  withColumn("crsdeptime", udfConvertValueNAtoNull(col("crsdeptime"))).
  withColumn("arrtime", udfConvertValueNAtoNull(col("arrtime"))).
  withColumn("crsarrtime", udfConvertValueNAtoNull(col("crsarrtime"))).
  withColumn("flightnum", udfConvertValueNAtoNull(col("flightnum"))).
  withColumn("actualelapsedtime", udfConvertValueNAtoNull(col("actualelapsedtime"))).
  withColumn("crselapsedtime", udfConvertValueNAtoNull(col("crselapsedtime"))).
  withColumn("airtime", udfConvertValueNAtoNull(col("airtime"))).
  withColumn("arrdelay", udfConvertValueNAtoNull(col("arrdelay"))).
  withColumn("depdelay", udfConvertValueNAtoNull(col("depdelay"))).
  withColumn("distance", udfConvertValueNAtoNull(col("distance"))).
  withColumn("taxiin", udfConvertValueNAtoNull(col("taxiin"))).
  withColumn("taxiout", udfConvertValueNAtoNull(col("taxiout"))).
  withColumn("cancelled", udfConvertValueNAtoNull(col("cancelled"))).
  withColumn("diverted", udfConvertValueNAtoNull(col("diverted"))).
  withColumn("carrierdelay", udfConvertValueNAtoNull(col("carrierdelay"))).
  withColumn("weatherdelay", udfConvertValueNAtoNull(col("weatherdelay"))).
  withColumn("nasdelay", udfConvertValueNAtoNull(col("nasdelay"))).
  withColumn("securitydelay", udfConvertValueNAtoNull(col("securitydelay")))

convertedDf3.printSchema()

ds.filter(x => x.arrdelay == "NA").count
ds.filter(x => x.arrdelay != "NA").count
ds.filter(x => x.depdelay == "NA").count
ds.filter(x => x.depdelay != "NA").count

ds.select($"arrdelay", $"depdelay").limit(1).show()