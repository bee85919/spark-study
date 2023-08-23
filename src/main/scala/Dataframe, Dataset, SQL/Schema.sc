val rdd = spark.sparkContext.textFile(path)
val rddCsv = rdd.map(f=>{
  f.split(",")
})
rddCsv.count

import org.apache.spark.sql.types.{StringType, StructField, StructType}

val schema = StructType(Array(
  StructField("year", StringType, true),
  StructField("month", StringType, true),
  StructField("dayofmonth", StringType, true),
  StructField("dayofweek", StringType, true),
  StructField("deptime", StringType, true),
  StructField("crsdeptime", StringType, true),
  StructField("arrtime", StringType, true),
  StructField("crsarrtime", StringType, true),
  StructField("uniquecarrier", StringType, true),
  StructField("flightnum", StringType, true),
  StructField("tailnum", StringType, true),
  StructField("actualelapsedtime", StringType, true),
  StructField("crselapsedtime", StringType, true),
  StructField("airtime", StringType, true),
  StructField("arrdelay", StringType, true),
  StructField("depdelay", StringType, true),
  StructField("origin", StringType, true),
  StructField("dest", StringType, true),
  StructField("distance", StringType, true),
  StructField("taxiin", StringType, true),
  StructField("taxiout", StringType, true),
  StructField("cancelled", StringType, true),
  StructField("cancellationcode", StringType, true),
  StructField("diverted", StringType, true),
  StructField("carrierdelay", StringType, true),
  StructField("weatherdelay", StringType, true),
  StructField("nasdelay", StringType, true),
  StructField("securitydelay", StringType, true),
  StructField("lateaircraftdelay", StringType, true)
))

import org.apache.spark.sql.Row

val rowRdd = rddCsv.map(a => Row(a(0), a(1), a(2), a(3), a(4), a(5),  a(6), a(7), a(8), a(9),
  a(10), a(11), a(12), a(13), a(14), a(15),  a(16), a(17), a(18), a(19),
  a(20), a(21), a(22), a(23), a(24), a(25),  a(26), a(27), a(28)
))

val df = spark.createDataFrame(rowRdd, schema)

df.printSchema
df.limit(1).show()

val hiveDF = spark.sql("select * from employee")
hiveDF.printSchema()
hiveDF.limit(1).show()