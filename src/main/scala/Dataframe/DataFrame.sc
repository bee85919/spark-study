val dataPath = System.getenv("SPARK_DATA")
val path=s"${dataPath}/input/2008.csv"
val rddCsv = spark.read.csv(path)
rddCsv.count

val columns = Seq("year" ,
  "month" ,
  "dayofmonth" ,
  "dayofweek" ,
  "deptime" ,
  "crsdeptime" ,
  "arrtime" ,
  "crsarrtime" ,
  "uniquecarrier" ,
  "flightnum" ,
  "tailnum" ,
  "actualelapsedtime" ,
  "crselapsedtime" ,
  "airtime" ,
  "arrdelay" ,
  "depdelay" ,
  "origin" ,
  "dest" ,
  "distance" ,
  "taxiin" ,
  "taxiout" ,
  "cancelled" ,
  "cancellationcode" ,
  "diverted" ,
  "carrierdelay" ,
  "weatherdelay" ,
  "nasdelay" ,
  "securitydelay" ,
  "lateaircraftdelay"
)

val df = rddCsv.toDF(columns: _*)
df.printSchema()
df.limit(1).show()

val dfNoSchema = rddCsv.toDF()
dfNoSchema.printSchema()
dfNoSchema.limit(1).show()

df.limit(1).dtypes.foreach(x=> println(x._1 + " " + x._2))