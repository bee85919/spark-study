/*
/Users/b06/spark-data/input/2008.csv
Variable descriptions:
Name Description
1 Year 1987-2008
2 Month 1-12
3 DayofMonth 1-31
4 DayOfWeek 1 (Monday) - 7 (Sunday)
5 DepTime actual departure time (local, hhm m)
6 CRSDepTime scheduled departure time (local, hhmm)
7 ArrTime actual arrival time (local, hhmm)
8 CRSArrTime scheduled arrival time (local, hhmm)
9 UniqueCarrier unique carrier code
10 FlightNum flight number
11 TailNum plane tail number
12 ActualElapsedTime in minutes
13 CRSElapsedTime in minutes
14 AirTime in minutes
15 ArrDelay arrival delay, in minutes
16 DepDelay departure delay, in minutes
17 Origin origin IATA airport code
18 Dest destination IATA airport code
19 Distance in miles
20 TaxiIn taxi in time, in minutes
21 TaxiOut taxi out time in minutes
22 Cancelled was the flight cancelled?
23 CancellationCode reason for cancellation (A = carrier, B = weather, C = NAS, D = security)
24 Diverted 1 = yes, 0 = no
25 CarrierDelay in minutes
26 WeatherDelay in minutes
27 NASDelay in minutes
28 SecurityDelay in minutes
29 LateAircraftDelay in minutes
*/


/*
/Users/b06/spark-data/metadata/plane-data.csv
1 tailnum
2 type
3 manufacturer
4 issue_date
5 model
6 status
7 aircraft_type
8 engine_type
9 year
*/

/*
7.2 Spark SQL 퀴즈 2
airline 데이터에 plane-data 에서 tailnum(0), manufacturer(2), issue_date(3), model(4) 를 조인한 Datafame 또는 Dataset 을 만든다.
이를 통해 어떤 생산자가 만든 비행기가, 어떤 모델의 비행기가, 몇 년식 비행기가 가장 사용성이 좋은지 데이터를 구한다.
사용성이 좋다는 기준 - 하나의 airplane (unique tailnum)이 (발행일 issue_date 로부터) 얼마나 오랜기간 사용되었나
*/


// 모듈 import
import spark.implicits._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


// airline 스키마
val airlineSchema = StructType(Array(
  StructField("Year", IntegerType, true),
  StructField("Month", IntegerType, true),
  StructField("DayofMonth", IntegerType, true),
  StructField("DayOfWeek", IntegerType, true),
  StructField("DepTime", StringType, true),
  StructField("CRSDepTime", StringType, true),
  StructField("ArrTime", StringType, true),
  StructField("CRSArrTime", StringType, true),
  StructField("UniqueCarrier", StringType, true),
  StructField("FlightNum", StringType, true),
  StructField("TailNum", StringType, true),
  StructField("ActualElapsedTime", StringType, true),
  StructField("CRSElapsedTime", StringType, true),
  StructField("AirTime", StringType, true),
  StructField("ArrDelay", StringType, true),
  StructField("DepDelay", StringType, true),
  StructField("Origin", StringType, true),
  StructField("Dest", StringType, true),
  StructField("Distance", StringType, true),
  StructField("TaxiIn", StringType, true),
  StructField("TaxiOut", StringType, true),
  StructField("Cancelled", StringType, true),
  StructField("CancellationCode", StringType, true),
  StructField("Diverted", StringType, true),
  StructField("CarrierDelay", StringType, true),
  StructField("WeatherDelay", StringType, true),
  StructField("NASDelay", StringType, true),
  StructField("SecurityDelay", StringType, true),
  StructField("LateAircraftDelay", StringType, true)
))


// airline 불러오기
val airlinePath = "/Users/b06/spark-data/input/2008.csv"
val airlineData = spark.read.
  option("header", "false").
  schema(airlineSchema).
  csv(airlinePath)


// plane 불러오기
val planePath = "/Users/b06/spark-data/metadata/plane-data.csv"
val planeRdd = sc.textFile(planePath).
  map(line => line.split(",")).
  filter(fields =>
    fields.length == 9 &&
      fields(0).nonEmpty &&
      fields(2).nonEmpty &&
      fields(3).nonEmpty &&
      fields(4).nonEmpty
  ).
  map(fields => (fields(0), fields(2), fields(3), fields(4)))
val planeData = planeRdd.toDF("tailnum", "manufacturer", "issue_date", "model")


// join
val joinedData = planeData.
  join(airlineData, planeData("tailnum") === airlineData("TailNum")).
  drop(airlineData("TailNum"))


// issue_date를 날짜 형식으로 변환
val withIssueDate = joinedData.withColumn("issue_date", to_date(col("issue_date"), "MM/dd/yyyy"))


// 발행일로부터 현재까지의 기간을 계산
val withDuration = withIssueDate.withColumn("duration", datediff(current_date(), col("issue_date")))


// 비행기별로 기간을 집계
val usageStats = withDuration.groupBy("tailnum").agg(
  first("manufacturer").as("manufacturer"),
  first("model").as("model"),
  min("issue_date").as("first_issue_date"),
  max("duration").as("max_duration")
)


// 결과를 출력합니다.
usageStats.orderBy(desc("max_duration")).show()


/*
+-------+-----------------+--------------+----------------+------------+
|tailnum|     manufacturer|         model|first_issue_date|max_duration|
+-------+-----------------+--------------+----------------+------------+
| N567AA|      DEHAVILLAND|   OTTER DHC-3|      1976-01-09|       17394|
+-------+-----------------+--------------+----------------+------------+
*/
