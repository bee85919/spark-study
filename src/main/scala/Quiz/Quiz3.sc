/*
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


import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics

// 비행 운행 데이터 로딩
val flightDataPath = s"${dataPath}/input/2008.csv"
val flightRdd = spark.sparkContext.textFile(flightDataPath)
val flightData = flightRdd.map(_.split(",")).filter(_(15) != "NA")


// 비행기 데이터 로딩 (노후화 데이터가 포함된 파일)
val planeDataPath = s"${dataPath}/metadata/plane-data.csv"
val planeRdd = spark.sparkContext.textFile(planeDataPath)
val planeData = planeRdd.map(_.split(",")).
  filter(fields => fields.length == 9 && fields(8) != "" && fields(8) != "None").
  map(fields => (fields(0), fields(8)))

// 비행기 노후화와 지연시간 결합
val delayAndAge = flightData.map(fields => (fields(10), fields(15).toDouble)).
  join(planeData).
  map { case (_, (delay, age)) => (delay, age.toDouble) }


// 지연시간과 노후화 데이터를 r1, r2로 정의
val r1RDD = sc.parallelize(delayAndAge.map(_._1).collect(), 5)
val r2RDD = sc.parallelize(delayAndAge.map(_._2).collect(), 5)

val corr = Statistics.corr(r1RDD, r2RDD, "pearson")
println(s"상관관계: $corr")


