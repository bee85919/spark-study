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


// 데이터 불러오기
val dataPath = System.getenv("SPARK_DATA")
val path=s"${dataPath}/input/2008.csv"
val rdd = spark.sparkContext.textFile(path)
val csv = rdd.map(f => {
  f.split(",")
})
val fRdd = csv.filter(_(15) != "NA").filter(_(15).toInt > 0)


// 지연 시간이 발생한 데이터 추출
val sumDelayed = fRdd.map(fields => (fields(16), fields(15).toInt)).reduceByKey(_ + _)


// 지연 개수 계산
val cntDelayed = fRdd.map(fields => (fields(16), 1)).reduceByKey(_ + _)


// join
val joinDelayed = sumDelayed.join(cntDelayed)


// 평균 계산
val avgDelayed = joinDelayed.map { case (origin, (sum, cnt)) => (origin, sum.toDouble / cnt) }


// 출력
avgDelayed.collect().foreach(println)
avgDelayed.count()
