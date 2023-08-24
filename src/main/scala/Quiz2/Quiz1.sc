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


/*
7.1 Spark SQL 퀴즈 1

airplane 데이터에는 다음 지연 사유 데이터 종류들이 있다:
  - CarrierDelay      (24)
  - WeatherDelay      (25)
  - NASDelay          (26)
  - SecurityDelay     (27)
  - LateAircraftDelay (28)

각 출발 공항별(16)로 어떤 delay가 출발 지연(15)에 가장 큰 영향을 미치는지(상관관계수가 높은지) 구하라.
  - 상관관계수 구하는 방법은 Spark RDD 의 퀴즈를 참고
*/


// 모듈 import
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics


// 지연 사유 인덱스와 이름 매핑
val delayReasonNames = Map(
  24 -> "CarrierDelay",
  25 -> "WeatherDelay",
  26 -> "NASDelay",
  27 -> "SecurityDelay",
  28 -> "LateAircraftDelay"
)


// 데이터 불러오기
val dataPath = System.getenv("SPARK_DATA")
val path = s"${dataPath}/input/2008.csv"
val rdd = spark.sparkContext.textFile(path)
val csv = rdd.map(f => f.split(","))


// 출발 지연이 발생한 데이터만 추출 (NA 및 숫자가 아닌 값 제외)
val fRdd = csv.filter(fields => {
  try {
    fields(15) != "NA" && fields(15).toInt > 0
  } catch {
    case _: NumberFormatException => false
  }
})


// 각 지연 사유별 상관관계 계산
val delayReasons = delayReasonNames.keys.toList
val correlations = delayReasons.map { reasonIdx =>
  val reasonAndDelayData = fRdd.filter(fields => fields(reasonIdx) != "NA" && fields(15) != "NA").
    map(fields => (fields(reasonIdx).toDouble, fields(15).toDouble))
  val reasonRddDoubles = reasonAndDelayData.map(_._1)
  val delayRddDoubles = reasonAndDelayData.map(_._2)
  val corr = Statistics.corr(reasonRddDoubles, delayRddDoubles, "pearson")
  corr
}

// 최대 상관관계 찾기
val maxCorrelationIdx = correlations.indices.maxBy(correlations)
val maxCorrelation = correlations(maxCorrelationIdx)
val maxCorrelationReasonIdx = delayReasons(maxCorrelationIdx)

// 결과 출력
println(s"가장 큰 영향을 미치는 지연 사유: ${delayReasonNames(maxCorrelationReasonIdx)}, 상관관계: $maxCorrelation")
