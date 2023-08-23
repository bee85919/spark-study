// 1
val aggregatedDf = groupByDf.agg(
  sum(col("depdelay")),
  avg(col("depdelay")),
  sum(col("arrdelay")),
  avg(col("arrdelay")),
)
aggregatedDf.printSchema()
aggregatedDf.limit(10).show()


// 2
val sortedDf = aggregatedDf.sort(col("avg(depdelay)").desc, col("avg(arrdelay)").desc)
sortedDf.limit(10).show()


// 3
val aggregatedDf2 = groupByDf2.agg(
  sum(col("depdelay")),
  avg(col("depdelay")),
  sum(col("arrdelay")),
  avg(col("arrdelay")),
)
aggregatedDf2.printSchema()
aggregatedDf2.limit(10).show()

val sortedDf2 = aggregatedDf2.sort(col("avg(depdelay)").desc, col("avg(arrdelay)").desc)
sortedDf2.limit(10).show()
