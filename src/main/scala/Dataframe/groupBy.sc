// 1
val groupByDf = filteredDf.groupBy(
  col("ORIGIN"),
  col("DEST")
)
groupByDf.toString()


// 2
val groupByDf2 = filteredDf.groupBy(
  col("ORIGIN")
)