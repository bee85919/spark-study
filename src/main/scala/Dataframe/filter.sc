val filteredDf = convertedDf5.filter(col("is_delayed") === true)
filteredDf.count()


// SQL
convertedDf5.createOrReplaceTempView("temp5")
val filteredDfBySql = convertedDf5.sqlContext.sql("select * from temp5 where is_delayed = 'true'")
filteredDfBySql.count()

// 성능 차이가 날까? spark history server 에서 DAG 그래프와 함께 확인