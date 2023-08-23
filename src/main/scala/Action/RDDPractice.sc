// path
val dataPath = System.getenv("SPARK_DATA")
val path=s"${dataPath}/input/2008.csv"


// RDD parallelize
val rdd = sc.parallelize(Array(1,2,3,4,5,6,7,8,9,10))

rdd.collect()
rdd.getNumPartitions
rdd.first
rdd.collect().foreach(println)


// empty rdd
sc.parallelize(Seq.empty[String])


// read text file
val rdd = spark.sparkContext.textFile(path)

rdd.first
rdd.take(5).foreach(println)

// read csv
val rddCsv = rdd.map(f=>{
  f.split(",")
})

rddCsv.first