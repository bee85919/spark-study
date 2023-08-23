// filter
val filteredRdd = rddCsv.filter(_(14) != "NA").filter(_(14).toInt > 0)
val totalDelayedArrivals = filteredRdd.count()


// groupByKey
val distancesByDestForDelayedArrival =  filteredRdd.map({case a => (a(17), a(18).toInt)}).groupByKey()


// reduceBy
val sumDistanceByDestForDelayedArrival =  filteredRdd.map({case a => (a(17), a(18).toInt)}).reduceByKey(_ + _)
val countByDestForDelayedArrival =  filteredRdd.map({case a => (a(17), 1)}).reduceByKey(_ + _)


// join
val joinedByDest = sumDistanceByDestForDelayedArrival.join(countByDestForDelayedArrival)


// sort
val sortedByCount = joinedByDest.sortBy(_._2._2, false)


// average
val avgDistanceByDestForDelayedArrival = joinedByDest.map({case (key, (sumOfDistances, count)) => (key, sumOfDistances/count)})
avgDistanceByDestForDelayedArrival.collect()