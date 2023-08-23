val accum = sc.longAccumulator("SumAccumulator")
sc.parallelize(Array(1, 2, 3)).foreach(x => accum.add(x))
accum.value // Long = 0