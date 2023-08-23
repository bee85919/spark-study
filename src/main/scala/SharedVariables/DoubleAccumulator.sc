val doubleAccum = sc.doubleAccumulator("Double SumAccumulator")
sc.parallelize(Array(1.1, 2.2, 3.3)).foreach(x => doubleAccum.add(x))
doubleAccum.value