val longAccum = sc.longAccumulator("Long SumAccumulator")
sc.parallelize(Array(1, 2, 3)).foreach(x => longAccum.add(x))
longAccum.value