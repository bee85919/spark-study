// default storageLevel
rdd.cache()
rdd.getStorageLevel

val df = rdd.toDF()
df.cache()
df.storageLevel

// unpersist
rdd.unpersist()
df.unpersist()