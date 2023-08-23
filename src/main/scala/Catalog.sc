// spark-shell에서 진행
val ds = spark.catalog.listDatabases
ds.show(false)

val ds2 = spark.catalog.listTables
ds2.show(false)