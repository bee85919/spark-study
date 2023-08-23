// year, month, day
import org.apache.spark.sql.types.DateType

val convertedDf = df.withColumn("date",
  concat(col("year"), lit("-"), col("month"), lit("-"), col("dayofmonth"))
    .cast(DateType)
)

convertedDf.limit(1).show()


// date, int
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.functions._

val convertedDf2 = convertedDf.withColumn("crsdep_ts",
  concat(
    col("date"),
    lit(" "),
    col("crsdeptime").cast(IntegerType).divide(lit(100)).cast(IntegerType),
    lit(":"),
    col("crsdeptime").cast(IntegerType).mod(100).cast(IntegerType),
    lit(":"),
    lit("00")
  ).cast(TimestampType)
)

convertedDf2.filter(col("arrdelay") === "NA").count
convertedDf2.filter(col("arrdelay") =!= "NA").count
convertedDf2.filter(col("depdelay") === "NA").count
convertedDf2.filter(col("depdelay") =!= "NA").count

val convertedDf3 = convertedDf2.withColumn("arrdelay",
  when(col("arrdelay") === lit("NA"), null)
    .otherwise(col("arrdelay"))
).withColumn("depdelay",
  when(col("depdelay") === lit("NA"), null)
    .otherwise(col("depdelay"))
)

// when
convertedDf3.filter(col("arrdelay").isNull).count
convertedDf3.filter(col("arrdelay").isNotNull).count
convertedDf3.filter(col("depdelay").isNull).count
convertedDf3.filter(col("depdelay").isNotNull).count

convertedDf2.select(col("date"), col("crsdeptime"), col("crsdep_ts")).limit(1).show()


// casting
val convertedDf4 = convertedDf3.withColumn("arrdelay",
  col("arrdelay").cast(IntegerType)
).withColumn("depdelay",
  col("depdelay").cast(IntegerType)
)
convertedDf4.printSchema()


// create new column
import org.apache.spark.sql.types.BooleanType

convertedDf4.filter(col("arrdelay") > 0 or col("depdelay") > 0).count()

val convertedDf5 = convertedDf4.
  withColumn("is_delayed",
    when(col("arrdelay") > 0, true)
      .otherwise(
        when(col("depdelay") > 0, true)
          .otherwise(false)
      ).cast(BooleanType)
  )

val result5 = convertedDf5.filter(col("is_delayed") === true)
result5.count
result5.select(col("arrdelay"), col("depdelay"), col("is_delayed")).limit(3).show()
result5.filter(col("arrdelay").isNull).select(col("arrdelay"), col("depdelay"), col("is_delayed")).limit(3).show()