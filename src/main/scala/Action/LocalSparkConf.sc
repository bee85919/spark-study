object LocalSparkConf{
        val sparkHome=System.getenv("SPARK_HOME")
        val logDir=s"file:${sparkHome}/event"
        def get(appName:String):SparkConf=new SparkConf()
        .setMaster("local[*]")
        .setAppName(appName)
        .set("spark.eventLog.enabled","true")
        .set("spark.eventLog.dir",logDir)
}
