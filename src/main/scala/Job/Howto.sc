$SPARK_HOME/bin/spark-submit \
  --class <main-class> \
  --master <master-url> \
    --deploy-mode <deploy-mode> \
      --conf <key>=<value> \
        ... # other options
        <application-jar> \
          [application-arguments]