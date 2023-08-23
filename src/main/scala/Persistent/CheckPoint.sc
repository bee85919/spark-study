sc.setCheckpointDir(s"${dataPath}/checkpoint")
sc.getCheckpointDir

rdd.checkpoint()