import org.apache.spark.storage.StorageLevel
rdd.unpersist()
rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
rdd.getStorageLevel