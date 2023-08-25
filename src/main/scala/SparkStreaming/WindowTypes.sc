import spark.implicits._

val events = ... // streaming DataFrame of schema { timestamp: Timestamp, userId: String }

// Group the data by session window and userId, and compute the count of each group
val sessionizedCounts = events
  .withWatermark("timestamp", "10 minutes")
  .groupBy(
    session_window($"timestamp", "5 minutes"),
    $"userId")
  .count()



import spark.implicits._

val events = ... // streaming DataFrame of schema { timestamp: Timestamp, userId: String }

val sessionWindow = session_window($"timestamp", when($"userId" === "user1", "5 seconds")
  .when($"userId" === "user2", "20 seconds")
  .otherwise("5 minutes"))

// Group the data by session window and userId, and compute the count of each group
val sessionizedCounts = events
  .withWatermark("timestamp", "10 minutes")
  .groupBy(
    Column(sessionWindow),
    $"userId")
  .count()