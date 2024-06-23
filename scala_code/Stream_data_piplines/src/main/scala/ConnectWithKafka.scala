import org.apache.spark.sql.SparkSession
/**
 * Kafka is running on docker
 */
object ConnectWithKafka extends App {
  val spark = SparkSession
    .builder
    .appName("Getting data from Kafka")
    .config("spark.sql.shuffle.partitions", "5")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  // Subscribe to 1 topic
  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "host.docker.internal:9092")
    .option("subscribe", "quickstart")
    .option("startingOffsets", "earliest")
    .load()

  // Select the value of the Kafka message and cast it to a string
  val kafkaData = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]

  // Print the data to the console
  val query = kafkaData.writeStream
    .outputMode("append")
//    .outputMode("complete")
    .format("console")
    .start()

  query.awaitTermination()

}
