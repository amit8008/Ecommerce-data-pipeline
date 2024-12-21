package com.examples.activity_data

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}

object flatMapGroupsWithStateExample2 extends App {

  /**
   * This example analyzes the activity dataset from this chapter and
   * outputs the average reading of each device periodically,
   * creating a window based on the count of events and
   * outputting it each time it has accumulated 500 events for that device.
   * You define two case classes for this task:
   * the input row format (which is simply a device and a timestamp);
   * and the state and output rows (which contain the current count of records collected,
   * device ID,
   * and an array of readings for the events in the window).
   */

  case class InputRow(device: String, timestamp: java.sql.Timestamp, x: Double)
  case class DeviceState(device: String, var values: Array[Double],
                         var count: Int)
  case class OutputRow(device: String, previousAverage: Double)

  def updateWithEvent(state:DeviceState, input:InputRow):DeviceState = {
    state.count += 1
    // maintain an array of the x-axis values
    state.values = state.values ++ Array(input.x)
    state
  }

  def updateAcrossEvents(device:String, inputs: Iterator[InputRow],
                         oldState: GroupState[DeviceState]):Iterator[OutputRow] = {
    inputs.toSeq.sortBy(_.timestamp.getTime).toIterator.flatMap { input =>
      val state = if (oldState.exists) oldState.get
      else DeviceState(device, Array(), 0)

      val newState = updateWithEvent(state, input)
      if (newState.count >= 500) {
        // One of our windows is complete; replace our state with an empty
        // DeviceState and output the average for the past 500 items from
        // the old state
        oldState.update(DeviceState(device, Array(), 0))
        Iterator(OutputRow(device,
          newState.values.sum / newState.values.length.toDouble))
      }
      else {
        // Update the current DeviceState object in place and output no
        // records
        oldState.update(newState)
        Iterator()
      }
    }
  }


  val config = ConfigFactory.load()

  val spark = SparkSession
    .builder()
    .appName("Example 2 for Arbitrary Stateful Processing")
    .master("local[*]")
    .config("spark.sql.sheuffle.partition", "5")
    .config("spark.sql.streaming.schemaInference", "true")
    .config("spark.sql.streaming.checkpointLocation", config.getString("activity_data.checkpointLocation"))
    .getOrCreate()

  val streaming = spark.readStream
    .option("maxFilesPerTrigger", "1")
    .json(config.getString("activity_data.path"))

  val withEventTime = streaming
    .withColumn("event_time", (col("Creation_Time")/1e9).cast("timestamp"))

  import spark.implicits._

  val flatMapGroupsWithStateStatefulStreaming2 = withEventTime
    .selectExpr("Device as device",
      "cast(Creation_Time/1000000000 as timestamp) as timestamp",
      "x")
    .as[InputRow]
    .groupByKey(_.device)
    .flatMapGroupsWithState(OutputMode.Update(),
      GroupStateTimeout.NoTimeout)(updateAcrossEvents)
    .writeStream
//    .queryName("count_based_device")
    .format("Console")
    .option("truncate", "false")
    .outputMode("update")
    .start()


  flatMapGroupsWithStateStatefulStreaming2.awaitTermination()
}
