package com.examples.activity_data

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}

object mapGroupsWithStateExample1 extends App {
  /**
   * example of stateful processing uses a feature called mapGroupsWithState.
   * This is similar to a user-defined aggregation function that takes as input an update set of data and
   * then resolves it down to a specific key with a set of values.
   * There are several things you’re going to need to define along the way:
   *
   * Three class definitions: an input definition, a state definition, and optionally an output definition.
   *
   * A function to update the state based on a key, an iterator of events, and a previous state.
   *
   * A time-out parameter
   */

  case class InputRow(user:String, timestamp:java.sql.Timestamp, activity:String)
  case class UserState(user:String,
                       var activity:String,
                       var start:java.sql.Timestamp,
                       var end:java.sql.Timestamp)

  def updateUserStateWithEvent(state:UserState, input:InputRow):UserState = {
    if (Option(input.timestamp).isEmpty) {
      return state
    }
    if (state.activity == input.activity) {

      if (input.timestamp.after(state.end)) {
        state.end = input.timestamp
      }
      if (input.timestamp.before(state.start)) {
        state.start = input.timestamp
      }
    } else {
      if (input.timestamp.after(state.end)) {
        state.start = input.timestamp
        state.end = input.timestamp
        state.activity = input.activity
      }
    }

    state
  }

  def updateAcrossEvents(user:String,
                         inputs: Iterator[InputRow],
                         oldState: GroupState[UserState]):UserState = {
    var state:UserState = if (oldState.exists) oldState.get else UserState(user,
      "",
      new java.sql.Timestamp(6284160000000L),
      new java.sql.Timestamp(6284160L)
    )
    // we simply specify an old date that we can compare against and
    // immediately update based on the values in our data

    for (input <- inputs) {
      state = updateUserStateWithEvent(state, input)
      oldState.update(state)
    }
    state
  }

  val config = ConfigFactory.load()

  val spark = SparkSession
    .builder()
    .appName("This is example of Removing Duplicate data from stream")
    .master("local[*]")
    .config("spark.sql.shuffle.partition", "5")
    .config("spark.sql.streaming.schemaInference", "true")
    .config("spark.sql.streaming.checkpointLocation",config.getString("activity_data.checkpointLocation"))
    .getOrCreate()

  val streaming = spark.readStream
    .option("maxFilesPerTrigger", "1")
    .json(config.getString("activity_data.path"))

  val withEvenTime = streaming
    .withColumn("event_time", (col("Creation_Time")/1e9).cast("timestamp"))

  import spark.implicits._

  val mapGroupsWithStateStatefulStreaming = withEvenTime
    .selectExpr("User as user",
      "cast(Creation_Time/1000000000 as timestamp) as timestamp", "gt as activity")
    .as[InputRow]
    .groupByKey(_.user)
    .mapGroupsWithState(GroupStateTimeout.NoTimeout)(updateAcrossEvents)
    .writeStream
//    .queryName("events_per_window")
    .format("Console")
    .option("truncate", "false")
    .outputMode("update")
    .start()

  mapGroupsWithStateStatefulStreaming.awaitTermination()

}
