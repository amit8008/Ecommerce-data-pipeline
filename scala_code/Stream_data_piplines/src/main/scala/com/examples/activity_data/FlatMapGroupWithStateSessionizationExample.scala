package com.examples.activity_data

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object FlatMapGroupWithStateSessionizationExample extends App {

  /**
   * Sessions are simply unspecified time windows with a series of events that occur.
   * Typically, you want to record these different events in an array in order to
   * compare these sessions to other sessions in the future. In a session,
   * you will likely have arbitrary logic to maintain and update your state over time
   * as well as certain actions to define when state ends (like a count) or a simple time-out.
   * Let’s build on the previous example and define it a bit more strictly as a session.
   *
   * At times, you might have an explicit session ID that you can use in your function.
   * This obviously makes it much easier because you can just perform a simple aggregation and
   * might not even need your own stateful logic.
   * In this case, you’re creating sessions on the fly from a user ID and some time information and
   * if you see no new event from that user in five seconds, the session terminates.
   * You’ll also notice that this code uses time-outs differently than we have in other examples.
   */

  case class InputRow(uid:String, timestamp:java.sql.Timestamp, x:Double,
                      activity:String)
  case class UserSession( uid:String, var timestamp:java.sql.Timestamp,
                         var activities: Array[String], var values: Array[Double])
  case class UserSessionOutput( uid:String, var activities: Array[String],
                               var xAvg:Double)

  def updateWithEvent(state:UserSession, input:InputRow):UserSession = {
    // handle malformed dates
    if (Option(input.timestamp).isEmpty) {
      return state
    }

    state.timestamp = input.timestamp
    state.values = state.values ++ Array(input.x)
    if (!state.activities.contains(input.activity)) {
      state.activities = state.activities ++ Array(input.activity)
    }
    state
  }

  import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode, GroupState}

  def updateAcrossEvents(uid:String,
                         inputs: Iterator[InputRow],
                         oldState: GroupState[UserSession]):Iterator[UserSessionOutput] = {

    inputs.toSeq.sortBy(_.timestamp.getTime).toIterator.flatMap { input =>
      val state = if (oldState.exists) oldState.get else UserSession(
        uid,
        new java.sql.Timestamp(6284160000000L),
        Array(),
        Array())
      val newState = updateWithEvent(state, input)

      if (oldState.hasTimedOut) {
        val state = oldState.get
        oldState.remove()
        Iterator(UserSessionOutput(uid,
          state.activities,
          newState.values.sum / newState.values.length.toDouble))
      } else if (state.values.length > 1000) {
        val state = oldState.get
        oldState.remove()
        Iterator(UserSessionOutput(uid,
          state.activities,
          newState.values.sum / newState.values.length.toDouble))
      } else {
        oldState.update(newState)
        oldState.setTimeoutTimestamp(newState.timestamp.getTime, "25 minutes")
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

  val flatMapGroupsWithStateStatefulSessionizationExample = withEventTime
    .where("x is not null")
    .selectExpr("user as uid",
      "cast(Creation_Time/1000000000 as timestamp) as timestamp",
      "x", "gt as activity")
    .as[InputRow]
    .withWatermark("timestamp", "30 minutes")
    .groupByKey(_.uid)
    .flatMapGroupsWithState(OutputMode.Update(),
      GroupStateTimeout.EventTimeTimeout)(updateAcrossEvents)
    .writeStream
//    .queryName("count_based_device")
    .format("Console")
    .option("truncate", "false")
    .outputMode("update")
    .start()


  flatMapGroupsWithStateStatefulSessionizationExample.awaitTermination()

}
