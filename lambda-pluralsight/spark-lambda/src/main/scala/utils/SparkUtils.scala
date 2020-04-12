package utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, StreamingContext}

object SparkUtils {
  def getSparkContext(appName : String): SparkSession = {
    val checkpointDirectory = "C:\\Users\\aelgdaou\\Downloads\\data"
    val spark = SparkSession.builder().appName(appName).master("local[*]").config("dfs.replication","1").getOrCreate()
    spark.sparkContext.setCheckpointDir(checkpointDirectory)
    spark
  }

  def getStreamingContext(streamingApp: (SparkSession, Duration) => StreamingContext, spark: SparkSession, batchDuration: Duration) = {
    val sc = spark.sparkContext
    val creatingFunc = () => streamingApp(spark, batchDuration)
    val ssc = sc.getCheckpointDir match {
      case Some(checkpointDir) => StreamingContext.getActiveOrCreate(checkpointDir, creatingFunc, sc.hadoopConfiguration, createOnError = true)
      case None => StreamingContext.getActiveOrCreate(creatingFunc)
    }
    sc.getCheckpointDir.foreach(cp => ssc.checkpoint(cp))
    ssc
  }
}
