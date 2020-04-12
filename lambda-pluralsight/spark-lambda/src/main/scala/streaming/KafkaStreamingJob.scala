package streaming


import com.twitter.algebird.HyperLogLogMonoid
import config.Settings
import domain.{Activity, ActivityByProduct, VisitorsByProduct}
import functions._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.max
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import utils.SparkUtils

object KafkaStreamingJob {
  def main(args: Array[String]): Unit = {
    val wlc = Settings.WebLogGen
    val spark = SparkUtils.getSparkContext("lambda with spark")
    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._
    val topic = wlc.kafkaTopic
    val batchDuration = Seconds(4)

    val kafkaParams: Map[String, Object] = Map(
      "bootstrap.servers" -> "192.168.56.101:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "lambda",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> Boolean.box(true)
    )

    def streamingApp(spark: SparkSession, batchDuration: Duration) = {
      val ssc = new StreamingContext(spark.sparkContext, batchDuration)
      val hdfsPath = wlc.hdfsPath
      val hdfsData = spark.read.parquet(hdfsPath)
      val fromOffset = hdfsData.groupBy("topic", "kafkaPartition").agg(max("untilOffset").as("untilOffset"))
        .collect().map{row =>
        (new TopicPartition(row.getString(0), row.getInt(1)), row.getString(2).toLong )
      }.toMap

      val kstream = fromOffset.isEmpty match {
        case true =>  KafkaUtils.createDirectStream[String, String](ssc
          , LocationStrategies.PreferConsistent
          ,ConsumerStrategies.Subscribe[String, String](List(topic), kafkaParams))
        case false => KafkaUtils.createDirectStream[String, String](ssc
          , LocationStrategies.PreferConsistent
          ,ConsumerStrategies.Subscribe[String, String](List(topic), kafkaParams, fromOffset))
      }

      val activityStream = kstream.transform(rddToRDDActivity(_)).cache()

      // writing activities to HDFS
      activityStream.foreachRDD { rdd =>
        val activityDF = rdd
          .toDF()
          .selectExpr("timestamp_hour", "referrer", "action", "prevPage", "page",
            "visitor", "product", "inputProps.topic as topic", "inputProps.kafkaPartition as kafkaPartition",
            "inputProps.fromOffset as fromOffset", "inputProps.untilOffset as untilOffset")
        activityDF.write
          .partitionBy("topic", "kafkaPartition", "timestamp_hour")
          .mode(SaveMode.Append)
          .parquet(hdfsPath)
      }

      val activityStateSpec = StateSpec.function(mapActivityStateFunc).timeout(Minutes(120))
      val statefulActivityByProduct =  activityStream.transform( rdd => {
        val df = rdd.toDF()
        df.createOrReplaceTempView("activity")
        val activityByProduct = spark.sql("""
                                            |SELECT product,
                                            |timestamp_hour,
                                            |sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
                                            |sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
                                            |sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
                                            |from activity
                                            |group by product, timestamp_hour
                                            |""".stripMargin)
        activityByProduct
          .map { r => ((r.getString(0), r.getLong(1)),
            ActivityByProduct(r.getString(0), r.getLong(1),r.getLong(2),r.getLong(3), r.getLong(4))
          )}.rdd
      }).mapWithState(activityStateSpec)

      val activityStateSnapshot = statefulActivityByProduct.stateSnapshots()
      activityStateSnapshot
        .reduceByKeyAndWindow(
          (a, b) => b,
          (x, y) => x,
          Seconds(30 / 4 * 4)
        ) // only save or expose the snapshot every x seconds
        .foreachRDD(rdd => {
          rdd.map(sr => {
            ActivityByProduct(sr._1._1, sr._1._2, sr._2._1,sr._2._2, sr._2._3)
          }).toDF().createOrReplaceTempView("ActivityByProduct")
        })

      // unique visitors by product
      val visitorStateSpec = StateSpec.function(mapVisitorsStateFunc _).timeout(Minutes(120))

      val hll = new HyperLogLogMonoid(12)
      val statefulVisitorsByProduct = activityStream.map(a => {
        ((a.product, a.timestamp_hour), hll.toHLL(a.visitor.getBytes))
      }).mapWithState(visitorStateSpec)
      val visitorStateSnapshot = statefulVisitorsByProduct.stateSnapshots()
      visitorStateSnapshot
        .reduceByKeyAndWindow(
          (a, b) => b,
          (x, y) => x,
          Seconds(30 / 4 * 4)
        ) // only save or expose the snapshot every x seconds
        .foreachRDD(rdd => {
          rdd.map(sr => {
            VisitorsByProduct(sr._1._1, sr._1._2, sr._2.approximateSize.estimate)
          }).toDF().createOrReplaceTempView("VisitorsByProduct")
        })
      ssc
    }
    val ssc = SparkUtils.getStreamingContext(streamingApp, spark, batchDuration)
    ssc.start()
    ssc.awaitTermination()

  }
}
