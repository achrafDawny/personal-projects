package batch

import java.lang.management.ManagementFactory

import config.Settings
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import domain._
import utils.SparkUtils
object BatchJob {
  def main(args: Array[String]): Unit = {

    val spark = SparkUtils.getSparkContext("lambda with spark")
    import spark.implicits._
    val sc = spark.sparkContext
    val wlc = Settings.WebLogGen
    val hdfsPath = wlc.hdfsPath
    sc.setLogLevel("ERROR")

    val inputDF = spark.read.parquet(hdfsPath)
      .where(unix_timestamp() - $"timestamp_hour" <= 1000* 60 * 60 * 6)
    inputDF.createOrReplaceTempView("activity")

    val activityByProduct = spark.sql("""
        |SELECT product,
        |timestamp_hour,
        |sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
        |sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
        |sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
        |from activity
        |group by product, timestamp_hour
        |""".stripMargin).cache()
    val visitorsByProduct = inputDF.select($"product", $"timestamp_hour", $"visitor")
      .groupBy($"product",$"timestamp_hour")
      .agg(countDistinct($"visitor").as("unique_visitors"))

    activityByProduct.write.partitionBy("timestamp_hour").mode(SaveMode.Append)
    .parquet("hdfs://192.168.56.101:9000/user/node01/output")

    visitorsByProduct.take(10).foreach(println)
    activityByProduct.take(10).foreach(println)
  }
}
