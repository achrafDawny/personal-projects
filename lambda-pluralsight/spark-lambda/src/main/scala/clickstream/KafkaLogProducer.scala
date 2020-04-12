package clickstream

import java.io.FileWriter
import java.util.Properties

import config.Settings
import org.apache.commons.io.FileUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}


import scala.util.Random

object KafkaLogProducer extends App {
  val wlc = Settings.WebLogGen
  val products = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/products.csv")).getLines().toArray
  val referrers = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/referrers.csv")).getLines().toArray
  val visitors = (0 to wlc.visitors).map("Visitor-" + _ )
  val pages = (0 to wlc.pages).map("Page-" + _ )

  val rnd = new Random()

  val topic = wlc.kafkaTopic
  val props = new Properties()

  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.ACKS_CONFIG, "all")
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "WebLogProducer")

  val kafkaProducer = new KafkaProducer[Nothing, String](props)
  println(kafkaProducer.partitionsFor(topic))

  val filePath = wlc.filePath
  val destPath = wlc.destPath

  for(fileCount <- 1 to wlc.numberOfFiles) {

    //val fw = new FileWriter(filePath, true)

    val incrementTimeEvery = rnd.nextInt(wlc.records - 1) + 1
    var timestamp = System.currentTimeMillis()
    var adjustedTimestamp = timestamp
    for (iteration <- 1 to wlc.records) {
      adjustedTimestamp = adjustedTimestamp + ((System.currentTimeMillis() - timestamp) * wlc.timeMultiplier)
      timestamp = System.currentTimeMillis()
      val action = iteration % (rnd.nextInt(200) + 1) match {
        case 0 => "purchase"
        case 1 => "add_to_cart"
        case _ => "page_view"
      }
      val referrer = referrers(rnd.nextInt(referrers.length - 1))
      val prevPage = referrer match {
        case "Internal" => pages(rnd.nextInt(pages.length - 1))
        case _ => ""
      }
      val visitor = visitors(rnd.nextInt(visitors.length - 1))
      val page = pages(rnd.nextInt(pages.length - 1))
      val product = products(rnd.nextInt(products.length - 1))

      val line = s"$adjustedTimestamp\t$referrer\t$action\t$prevPage\t$visitor\t$page\t$product\n"
      val producerRecord = new ProducerRecord[Nothing, String](topic, line)
      kafkaProducer.send(producerRecord)
      //fw.write(line)
      if (iteration % incrementTimeEvery == 0) {
        println(s"Sent $iteration messages")
        val sleeping = rnd.nextInt(incrementTimeEvery * 60)
        println(s"Sleeping for $sleeping ms")
        Thread sleep sleeping
      }
    }
    //fw.close()

    //val outputFile = FileUtils.getFile(s"${destPath}\\data_$timestamp")
    //println(s"Moving produced data to $outputFile")
    //FileUtils.moveFile(FileUtils.getFile(filePath), outputFile)
    val sleeping = 2000
    println(s"Sleeping for $sleeping ms")

  }

  kafkaProducer.close()


}
