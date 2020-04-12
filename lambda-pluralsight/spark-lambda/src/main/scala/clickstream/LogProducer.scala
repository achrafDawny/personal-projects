package clickstream

import java.io.FileWriter

import config.Settings
import org.apache.commons.io.FileUtils

import scala.util.Random

object LogProducer extends App {
  val wlc = Settings.WebLogGen
  val products = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/products.csv")).getLines().toArray
  val referrers = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/referrers.csv")).getLines().toArray
  val visitors = (0 to wlc.visitors).map("Visitor-" + _ )
  val pages = (0 to wlc.pages).map("Page-" + _ )

  val rnd = new Random()
  val filePath = wlc.filePath
  val destPath = wlc.destPath

  for(fileCount <- 1 to wlc.numberOfFiles) {

    val fw = new FileWriter(filePath, true)

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
      fw.write(line)
      if (iteration % incrementTimeEvery == 0) {
        println(s"Sent $iteration messages")
        val sleeping = rnd.nextInt(incrementTimeEvery * 60)
        println(s"Sleeping for $sleeping ms")
        Thread sleep sleeping
      }
    }
    fw.close()

    val outputFile = FileUtils.getFile(s"${destPath}\\data_$timestamp")
    println(s"Moving produced data to $outputFile")
    FileUtils.moveFile(FileUtils.getFile(filePath), outputFile)
    val sleeping = 5000
    println(s"Sleeping for $sleeping ms")

  }
}
