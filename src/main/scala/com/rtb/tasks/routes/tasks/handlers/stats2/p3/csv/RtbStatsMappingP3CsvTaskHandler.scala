package com.rtb.tasks.routes.tasks.handlers.stats2.p3.csv

import com.common.utils.logging.LoggingSupport
import com.rtb.tasks.config.{Config, ConfigSupport}
import com.rtb.tasks.routes.tasks.handlers.TaskHandler
import com.rtb.tasks.routes.tasks.models.TaskRequest
import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}

/**
 * Created by Niv on 27/07/2025
 */
class RtbStatsMappingP3CsvTaskHandler(val config: Config) extends TaskHandler with ConfigSupport with LoggingSupport {

  override def handle(taskRequest: TaskRequest): Unit = {
    debug(s"running RtbStatsMappingP3CsvTaskHandler.")
    writeSspEndpointsMapping()
    writeDspMapping()
  }

  private def writeSspEndpointsMapping(): Unit = {
    val data = getSspEndpointsMapping
    saveToCsv(data, "ssps")
  }

  private def writeDspMapping(): Unit = {
    val data = getDspMapping
    saveToCsv(data, "dsps")
  }

  private def getSspEndpointsMapping: Seq[Map[String, String]] = {
    dao.getRtbSspEndpointsData
      .filter(e => dao.getRtbSspData(e.sspId).exists(_.partnerId == "3"))
      .collect { case e if e.titleApi != "" && e.titleApi.toLowerCase.contains("appstock") => Map("id" -> e.id, "title" -> e.titleApi) }
  }

  private def getDspMapping: Seq[Map[String, String]] = {
    dao.getRtbDspsData
      .filter(d => d.partnerId == "3")
      .collect { case e if e.titleApi != "" && e.titleApi.toLowerCase.contains("appstock") => Map("id" -> e.id, "title" -> e.titleApi) }
  }

  private def saveToCsv(data: Seq[Map[String, String]], fileName: String): Unit = {
    writeCsv(data, s"/usr/share/data/mappings/$fileName.csv")
  }

  def writeCsv(data: Seq[Map[String, String]], filePath: String): Unit = {
    if (data.isEmpty) return

    // Ensure parent directories exist
    val parentDir = Paths.get(filePath).getParent
    if (parentDir != null) {
      Files.createDirectories(parentDir)
    }

    val writer = new PrintWriter(new File(filePath))

    try {
      // Collect all possible headers from the data
      val headers = data.flatMap(_.keys).distinct
      writer.println(headers.mkString(","))

      // Sort rows by "id" (safely parsing to Int, fallback = 0)
      val sortedData = data.sortBy { row =>
        row.get("id").flatMap(s => scala.util.Try(s.toInt).toOption).getOrElse(0)
      }

      // Write each row
      sortedData.foreach { row =>
        val values = headers.map { h =>
          val v = row.getOrElse(h, "")
          escapeCsv(v)
        }
        writer.println(values.mkString(","))
      }
    } finally {
      writer.close()
    }
  }

  // Escape function for CSV-safe values
  private def escapeCsv(value: String): String = {
    if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
      "\"" + value.replace("\"", "\"\"") + "\""
    } else {
      value
    }
  }
}