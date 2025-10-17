package com.rtb.tasks.routes.tasks.handlers.currencies

import java.io.IOException
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import com.common.utils.logging.LoggingSupport
import com.rtb.tasks.config.{Config, ConfigSupport}
import com.rtb.tasks.routes.tasks.handlers.TaskHandler
import com.rtb.tasks.routes.tasks.handlers.currencies.models.CurrenciesData
import com.rtb.tasks.routes.tasks.models.TaskRequest
import scala.concurrent.Future
import com.common.utils.types.TypesUtil._
import com.common.utils.json.JsonFormats._
import com.common.utils.json.Json4sSupport._

/**
  * Created by Niv on 09/08/2023
  */
final class CurrenciesApiTaskHandler(val config: Config) extends TaskHandler with ConfigSupport with LoggingSupport {

  override def handle(taskRequest: TaskRequest): Unit = {
    debug("starting currencies api task.")
    getCurrencies(taskRequest)
      .map(save(_, taskRequest))
      .toFutureEither
      .foreach { result =>
        debug(s"currencies api task completed. result => $result")
      }
  }

  private def getCurrencies(taskRequest: TaskRequest): Future[CurrenciesData] = {
    Http().singleRequest(
      HttpRequest(
        uri = Uri("https://openexchangerates.org/api/latest.json?app_id=1df33e92e20b4ed2b989e3573b90969b"),
        method = HttpMethods.GET
    )).flatMap { response =>
      response.status match {
        case StatusCodes.OK =>
          Unmarshal[ResponseEntity](response.entity.withContentType(ContentTypes.`application/json`)).to[CurrenciesData]
        case _ => {
          response.discardEntityBytes()
          val errorMsg = s"bidswitch api get token failed with status code ${response.status}"
          info(s"$errorMsg")
          Future.failed(new IOException(errorMsg))
        }
      }
    }
  }

  private def save(currenciesData: CurrenciesData, taskRequest: TaskRequest): Unit = {
    require(currenciesData.base == "USD")
    currenciesData.rates.foreach { rate =>
      val sql = s"""|INSERT INTO currencies (iso3, rate, last_update) VALUES("${rate._1}", ${rate._2}, NOW()) ON DUPLICATE KEY UPDATE
                    |iso3="${rate._1}", rate=${rate._2}, last_update=NOW()
                    |""".stripMargin

      rtbDb.insert(sql)
    }
  }
}