package com.rtb.tasks.routes.tasks.handlers.bidswitch

import java.io.IOException
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.unmarshalling.Unmarshal
import com.common.rtb.models.data.RtbSspEndpointData
import com.common.utils.json.JsonUtil
import com.common.utils.logging.LoggingSupport
import com.rtb.tasks.config.{Config, ConfigSupport}
import com.rtb.tasks.routes.tasks.handlers.TaskHandler
import com.rtb.tasks.routes.tasks.models.TaskRequest
import scala.concurrent.Future
import scala.collection.immutable.Seq
import com.common.utils.types.TypesUtil._

/**
  * Created by Niv on 04/08/2023
  */
final class BidSwitchApiTaskHandler(val config: Config) extends TaskHandler with ConfigSupport with LoggingSupport {

  override def handle(taskRequest: TaskRequest): Unit = {
    debug("starting BidSwitch api task.")

    (for {
      token     <- getToken(taskRequest)
      sspData   <- getSspsData(taskRequest)
      status    <- upload(sspData, token, taskRequest)
    } yield status)
      .toFutureEither
      .foreach { result =>
        debug(s"bidswitch api task completed. result => $result")
      }
  }

  private def getToken(taskRequest: TaskRequest): Future[String] = {
    Http().singleRequest(
      HttpRequest(
      uri = Uri("https://iam.bidswitch.com/auth/realms/bidswitch/protocol/openid-connect/token"),
      method = HttpMethods.POST,
      entity = akka.http.scaladsl.model.FormData(
        Map(
          "client_id"   -> "public-client",
          "grant_type"  -> "password",
          "username"    -> "support@keenkale.com",
          "password"    -> "auUXJ%AHqGA=9yR<1",
          "scope"       -> "openid email profile"
        )).toEntity,
      protocol = HttpProtocols.`HTTP/1.1`)
    ).flatMap { response =>
      response.status match {
        case StatusCodes.OK =>
          Unmarshal[ResponseEntity](response.entity.withContentType(ContentTypes.`application/json`)).to[String].map { res =>
            JsonUtil.fromJson[Map[String, Any]](res).toOption.getOrElse(Map.empty).get("access_token").map(_.toString).get
          }
        case _ => {
          response.discardEntityBytes()
          val errorMsg = s"bidswitch api get token failed with status code ${response.status}"
          info(s"$errorMsg")
          Future.failed(new IOException(errorMsg))
        }
      }
    }
  }

  private def getSspsData(taskRequest: TaskRequest): Future[Seq[SspData]] = {
    Future.sequence(
      taskRequest.params.get("ssp_ids").map(_.split(",").map(dao.getRtbSspEndpointData)
        .collect { case Some(s) => s }).map(_.toSeq).getOrElse(Seq.empty)
        .map(getSspData(_, taskRequest))
    )
  }

  private def getSspData(sspData: RtbSspEndpointData, taskRequest: TaskRequest): Future[SspData] = {
    Http().singleRequest(
      HttpRequest(
        uri = Uri(s"https://api.rapidbidding.com/data/api/${sspData.token}?key=${sspData.apiKey.getOrElse("")}&date=${taskRequest.params("date")}"),
        method = HttpMethods.GET,
        protocol = HttpProtocols.`HTTP/1.1`)
    ).flatMap { response =>
      response.status match {
        case StatusCodes.OK =>
          Unmarshal[ResponseEntity](response.entity.withContentType(ContentTypes.`application/json`)).to[String].map { res =>
            val result = JsonUtil.fromJson[Map[String, String]](res).toOption.getOrElse(Map.empty)
            val impressions     = result.get("impressions").flatMap(_.toLongSafe).getOrElse(0L)
            val cost            = result.get("cost").flatMap(_.toDoubleSafe).getOrElse(0.0)
            SspData(sspData.id, impressions, cost)
          }
        case _ => {
          response.discardEntityBytes()
          val errorMsg = s"bidswitch get sspData failed with status code ${response.status}"
          info(s"$errorMsg")
          Future.failed(new IOException(errorMsg))
        }
      }
    }
  }

  private def upload(sspData: Seq[SspData], token: String, taskRequest: TaskRequest): Future[String] = {
    debug(s"upload. token=$token, sspData=${sspData.mkString(",")}")
    val imps = sspData.map(_.imps).sum
    val cost = sspData.map(_.cost).sum
    val json =
      s"""|
        |{
        |   "reports":[
        |      {
        |         "timezone":"UTC",
        |         "currency":"USD",
        |         "data":{
        |            "${taskRequest.params("date")}":[
        |               {
        |                  "imps":$imps,
        |                  "cost":$cost
        |               }
        |            ]
        |         }
        |      }
        |   ]
        |}
        |""".stripMargin

    Http().singleRequest(
      HttpRequest(
        uri = Uri("https://api.bidswitch.com/discrepancy-check/v1.0/dsp/481/upload-report/"),
        method = HttpMethods.POST,
        entity = HttpEntity(ContentTypes.`application/json`, json)).addCredentials(OAuth2BearerToken(token))
    ).flatMap { response =>
      response.status match {
        case StatusCodes.OK =>
          Unmarshal[ResponseEntity](response.entity.withContentType(ContentTypes.`application/json`)).to[String]
        case _ => {
          response.discardEntityBytes()
          val errorMsg = s"bidswitch api upload failed with status code ${response.status}"
          info(s"$errorMsg")
          Future.failed(new IOException(errorMsg))
        }
      }
    }
  }

  case class SspData(sspId: String, imps: Long = 0, cost: Double = 0.0)
}