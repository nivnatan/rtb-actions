package com.rtb.admin

import java.net.InetSocketAddress
import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, ActorRef, ActorSystem, AllForOneStrategy, Props, Status, Terminated}
import akka.http.scaladsl.Http
import com.common.config.ServerConfiguration
import com.common.utils.ActorApp
import com.common.utils.logging.LoggingSupport
import com.rtb.admin.config.Config
import com.rtb.admin.routes.ActionsRoutes
import scala.concurrent.ExecutionContext
import akka.pattern.pipe

/**
  * Created by Niv on 11/12/2021
  */
object ActionsServer extends ActorApp {

  override def manager: Props = {
    val interface = ServerConfiguration.Interface
    val port      = ServerConfiguration.Port
    Props(
      new ActionsServer(interface, port)
    )
  }
}

class ActionsServer(interface: String, port: Int) extends Actor with LoggingSupport with ActionsRoutes {

  implicit protected val system: ActorSystem           = context.system
  implicit protected val ec: ExecutionContext          = context.dispatcher
  implicit protected val classLoader: ClassLoader      = ActionsServer.getClass.getClassLoader

  protected val config                                 = Config(context)

  Http(context.system)
    .bindAndHandle(
      routes,
      interface,
      port
    )
    .pipeTo(context.self)

  override def receive: PartialFunction[Any, Unit] = {
    case Http.ServerBinding(a)                    => handleServerBinding(a)
    case Status.Failure(c)                        => handleBindFailure(c)
    case Terminated(actor)                        => handleTerminated(actor)
  }

  private def handleServerBinding(address: InetSocketAddress): Unit = {
    info(s"listening on $address")
  }

  private def handleBindFailure(cause: Throwable): Unit = {
    error(s"can't bind to $interface:$port! reason - $cause")
    context.stop(context.self)
  }

  private def handleTerminated(actor: ActorRef): Unit = {
    logger.error("Terminating the system because {} terminated!", actor.path)
    context.system.terminate()
  }

  override val supervisorStrategy: AllForOneStrategy = AllForOneStrategy() {
    case e =>
      error("rtb tasks has received an error. error - " + e)
      Stop
  }
}
