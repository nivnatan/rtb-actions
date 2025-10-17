package com.rtb.tasks

import java.net.InetSocketAddress
import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, ActorRef, ActorSystem, AllForOneStrategy, Props, Status, Terminated}
import akka.http.scaladsl.Http
import com.common.config.ServerConfiguration
import com.common.utils.ActorApp
import com.common.utils.logging.LoggingSupport
import com.rtb.tasks.routes.TasksRoutes
import com.rtb.tasks.config.Config
import akka.pattern.pipe
import scala.concurrent.ExecutionContext

/**
  * Created by Niv on 11/12/2021
  */
object TasksServer extends ActorApp {

  override def manager: Props = {
    val interface = ServerConfiguration.Interface
    val port      = ServerConfiguration.Port
    Props(
      new TasksServer(interface, port)
    )
  }
}

class TasksServer(interface: String, port: Int) extends Actor with LoggingSupport with TasksRoutes {

  implicit protected val system: ActorSystem           = context.system
  implicit protected val ec: ExecutionContext          = context.dispatcher
  implicit protected val classLoader: ClassLoader      = TasksServer.getClass.getClassLoader

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
