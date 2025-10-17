lazy val commonSettings = Seq(
  name := "rtb-actions",
  version := "1.0",
  scalaVersion := "2.13.6"
)

lazy val common = ProjectRef(file("../common"), "common")

lazy val root = (project in file(".")).dependsOn(common)
  .settings(
    commonSettings,
    name := "rtb-actions",
    libraryDependencies ++= {
      val akkaVersion = "2.6.3"
      val akkaHttpV = "10.1.11"
      val slickV = "3.2.0"
      val circeVersion = "0.12.3"
      Seq(
        "com.typesafe.akka" %% "akka-stream" % akkaVersion,
        "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion,
        "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
        "com.typesafe.akka" %% "akka-http-core" % akkaHttpV,
        "com.typesafe.akka" %% "akka-http" % akkaHttpV,
        "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpV,
        "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpV,
        "com.typesafe.akka" %% "akka-http-jackson" % akkaHttpV,
        "com.typesafe.akka" %% "akka-http-xml" % akkaHttpV,
        "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
        "ch.qos.logback" % "logback-classic" % "1.2.3",
        "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
        "org.slf4j" % "log4j-over-slf4j" % "1.7.25",
        "org.slf4j" % "jul-to-slf4j" % "1.7.25",
        "mysql" % "mysql-connector-java" % "5.1.16",
        "org.json4s" %% "json4s-jackson" % "4.0.3",
        "org.json4s" %% "json4s-xml" % "4.0.3",
        "org.json4s" %% "json4s-native" % "4.0.3",
        "org.typelevel" %% "cats-core" % "2.6.1",
        "org.typelevel" %% "cats-kernel" % "2.6.1",
        "com.google.guava" % "guava" % "31.0.1-jre",
        "org.apache.commons" % "commons-math3" % "3.6.1",
        "commons-codec" % "commons-codec" % "1.15",
        "com.typesafe.akka" %% "akka-stream-kafka" % "2.0.7",
        "org.fluentd" % "fluent-logger" % "0.3.4",
        "com.aerospike" % "aerospike-client" % "5.1.11",
        "io.netty" % "netty-all" % "4.1.68.Final"
      )
    }
  )

scalacOptions in Test ++= Seq("-Yrangepos")

enablePlugins(JavaAppPackaging)
enablePlugins(SystemVPlugin)
