import sbt.*

object Dependencies {

  object Cats {
    lazy val core          = "org.typelevel" %% "cats-core"      % "2.13.0"
    lazy val effect        = "org.typelevel" %% "cats-effect"    % "3.7.1"
    lazy val log4cats      = "org.typelevel" %% "log4cats-core"  % "2.8.0"
    lazy val log4catsSlf4j = "org.typelevel" %% "log4cats-slf4j" % "2.8.0"
  }

  object Fs2 {
    lazy val core  = "co.fs2"          %% "fs2-core"  % "3.14.0"
    lazy val kafka = "com.github.fd4s" %% "fs2-kafka" % "3.9.1"
  }

  lazy val testContainersKafka = "org.testcontainers" % "kafka"                         % "1.20.1"
  lazy val scalaTest           = "org.scalatest"     %% "scalatest"                     % "3.2.20"
  lazy val catsEffectTesting   = "org.typelevel"     %% "cats-effect-testing-scalatest" % "1.8.0"

  lazy val logbackClassic = "ch.qos.logback" % "logback-classic" % "1.6.6"
}
