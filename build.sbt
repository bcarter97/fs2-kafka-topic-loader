import Dependencies.*

lazy val scmUrl = "https://github.com/bcarter97/fs2-kafka-topic-loader"

organization  := "io.github.bcarter97"
description   := "Read the contents of provided Kafka topics"
licenses      := List(License("BSD-3-Clause", uri("https://opensource.org/licenses/BSD-3-Clause")))
homepage      := Some(uri(scmUrl))
scmInfo       := Some(ScmInfo(uri(scmUrl), "scm:git@github.com:bcarter97/fs2-kafka-topic-loader.git"))
versionScheme := Some("early-semver")
developers    := List(
  Developer(
    "bcarter97",
    "Ben Carter",
    "benjohn.carter@gmail.com",
    uri("https://github.com/bcarter97")
  )
)

scalaVersion      := "3.9.0"
semanticdbEnabled := true
semanticdbVersion := scalafixSemanticdb.revision
CommonSettings.default

lazy val root = rootProject.autoAggregate
  .settings(
    name                     := "fs2-kafka-topic-loader",
    Test / parallelExecution := false,
    Test / fork              := true,
    libraryDependencies ++= Seq(
      Cats.core,
      Cats.effect,
      Cats.log4cats,
      Cats.log4catsSlf4j  % Test,
      Fs2.core,
      Fs2.kafka,
      catsEffectTesting   % Test,
      logbackClassic      % Test,
      scalaTest           % Test,
      testContainersKafka % Test
    )
  )

lazy val it = (project in file("it"))
  .settings(
    name           := "integration-test",
    publish / skip := true
  )
  .dependsOn(root % "test->test;compile->compile")

Global / scalafmtOnCompile := true
