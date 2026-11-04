import Dependencies.*

lazy val scala3                 = "3.9.0"
lazy val supportedScalaVersions = List(scala3)
lazy val scmUrl                 = "https://github.com/bcarter97/fs2-kafka-topic-loader"

ThisBuild / organization  := "io.github.bcarter97"
ThisBuild / description   := "Read the contents of provided Kafka topics"
ThisBuild / licenses      := List("BSD New" -> url("https://opensource.org/licenses/BSD-3-Clause"))
ThisBuild / homepage      := Some(url(scmUrl))
ThisBuild / scmInfo       := Some(ScmInfo(url(scmUrl), "scm:git@github.com:bcarter97/fs2-kafka-topic-loader.git"))
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / developers    := List(
  Developer(
    "bcarter97",
    "Ben Carter",
    "benjohn.carter@gmail.com",
    url("https://github.com/bcarter97")
  )
)

ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision

lazy val root = (project in file("."))
  .settings(
    name               := "fs2-kafka-topic-loader",
    scalaVersion       := scala3,
    crossScalaVersions := supportedScalaVersions,
    CommonSettings.default,
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
    name         := "integration-test",
    scalaVersion := scala3,
    publish      := false
  )
  .dependsOn(root % "test->test;compile->compile")

Test / parallelExecution := false
Test / fork              := true

Global / onChangedBuildSource := ReloadOnSourceChanges
Global / scalafmtOnCompile    := true
