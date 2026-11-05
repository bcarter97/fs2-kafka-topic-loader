package utils

import java.util.UUID

import base.AsyncIntSpec
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.syntax.all.*
import cats.effect.{Async, Resource}
import cats.syntax.all.*
import fs2.Stream
import fs2.kafka.{AutoOffsetReset, ConsumerRecord, ConsumerSettings, KafkaConsumer}
import io.github.bcarter97.fs2.kafka.topicloader.{LoadTopicStrategy, TopicLoader}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.scalatest.Assertion
import org.scalatest.concurrent.AbstractPatienceConfiguration
import org.scalatest.exceptions.TestFailedException
import org.typelevel.log4cats.LoggerFactory
import org.typelevel.log4cats.slf4j.Slf4jFactory
import utils.KafkaContainer.KafkaConfig

import scala.concurrent.duration.*

trait KafkaHelpers[F[_]] {
  self: AsyncIntSpec[F] & EmbeddedKafka[F] & AbstractPatienceConfiguration =>

  val groupId    = "test-consumer-group"
  val testTopic1 = "load-state-topic-1"
  val testTopic2 = "load-state-topic-2"

  given consumerSettings(using
      kafkaConfig: KafkaConfig,
      F: Async[F]
  ): ConsumerSettings[F, String, String] =
    ConsumerSettings[F, String, String]
      .withBootstrapServers(s"localhost:${kafkaConfig.kafkaPort}")
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withGroupId(groupId)

  val aggressiveCompactionConfig = Map(
    "cleanup.policy"            -> "compact",
    "delete.retention.ms"       -> "0",
    "min.cleanable.dirty.ratio" -> "0.01",
    "segment.ms"                -> "1"
  )

  val aggressiveDeletionConfig = Map(
    "cleanup.policy"            -> "delete",
    "delete.retention.ms"       -> "0",
    "retention.ms"              -> "0",
    "min.cleanable.dirty.ratio" -> "0.01",
    "segment.ms"                -> "1"
  )

  def records(r: Seq[Int]): Seq[(String, String)] = r.map(i => s"k$i" -> s"v$i")

  def recordToTuple[K, V](record: ConsumerRecord[K, V]): (K, V) = (record.key, record.value)

  /*
   * Note: Compaction is only triggered if messages are published as a separate statement.
   */
  def publishToKafkaAndTriggerCompaction(
      partitions: NonEmptySet[TopicPartition],
      messages: Seq[(String, String)]
  )(using kafkaConfig: KafkaConfig, F: Async[F]): F[Unit] = {
    val topic      = partitions.map(_.topic()).toList.head
    val fillerSize = 100
    val filler     = List.fill(fillerSize)(UUID.randomUUID().toString).map(x => (x, x))

    publishStringMessages(topic, messages) *> publishStringMessages(topic, filler)
  }

  def runLoader(
      topics: NonEmptyList[String],
      strategy: LoadTopicStrategy
  )(using consumerSettings: ConsumerSettings[F, String, String], F: Async[F]): F[List[(String, String)]] = {
    given LoggerFactory[F] = Slf4jFactory.create[F]
    TopicLoader
      .load(topics, strategy, consumerSettings)
      .compile
      .toList
      .map(_.map(recordToTuple))
      .timeoutTo(
        patienceConfig.timeout,
        F.raiseError(TestFailedException(s"TopicLoader did not complete after ${patienceConfig.timeout.toSeconds}s", 0))
      )
  }

  def runPartitionedLoader(
      topics: NonEmptyList[String],
      strategy: LoadTopicStrategy
  )(using
      consumerSettings: ConsumerSettings[F, String, String],
      F: Async[F]
  ): F[List[(TopicPartition, ConsumerRecord[String, String])]] = {
    given LoggerFactory[F] = Slf4jFactory.create[F]
    TopicLoader
      .loadPartitioned(topics, strategy, consumerSettings)
      .map((tp, records) => records.map(tp -> _))
      .parJoinUnbounded
      .compile
      .toList
      .timeoutTo(
        patienceConfig.timeout,
        F.raiseError(TestFailedException(s"TopicLoader did not complete after ${patienceConfig.timeout.toSeconds}s", 0))
      )
  }

  def loadAndRunR(topics: NonEmptyList[String])(
      onLoad: Resource.ExitCase => F[Unit],
      onRecord: ((String, String)) => F[Unit]
  )(using
      consumerSettings: ConsumerSettings[F, String, String],
      F: Async[F]
  ): Resource[F, Unit] =
    loadAndRunLoader(topics)(onLoad)
      .map(recordToTuple)
      .evalTap(onRecord)
      .compile
      .drain
      .background
      .void

  def loadAndRunPartitionedR(topics: NonEmptyList[String])(
      onLoad: Resource.ExitCase => F[Unit],
      onRecord: ((String, String)) => F[Unit]
  )(using
      consumerSettings: ConsumerSettings[F, String, String],
      F: Async[F]
  ): Resource[F, Unit] =
    loadAndRunPartitionedLoader(topics)(onLoad)
      .map((_, records) => records.map(recordToTuple).evalTap(onRecord))
      .parJoinUnbounded
      .compile
      .drain
      .background
      .void

  def loadAndRunPartitionedLoader(topics: NonEmptyList[String])(onLoad: Resource.ExitCase => F[Unit])(using
      consumerSettings: ConsumerSettings[F, String, String],
      F: Async[F]
  ): Stream[F, (TopicPartition, Stream[F, ConsumerRecord[String, String]])] = {
    given LoggerFactory[F] = Slf4jFactory.create[F]
    TopicLoader.loadAndRunPartitioned(topics, consumerSettings)(onLoad)
  }

  def loadAndRunLoader(topics: NonEmptyList[String])(onLoad: Resource.ExitCase => F[Unit])(using
      consumerSettings: ConsumerSettings[F, String, String],
      F: Async[F]
  ): Stream[F, ConsumerRecord[String, String]] = {
    given LoggerFactory[F] = Slf4jFactory.create[F]
    TopicLoader.loadAndRun(topics, consumerSettings)(onLoad)
  }

  def moveOffsetToEnd(
      partitions: NonEmptySet[TopicPartition]
  )(using kafkaConfig: KafkaConfig, F: Async[F]): F[Unit] =
    withAssignedConsumer(
      autoCommit = true,
      offsetReset = AutoOffsetReset.Earliest,
      partitions = partitions,
      groupId = groupId
    ) { consumer =>
      for {
        endOffsets         <- consumer.endOffsets(partitions.toSortedSet)
        offsetsAndMetadata <- F.pure(endOffsets.view.mapValues(OffsetAndMetadata(_, "")).toMap)
        _                  <- consumer.commitSync(offsetsAndMetadata)
      } yield ()
    }

  def publishToKafkaAndWaitForCompaction(
      partitions: NonEmptySet[TopicPartition],
      messages: Seq[(String, String)]
  )(using kafkaConfig: KafkaConfig, F: Async[F]): F[Unit] = for {
    _ <- publishToKafkaAndTriggerCompaction(partitions, messages)
    _ <- waitForCompaction(partitions)
  } yield ()

  def publishToKafkaAndWaitForDeletion(
      partitions: NonEmptySet[TopicPartition],
      messages: Seq[(String, String)]
  )(using kafkaConfig: KafkaConfig, F: Async[F]): F[Unit] = for {
    _ <- publishToKafkaAndTriggerCompaction(partitions, messages)
    _ <- waitForDeletion(partitions)
  } yield ()

  def waitForCompaction(
      partitions: NonEmptySet[TopicPartition]
  )(using kafkaConfig: KafkaConfig, F: Async[F]): F[Assertion] =
    consumeEventually(partitions) { records =>
      val messageKeys = records.map((k, _) => k)
      messageKeys should not be empty
      messageKeys should contain theSameElementsAs messageKeys.toSet

    }

  def waitForDeletion(
      partitions: NonEmptySet[TopicPartition]
  )(using kafkaConfig: KafkaConfig, F: Async[F]): F[Assertion] =
    consumeEventually(partitions) { records =>
      records shouldBe empty
    }

  def consumeEventually(
      partitions: NonEmptySet[TopicPartition],
      groupId: String = UUID.randomUUID().toString
  )(
      f: List[(String, String)] => Assertion
  )(using kafkaConfig: KafkaConfig, F: Async[F]): F[Assertion] =
    eventually {
      val records: F[List[(String, String)]] = withAssignedConsumer(
        autoCommit = false,
        offsetReset = AutoOffsetReset.Earliest,
        partitions = partitions,
        groupId = groupId
      )(readToEnd(partitions))

      records.map(f)
    }

  private def readToEnd(partitions: NonEmptySet[TopicPartition])(
      consumer: KafkaConsumer[F, String, String]
  )(using F: Async[F]): F[List[(String, String)]] =
    for {
      beginning <- consumer.beginningOffsets(partitions.toSortedSet)
      end       <- consumer.endOffsets(partitions.toSortedSet)
      nonEmpty   = end.keySet.filter(tp => end(tp) > beginning(tp))
      records   <- if (nonEmpty.isEmpty) F.pure(List.empty)
                   else
                     consumer.records
                       .map(_.record)
                       .mapAccumulate(nonEmpty) { (remaining, record) =>
                         val tp = TopicPartition(record.topic, record.partition)
                         (if (record.offset + 1 >= end(tp)) remaining - tp else remaining, recordToTuple(record))
                       }
                       .takeThrough((remaining, _) => remaining.nonEmpty)
                       .map((_, record) => record)
                       .interruptAfter(10.seconds)
                       .compile
                       .toList
    } yield records

  def withAssignedConsumer[T](
      autoCommit: Boolean,
      offsetReset: AutoOffsetReset,
      partitions: NonEmptySet[TopicPartition],
      groupId: String = UUID.randomUUID().toString
  )(f: KafkaConsumer[F, String, String] => F[T])(using kafkaConfig: KafkaConfig, F: Async[F]): F[T] =
    withConsumer(autoCommit, offsetReset, groupId)(consumer => consumer.assign(partitions) >> f(consumer))
}
