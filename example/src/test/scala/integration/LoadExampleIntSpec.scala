package integration

import base.KafkaSpecBase
import cats.data.NonEmptyList
import cats.effect.{IO, Ref}
import fs2.Stream
import fs2.kafka.*
import load.LoadExample
import org.apache.kafka.common.TopicPartition
import org.scalatest.Assertion
import org.typelevel.log4cats.LoggerFactory
import org.typelevel.log4cats.slf4j.Slf4jFactory
import utils.KafkaContainer.KafkaConfig
import utils.KafkaTestContainer

final class LoadExampleIntSpec extends KafkaSpecBase[IO], KafkaTestContainer[IO] {

  "LoadExample" should {
    "load previously seen messages into the store" in withRunningKafka { implicit kafkaConfig =>
      testContext { ctx =>
        import ctx.*

        for {
          _      <- publishStringMessage(inputTopic, "key1", "value1")
          _      <- runAppAndDiscard
          _      <- publishStringMessage(inputTopic, "key2", "value2")
          result <- runApp
        } yield result should contain theSameElementsInOrderAs List("value1", "value2")
      }
    }

    "not publish previously committed messages" in withRunningKafka { implicit kafkaConfig =>
      testContext { ctx =>
        import ctx.*

        for {
          _      <- publishStringMessage(inputTopic, "key1", "value1")
          _      <- runAppAndDiscard
          _      <- consumeStringMessage(outputTopic, autoCommit = true)
          _      <- publishStringMessage(inputTopic, "key2", "value2")
          _      <- runAppAndDiscard
          result <- consumeStringMessage(outputTopic, autoCommit = true)
        } yield result shouldBe "value2"
      }
    }
  }

  final case class TestContext()(using KafkaConfig) {

    val inputTopic  = "test-topic-1"
    val outputTopic = "output-topic-1"
    val appGroupId  = "load-example-consumer-group"

    private given LoggerFactory[IO] = Slf4jFactory.create[IO]

    given consumerSettings(using kafkaConfig: KafkaConfig): ConsumerSettings[IO, String, String] =
      ConsumerSettings[IO, String, String]
        .withBootstrapServers(s"localhost:${kafkaConfig.kafkaPort}")
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withGroupId(appGroupId)

    given producerSettings(using kafkaConfig: KafkaConfig): ProducerSettings[IO, String, String] =
      ProducerSettings[IO, String, String]
        .withBootstrapServers(s"localhost:${kafkaConfig.kafkaPort}")

    private val caughtUp: IO[Boolean] =
      for {
        endOffsets <- withConsumer(autoCommit = false) { consumer =>
                        consumer.partitionsFor(inputTopic).flatMap { partitions =>
                          consumer.endOffsets(partitions.map(p => TopicPartition(inputTopic, p.partition)).toSet)
                        }
                      }
        committed  <- withAdminClient(_.listConsumerGroupOffsets(appGroupId).partitionsToOffsetAndMetadata)
      } yield endOffsets.forall((tp, end) => committed.get(tp).exists(_.offset >= end))

    val runApp: IO[List[String]] =
      for {
        store  <- Ref.empty[IO, List[String]]
        _      <- LoadExample
                    .kafka[IO](topics = NonEmptyList.one(inputTopic), outputTopic = outputTopic, store = store)
                    .stream
                    .interruptWhen(Stream.eval(eventually(caughtUp.asserting(_ shouldBe true))).as(true))
                    .compile
                    .drain
        stored <- store.get
      } yield stored

    val runAppAndDiscard: IO[Unit] = runApp.void
  }

  private def testContext(test: TestContext => IO[Assertion])(using KafkaConfig): IO[Assertion] = {
    val testContext = TestContext()

    test(testContext)
  }
}
