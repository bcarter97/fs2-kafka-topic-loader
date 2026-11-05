package integration

import base.KafkaSpecBase
import cats.data.{NonEmptyList, State}
import cats.effect.{Deferred, IO, Ref}
import cats.syntax.all.*
import fs2.Stream
import load.LoadAndRunExample
import org.typelevel.log4cats.LoggerFactory
import org.typelevel.log4cats.slf4j.Slf4jFactory
import utils.KafkaContainer.KafkaConfig
import utils.KafkaTestContainer

import scala.concurrent.duration.*

final class LoadAndRunExampleIntSpec extends KafkaSpecBase[IO], KafkaTestContainer[IO] {

  private given LoggerFactory[IO] = Slf4jFactory.create[IO]

  "LoadAndRunExample.kafka" should {
    behave like loadAndRunExample(partitioned = false)
  }

  "LoadAndRunExample.kafkaPartitioned" should {
    behave like loadAndRunExample(partitioned = true)
  }

  private def loadAndRunExample(partitioned: Boolean): Unit = {
    "complete loaded with every existing record in the store" in withRunningKafka { implicit kafkaConfig =>
      val existing = records(1 to 30)

      for {
        _       <- createCustomTopics(NonEmptyList.one(testTopic1), partitions = 3)
        _       <- publishStringMessages(testTopic1, existing)
        example <- newExample
        loaded  <- run(partitioned, example).compile.drain.background.surround(example.loaded.get.timeout(30.seconds))
      } yield loaded shouldBe existing.toMap
    }

    "keep updating the store after loading" in withRunningKafka { implicit kafkaConfig =>
      val existing = records(1 to 30)
      val updates  = existing.take(5).map((k, v) => (k, v.reverse)) ++ records(31 to 35)

      for {
        _         <- createCustomTopics(NonEmptyList.one(testTopic1), partitions = 3)
        _         <- publishStringMessages(testTopic1, existing)
        example   <- newExample
        assertion <- run(partitioned, example).compile.drain.background.surround {
                       for {
                         _         <- example.loaded.get.timeout(30.seconds)
                         _         <- publishStringMessages(testTopic1, updates)
                         assertion <- eventually(
                                        example.store.get.asserting(_ shouldBe existing.toMap ++ updates.toMap)
                                      )
                       } yield assertion
                     }
      } yield assertion
    }
  }

  private val newExample: IO[LoadAndRunExample[IO]] =
    (Ref.of[IO, Map[String, String]](Map.empty).map(SlowUpdateRef(_, 20.millis)), Deferred[IO, Map[String, String]])
      .mapN(LoadAndRunExample.apply)

  private final class SlowUpdateRef[A](underlying: Ref[IO, A], delay: FiniteDuration) extends Ref[IO, A] {
    override def get: IO[A]                                                        = underlying.get
    override def set(a: A): IO[Unit]                                               = underlying.set(a)
    override def access: IO[(A, A => IO[Boolean])]                                 = underlying.access
    override def tryUpdate(f: A => A): IO[Boolean]                                 = underlying.tryUpdate(f)
    override def tryModify[B](f: A => (A, B)): IO[Option[B]]                       = underlying.tryModify(f)
    override def update(f: A => A): IO[Unit]                                       = IO.sleep(delay) >> underlying.update(f)
    override def modify[B](f: A => (A, B)): IO[B]                                  = underlying.modify(f)
    override def tryModifyState[B](state: State[A, B]): IO[Option[B]] = underlying.tryModifyState(state)
    override def modifyState[B](state: State[A, B]): IO[B]            = underlying.modifyState(state)
  }

  private def run(partitioned: Boolean, example: LoadAndRunExample[IO])(using KafkaConfig): Stream[IO, Unit] =
    if (partitioned) LoadAndRunExample.kafkaPartitioned(NonEmptyList.one(testTopic1), example, maxConcurrency = 3)
    else LoadAndRunExample.kafka(NonEmptyList.one(testTopic1), example)
}
