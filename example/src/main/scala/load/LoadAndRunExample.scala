package load

import cats.data.NonEmptyList
import cats.effect.{Async, Deferred, Ref, Resource}
import cats.syntax.all.*
import fs2.Stream
import fs2.kafka.{ConsumerRecord, ConsumerSettings}
import io.github.bcarter97.fs2.kafka.topicloader.TopicLoader
import org.typelevel.log4cats.LoggerFactory

final case class LoadAndRunExample[F[_]](
    store: Ref[F, Map[String, String]],
    loaded: Deferred[F, Map[String, String]]
)

object LoadAndRunExample {

  def kafka[F[_] : Async : LoggerFactory](
      topics: NonEmptyList[String],
      example: LoadAndRunExample[F]
  )(using consumerSettings: ConsumerSettings[F, String, String]): Stream[F, Unit] =
    TopicLoader
      .loadAndRun(topics, consumerSettings)(onLoad(example))
      .evalMap(process(example))

  def kafkaPartitioned[F[_] : Async : LoggerFactory](
      topics: NonEmptyList[String],
      example: LoadAndRunExample[F],
      maxConcurrency: Int
  )(using consumerSettings: ConsumerSettings[F, String, String]): Stream[F, Unit] =
    TopicLoader
      .loadAndRunPartitioned(topics, consumerSettings)(onLoad(example))
      .map((_, records) => records.evalMap(process(example)))
      .parJoin(maxConcurrency)

  private def onLoad[F[_] : Async](example: LoadAndRunExample[F])(exitCase: Resource.ExitCase): F[Unit] =
    exitCase match {
      case Resource.ExitCase.Succeeded => example.store.get.flatMap(example.loaded.complete).void
      case _                           => Async[F].unit
    }

  private def process[F[_]](example: LoadAndRunExample[F])(record: ConsumerRecord[String, String]): F[Unit] =
    example.store.update(_.updated(record.key, record.value))
}
