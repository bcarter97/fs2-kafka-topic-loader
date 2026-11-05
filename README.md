[![CI](https://github.com/bcarter97/fs2-kafka-topic-loader/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/bcarter97/fs2-kafka-topic-loader/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.bcarter97/fs2-kafka-topic-loader_3)](https://central.sonatype.com/artifact/io.github.bcarter97/fs2-kafka-topic-loader_3)

# fs2-kafka-topic-loader

Reads the contents of provided Kafka topics.

> [!NOTE]
> This project was originally developed at Sky as
> [`sky-uk/fs2-kafka-topic-loader`](https://github.com/sky-uk/fs2-kafka-topic-loader) and is now independently
> maintained. It is not affiliated with or endorsed by Sky.

This library is aimed for usage in applications that want a deterministic stream of Kafka messages that completes once
the last message (determined above) has been read. This is useful if an application shouldn't respond to new events
before it has processed all previously seen messages, or if Kafka is being used as a data store and the entire contents
of a topic needs to be reloaded on an application restart.

## Usage

### `Load`

Determined by the `LoadTopicStrategy`, this stream completes once all messages have been read.

Load strategies:

- `LoadAll` - reads the topics in their entirety
- `LoadCommitted` - reads up to the configured consumer-group's last committed Offset

See `LoadExample.kafka` in [`LoadExample.scala`](./example/src/main/scala/load/LoadExample.scala).

### `LoadPartitioned`

Like `Load`, but emits one stream per partition so that partitions can be processed concurrently, e.g. with `parJoin`.
Partition streams must run within the outer stream. See `LoadExample.kafkaPartitioned` in
[`LoadExample.scala`](./example/src/main/scala/load/LoadExample.scala).

### `LoadAndRun`

Read up to the latest offset, fire a callback and continue streaming messages.

## Example

Requires Scala 3.9 or later. Add the following to your `build.sbt`:

```scala
libraryDependencies += "io.github.bcarter97" %% "fs2-kafka-topic-loader" % "<version>"
```

### Load

```scala
import cats.data.NonEmptyList
import cats.effect.{IO, IOApp}
import fs2.kafka.ConsumerSettings
import org.typelevel.log4cats.LoggerFactory
import io.github.bcarter97.fs2.kafka.topicloader.{LoadAll, TopicLoader}

object Main extends IOApp.Simple {
  val consumerSettings: ConsumerSettings[IO, String, String] = ???

  given LoggerFactory[IO] = ???

  override def run: IO[Unit] =
    TopicLoader.load(NonEmptyList.one("topicToLoad"), LoadAll, consumerSettings).evalTap(IO.println).compile.drain
}
```

### LoadAndRun

```scala
import cats.data.NonEmptyList
import cats.effect.Resource.ExitCase
import cats.effect.{IO, IOApp, Ref}
import fs2.kafka.ConsumerSettings
import org.typelevel.log4cats.LoggerFactory
import io.github.bcarter97.fs2.kafka.topicloader.{LoadAll, TopicLoader}

object Main extends IOApp.Simple {
  val consumerSettings: ConsumerSettings[IO, String, String] = ???

  given LoggerFactory[IO] = ???

  val healthCheck: IO[Ref[IO, Boolean]] = Ref.of(false)

  val logger = LoggerFactory[IO].getLogger

  override def run: IO[Unit] =
    for {
      healthCheck <- healthCheck
      _           <- TopicLoader
                       .loadAndRun(NonEmptyList.one("topicToLoad"), consumerSettings) {
                         case ExitCase.Succeeded  => healthCheck.set(true)
                         case ExitCase.Errored(e) => logger.error(e)(s"Something went wrong: $e")
                         case ExitCase.Canceled   => logger.warn("Stream was cancelled before loading")
                       }
                       .compile
                       .drain
    } yield ()
}
```

## Configuration

Configuration from the Topic Loader is done via the `ConsumerSettings`. The group id of the Topic Loader should match
the group id of your application.

## Contributing

Contributions are welcomed! For contributions see [here](./CONTRIBUTING.md) for more information.
