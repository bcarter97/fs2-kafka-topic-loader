package io.github.bcarter97.fs2.kafka.topicloader

import cats.data.{NonEmptyList, NonEmptyMap}
import cats.effect.{Async, Deferred, Ref, Resource}
import cats.syntax.all.*
import cats.{Monad, Show}
import fs2.kafka.instances.*
import fs2.kafka.{ConsumerRecord, ConsumerSettings, KafkaConsumer}
import fs2.{Pipe, Pull, Stream}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.typelevel.log4cats.syntax.*
import org.typelevel.log4cats.{Logger, LoggerFactory}

import scala.collection.immutable.SortedMap

object TopicLoader extends TopicLoader {
  private[topicloader] case class LogOffsets(lowest: Long, highest: Long)

  private[topicloader] given Show[LogOffsets] =
    Show.show(lo => s"LogOffset(highest=${lo.highest}, lowest=${lo.lowest})")

  private case class PartitionLastOffset(topicPartition: TopicPartition, offset: Long)

  private case class HighestOffsetsWithRecord[K, V](
      partitionOffsets: Map[TopicPartition, Long],
      consumerRecord: Option[ConsumerRecord[K, V]] = none[ConsumerRecord[K, V]],
      partitionLastOffset: Option[PartitionLastOffset] = none[PartitionLastOffset]
  )

  private object WithRecord {
    def unapply[K, V](h: HighestOffsetsWithRecord[K, V]): Option[ConsumerRecord[K, V]] = h.consumerRecord
  }
}

trait TopicLoader {

  import TopicLoader.*

  /** Stream that loads the specified topics from the beginning and completes when the offsets reach the point specified
    * by the requested strategy.
    *
    * @param topics
    *   topics to load
    * @param strategy
    *   A [[LoadTopicStrategy]]
    * @param consumerSettings
    *   [[fs2.kafka.ConsumerSettings]] for the given topics
    */
  def load[F[_] : Async : LoggerFactory, K, V](
      topics: NonEmptyList[String],
      strategy: LoadTopicStrategy,
      consumerSettings: ConsumerSettings[F, K, V]
  ): Stream[F, ConsumerRecord[K, V]] =
    for {
      given Logger[F] <- Stream.eval(LoggerFactory[F].create)
      consumer        <- KafkaConsumer.stream(consumerSettings)
      record          <- load(topics, strategy, consumer)
    } yield record

  /** Stream of one stream per partition that loads the specified topics from the beginning. Each partition stream
    * completes when its offsets reach the point specified by the requested strategy, and the outer stream completes
    * once every partition stream has been emitted. Partitions with nothing to load are not emitted.
    *
    * The partition streams must be run within the scope of the outer stream, e.g. with `parJoin`.
    *
    * @param topics
    *   topics to load
    * @param strategy
    *   A [[LoadTopicStrategy]]
    * @param consumerSettings
    *   [[fs2.kafka.ConsumerSettings]] for the given topics
    */
  def loadPartitioned[F[_] : Async : LoggerFactory, K, V](
      topics: NonEmptyList[String],
      strategy: LoadTopicStrategy,
      consumerSettings: ConsumerSettings[F, K, V]
  ): Stream[F, (TopicPartition, Stream[F, ConsumerRecord[K, V]])] =
    for {
      given Logger[F] <- Stream.eval(LoggerFactory[F].create)
      consumer        <- KafkaConsumer.stream(consumerSettings)
      logOffsets      <- Stream.eval(logOffsetsForTopics(topics, strategy, consumer)).flatMap(Stream.fromOption(_))
      _               <- Stream.eval(info"log offsets: ${logOffsets.show}")
      partition       <- loadPartitioned(logOffsets, consumer)
    } yield partition

  /** Stream that loads the specified topics from the beginning. When the latest current offsets are reached, the
    * `onLoad` callback is evaluated, and the stream continues.
    *
    * @param topics
    *   topics to load
    * @param consumerSettings
    *   [[fs2.kafka.ConsumerSettings]] for the given topics
    * @param onLoad
    *   A callback that will be evaluated on once the current offsets are reached
    */
  def loadAndRun[F[_] : Async : LoggerFactory, K, V](
      topics: NonEmptyList[String],
      consumerSettings: ConsumerSettings[F, K, V]
  )(onLoad: Resource.ExitCase => F[Unit]): Stream[F, ConsumerRecord[K, V]] = {
    def postLoad(
        logOffsets: NonEmptyMap[TopicPartition, LogOffsets]
    )(using Logger[F]): Stream[F, ConsumerRecord[K, V]] =
      for {
        // The only consistent workaround for re-assigning offsets after the initial load is to re-create the consumer
        postLoadConsumer <- KafkaConsumer.stream(consumerSettings)
        _                <- Stream.eval(assignOffsets(logOffsets, postLoadConsumer)(_.highest))
        record           <- postLoadConsumer.records.map(_.record)
      } yield record

    for {
      given Logger[F] <- Stream.eval(LoggerFactory[F].create)
      preloadConsumer <- KafkaConsumer.stream(consumerSettings)
      logOffsets      <- Stream.eval(logOffsetsForTopics(topics, LoadAll, preloadConsumer)).flatMap(Stream.fromOption(_))
      _               <- Stream.eval(info"log offsets: ${logOffsets.show}")
      record          <- load(logOffsets, preloadConsumer).onFinalizeCase(onLoad) ++ postLoad(logOffsets)
    } yield record
  }

  /** Stream of one stream per partition that loads the specified topics from the beginning, and then continues
    * streaming new records. The `onLoad` callback is evaluated once every partition has been loaded up to its latest
    * offset at the time the stream started.
    *
    * A partition counts as loaded once its last loaded record has been pulled from its stream, so records should be
    * processed within each partition stream for `onLoad` to follow their processing. The partition streams must be run
    * within the scope of the outer stream, e.g. with `parJoin`.
    *
    * @param topics
    *   topics to load
    * @param consumerSettings
    *   [[fs2.kafka.ConsumerSettings]] for the given topics
    * @param onLoad
    *   A callback that will be evaluated once every partition has been loaded, or with the exit case of the stream if
    *   it terminates before then
    */
  def loadAndRunPartitioned[F[_] : Async : LoggerFactory, K, V](
      topics: NonEmptyList[String],
      consumerSettings: ConsumerSettings[F, K, V]
  )(onLoad: Resource.ExitCase => F[Unit]): Stream[F, (TopicPartition, Stream[F, ConsumerRecord[K, V]])] =
    for {
      given Logger[F] <- Stream.eval(LoggerFactory[F].create)
      loaded          <- Stream.eval(Deferred[F, Unit])
      completeLoad     = (exitCase: Resource.ExitCase) => loaded.complete(()).ifM(onLoad(exitCase), Async[F].unit)
      onUnloadedExit   = (exitCase: Resource.ExitCase) =>
                           exitCase match {
                             case Resource.ExitCase.Succeeded => Async[F].unit
                             case other                       => completeLoad(other)
                           }
      partition       <- {
                           for {
                             consumer   <- KafkaConsumer.stream(consumerSettings)
                             logOffsets <- Stream
                                             .eval(logOffsetsForTopics(topics, LoadAll, consumer))
                                             .flatMap(Stream.fromOption(_))
                             _          <- Stream.eval(info"log offsets: ${logOffsets.show}")
                             partition  <- loadAndRunPartitioned(logOffsets, consumer)(
                                             completeLoad(Resource.ExitCase.Succeeded)
                                           )
                           } yield partition.map(_.onFinalizeCase(onUnloadedExit))
                         }.onFinalizeCase(onUnloadedExit)
    } yield partition

  private def load[F[_] : Async : Logger, K, V](
      topics: NonEmptyList[String],
      strategy: LoadTopicStrategy,
      consumer: KafkaConsumer[F, K, V]
  ): Stream[F, ConsumerRecord[K, V]] =
    for {
      logOffsets <- Stream.eval(logOffsetsForTopics(topics, strategy, consumer)).flatMap(Stream.fromOption(_))
      _          <- Stream.eval(info"log offsets: ${logOffsets.show}")
      record     <- load(logOffsets, consumer)
    } yield record

  private def load[F[_] : Async : Logger, K, V](
      logOffsets: NonEmptyMap[TopicPartition, LogOffsets],
      consumer: KafkaConsumer[F, K, V]
  ): Stream[F, ConsumerRecord[K, V]] =
    for {
      _      <- Stream.eval(assignOffsets(logOffsets, consumer)(_.lowest))
      record <- consumer.records.map(_.record).through(filterBelowHighestOffset(logOffsets))
    } yield record

  private def loadPartitioned[F[_] : Async : Logger, K, V](
      logOffsets: NonEmptyMap[TopicPartition, LogOffsets],
      consumer: KafkaConsumer[F, K, V]
  ): Stream[F, (TopicPartition, Stream[F, ConsumerRecord[K, V]])] = {
    val (nonEmptyOffsets, emptyOffsets) = logOffsets.toSortedMap.partition((_, o) => o.highest > o.lowest)

    val logEmpty = Stream.eval {
      emptyOffsets.toList.traverse_((tp, o) => info"Not loading data from ${tp.show} at offset ${o.highest}")
    }

    def partitionStreams(toLoad: NonEmptyMap[TopicPartition, LogOffsets]) =
      for {
        _          <- Stream.eval(assignOffsets(toLoad, consumer)(_.lowest))
        partitions <- consumer.partitionsMapStream.take(1)
        partition  <- Stream.emits(partitions.toList.flatMap { (tp, stream) =>
                        toLoad(tp).map { offsets =>
                          tp -> stream
                            .map(_.record)
                            .through(filterBelowHighestOffset(NonEmptyMap.one(tp, offsets)))
                        }
                      })
      } yield partition

    logEmpty >> NonEmptyMap.fromMap(nonEmptyOffsets).fold(Stream.empty)(partitionStreams)
  }

  private def loadAndRunPartitioned[F[_] : Async : Logger, K, V](
      logOffsets: NonEmptyMap[TopicPartition, LogOffsets],
      consumer: KafkaConsumer[F, K, V]
  )(onAllLoaded: F[Unit]): Stream[F, (TopicPartition, Stream[F, ConsumerRecord[K, V]])] = {
    val toLoad = logOffsets.toSortedMap.filter((_, o) => o.highest > o.lowest)

    for {
      remaining  <- Stream.eval(Ref.of[F, Int](toLoad.size))
      _          <- Stream.eval(assignOffsets(logOffsets, consumer)(_.lowest))
      _          <- Stream.eval(Async[F].whenA(toLoad.isEmpty)(onAllLoaded))
      partitions <- consumer.partitionsMapStream.take(1)
      partition  <- Stream.emits(partitions.toList.map { (tp, stream) =>
                      val records = stream.map(_.record)

                      val markLoaded = (offset: Long) =>
                        info"Finished loading data from ${tp.show} at offset $offset" >>
                          remaining.updateAndGet(_ - 1).flatMap(n => Async[F].whenA(n == 0)(onAllLoaded))

                      tp -> toLoad.get(tp).fold(records)(o => loadThenRun(records, o.highest - 1)(markLoaded))
                    })
    } yield partition
  }

  private def loadThenRun[F[_], K, V](
      records: Stream[F, ConsumerRecord[K, V]],
      lastLoadedOffset: Long
  )(markLoaded: Long => F[Unit]): Stream[F, ConsumerRecord[K, V]] =
    records.pull
      .takeWhile(_.offset < lastLoadedOffset)
      .flatMap {
        case None       => Pull.done
        case Some(rest) =>
          rest.pull.uncons1.flatMap {
            case None                                                      => Pull.done
            case Some((record, tail)) if record.offset == lastLoadedOffset =>
              Pull.output1(record) >> Pull.eval(markLoaded(record.offset)) >> tail.pull.echo
            case Some((record, tail))                                      =>
              Pull.eval(markLoaded(record.offset - 1)) >> Pull.output1(record) >> tail.pull.echo
          }
      }
      .stream

  private def assignOffsets[F[_] : Monad : Logger, K, V](
      logOffsets: NonEmptyMap[TopicPartition, LogOffsets],
      consumer: KafkaConsumer[F, K, V]
  )(position: LogOffsets => Long): F[Unit] =
    for {
      _ <- debug"Assigning partitions: ${logOffsets.keys.mkString_(",")}"
      _ <- consumer.assign(logOffsets.keys)
      _ <- logOffsets.toNel.traverse_ { (tp, o) =>
             debug"Seeking to offset ${position(o)} for partition ${tp.show}" >> consumer.seek(tp, position(o))
           }
    } yield ()

  private def filterBelowHighestOffset[F[_] : Monad : Logger, K, V](
      logOffsets: NonEmptyMap[TopicPartition, LogOffsets]
  ): Pipe[F, ConsumerRecord[K, V], ConsumerRecord[K, V]] = stream => {
    val (nonEmptyOffsets, emptyOffsets) =
      logOffsets.toSortedMap.partition((_, o) => o.highest > o.lowest)

    val allHighestOffsets: HighestOffsetsWithRecord[K, V] =
      HighestOffsetsWithRecord[K, V](nonEmptyOffsets.map((p, o) => p -> (o.highest - 1)))

    Stream.eval {
      emptyOffsets.toList.traverse((tp, o) => info"Not loading data from ${tp.show} at offset ${o.highest}")
    } >>
      stream
        .scan(allHighestOffsets)(emitRecordRemovingConsumedPartition[K, V])
        .takeWhile(_.partitionOffsets.nonEmpty, takeFailure = true)
        .evalTapChunk(_.partitionLastOffset.traverse { last =>
          info"Finished loading data from ${last.topicPartition.show} at offset ${last.offset}"
        })
        .collect { case WithRecord(r) => r }
  }

  private def logOffsetsForTopics[F[_] : Async : Logger, K, V](
      topics: NonEmptyList[String],
      strategy: LoadTopicStrategy,
      consumer: KafkaConsumer[F, K, V]
  ): F[Option[NonEmptyMap[TopicPartition, LogOffsets]]] =
    for {
      topicPartitions             <- partitionsForTopics(topics, consumer)
      beginningOffsetPerPartition <- consumer.beginningOffsets(topicPartitions)
      endOffsets                  <- strategy match {
                                       case LoadAll       => consumer.endOffsets(topicPartitions)
                                       case LoadCommitted => earliestOffsets(consumer, beginningOffsetPerPartition)
                                     }
      logOffsets                   = beginningOffsetPerPartition.map { (partition, offset) =>
                                       partition -> LogOffsets(offset, endOffsets(partition))
                                     }
    } yield NonEmptyMap.fromMap(SortedMap.from(logOffsets))

  private def earliestOffsets[F[_] : Monad : Logger, K, V](
      consumer: KafkaConsumer[F, K, V],
      beginningOffsets: Map[TopicPartition, Long]
  ): F[Map[TopicPartition, Long]] =
    for {
      committed       <- offsetsAndMetadataFor(consumer, beginningOffsets)
      earliestOffsets <- beginningOffsets.toList.traverse { (tp, beginningOffset) =>
                           for {
                             maybeCommitted <- committed.get(tp).pure
                             earliest       <- maybeCommitted.fold(beginningOffset)(_.offset).pure
                             _              <- debug"Earliest offset for ${tp.show}: $earliest"
                           } yield tp -> earliest
                         }
    } yield earliestOffsets.toMap

  private def offsetsAndMetadataFor[F[_] : Monad : Logger, K, V](
      consumer: KafkaConsumer[F, K, V],
      beginningOffsets: Map[TopicPartition, Long]
  ): F[Map[TopicPartition, OffsetAndMetadata]] =
    for {
      committed       <- consumer.committed(beginningOffsets.keySet)
      filteredOffsets <- committed.toList.traverseFilter { (tp, offsetAndMetadata) =>
                           // It is possible to return null for an empty topic
                           Option(offsetAndMetadata) match {
                             case Some(offsetAndMetadata) =>
                               debug"${tp.show} had committed offset: ${offsetAndMetadata.show}"
                                 .as(Some(tp -> offsetAndMetadata))

                             case None =>
                               debug"${tp.show} had no committed offset"
                                 .as(none[(TopicPartition, OffsetAndMetadata)])
                           }
                         }
    } yield filteredOffsets.toMap

  private def partitionsForTopics[F[_] : Async, K, V](
      topics: NonEmptyList[String],
      consumer: KafkaConsumer[F, K, V]
  ): F[Set[TopicPartition]] =
    for {
      partitionInfo <- topics.toList.flatTraverse(consumer.partitionsFor)
    } yield partitionInfo.map(pi => TopicPartition(pi.topic, pi.partition)).toSet

  private def emitRecordRemovingConsumedPartition[K, V](
      t: HighestOffsetsWithRecord[K, V],
      r: ConsumerRecord[K, V]
  ): HighestOffsetsWithRecord[K, V] = {
    val partitionHighest: Option[Long] = t.partitionOffsets.get(TopicPartition(r.topic, r.partition))

    val reachedHighest: Option[TopicPartition] = for {
      offset  <- partitionHighest
      highest <- Option.when(r.offset >= offset)(TopicPartition(r.topic, r.partition))
    } yield highest

    val emittableRecord = partitionHighest.collect { case h if r.offset <= h => r }

    reachedHighest match {
      case Some(highest) =>
        HighestOffsetsWithRecord(
          partitionOffsets = t.partitionOffsets - highest,
          consumerRecord = emittableRecord,
          partitionLastOffset = PartitionLastOffset(highest, r.offset).some
        )

      case None =>
        HighestOffsetsWithRecord(
          partitionOffsets = t.partitionOffsets,
          consumerRecord = emittableRecord,
          partitionLastOffset = none[PartitionLastOffset]
        )
    }
  }

}
