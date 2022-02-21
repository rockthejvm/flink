package part2datastreams

import generators.gaming._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessAllWindowFunction, ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, GlobalWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector

import java.time.Instant
import scala.concurrent.duration._

object WindowFunctions {

  // use-case: stream of events for a gaming session

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  implicit val serverStartTime: Instant =
    Instant.parse("2022-02-02T00:00:00.000Z")

  val events: List[ServerEvent] = List(
    bob.register(2.seconds), // player "Bob" registered 2s after the server started
    bob.online(2.seconds),
    sam.register(3.seconds),
    sam.online(4.seconds),
    rob.register(4.seconds),
    alice.register(4.seconds),
    mary.register(6.seconds),
    mary.online(6.seconds),
    carl.register(8.seconds),
    rob.online(10.seconds),
    alice.online(10.seconds),
    carl.online(10.seconds)
  )

  val eventStream: DataStream[ServerEvent] = env
    .fromCollection(events)
    .assignTimestampsAndWatermarks( // extract timestamps for events (event time) + watermarks
      WatermarkStrategy
        .forBoundedOutOfOrderness(java.time.Duration.ofMillis(500)) // once you get an event with time T, you will NOT accept further events with time < T - 500
        .withTimestampAssigner(new SerializableTimestampAssigner[ServerEvent] {
          override def extractTimestamp(element: ServerEvent, recordTimestamp: Long) =
            element.eventTime.toEpochMilli
        })
    )

  // how many players were registered every 3 seconds?
  // [0...3s] [3s...6s] [6s...9s]
  val threeSecondsTumblingWindow = eventStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))
  /*
    |----0----|----1----|--------2--------|--------3--------|---------4---------|---5---|--------6--------|---7---|--------8--------|--9--|------10-------|------11------|
    |         |         | bob registered  | sam registered  | sam online        |       | mary registered |       | carl registered |     | rob online    |              |
    |         |         | bob online      |                 | rob registered    |       | mary online     |       |                 |     | alice online  |              |
    |         |         |                 |                 | alice registered  |       |                 |       |                 |     | carl online   |              |
    ^|------------ window one ----------- + -------------- window two ----------------- + ------------- window three -------------- + ----------- window four ----------|^
    |                                     |                                             |                                           |                                    |
    |            1 registrations          |               3 registrations               |              2 registration               |            0 registrations         |
    |     1643760000000 - 1643760003000   |        1643760005000 - 1643760006000        |       1643760006000 - 1643760009000       |    1643760009000 - 1643760012000   |
 */

  // count by windowAll
  class CountByWindowAll extends AllWindowFunction[ServerEvent, String, TimeWindow] {
  //                                                ^ input      ^ output  ^ window type
    override def apply(window: TimeWindow, input: Iterable[ServerEvent], out: Collector[String]): Unit = {
      val registrationEventCount = input.count(event => event.isInstanceOf[PlayerRegistered])
      out.collect(s"Window [${window.getStart} - ${window.getEnd}] $registrationEventCount")
    }
  }

  def demoCountByWindow(): Unit = {
    val registrationsPerThreeSeconds: DataStream[String] = threeSecondsTumblingWindow.apply(new CountByWindowAll)
    registrationsPerThreeSeconds.print()
    env.execute()
  }

  // alternative: process window function which offers a much richer API (lower-level)
  class CountByWindowAllV2 extends ProcessAllWindowFunction[ServerEvent, String, TimeWindow] {
    override def process(context: Context, elements: Iterable[ServerEvent], out: Collector[String]): Unit = {
      val window = context.window
      val registrationEventCount = elements.count(event => event.isInstanceOf[PlayerRegistered])
      out.collect(s"Window [${window.getStart} - ${window.getEnd}] $registrationEventCount")
    }
  }

  def demoCountByWindow_v2(): Unit = {
    val registrationsPerThreeSeconds: DataStream[String] = threeSecondsTumblingWindow.process(new CountByWindowAllV2)
    registrationsPerThreeSeconds.print()
    env.execute()
  }

  // alternative 2: aggregate function
  class CountByWindowV3 extends AggregateFunction[ServerEvent, Long, Long] {
    //                                             ^ input     ^ acc  ^ output

    // start counting from 0
    override def createAccumulator(): Long = 0L

    // every element increases accumulator by 1
    override def add(value: ServerEvent, accumulator: Long) =
      if (value.isInstanceOf[PlayerRegistered]) accumulator + 1
      else accumulator

    // push a final output out of the final accumulator
    override def getResult(accumulator: Long) = accumulator

    // accum1 + accum2 = a bigger accumulator
    override def merge(a: Long, b: Long) = a + b
  }

  def demoCountByWindow_v3(): Unit = {
    val registrationsPerThreeSeconds: DataStream[Long] = threeSecondsTumblingWindow.aggregate(new CountByWindowV3)
    registrationsPerThreeSeconds.print()
    env.execute()
  }

  /**
   * Keyed streams and window functions
   */
  // each element will be assigned to a "mini-stream" for its own key
  val streamByType: KeyedStream[ServerEvent, String] = eventStream.keyBy(e => e.getClass.getSimpleName)

  // for every key, we'll have a separate window allocation
  val threeSecondsTumblingWindowByType = streamByType.window(TumblingEventTimeWindows.of(Time.seconds(3)))

  /*
    === Registration Events Stream ===
    |----0----|----1----|--------2--------|--------3--------|---------4---------|---5---|--------6--------|---7---|--------8--------|--9--|------10-------|------11------|
    |         |         | bob registered  | sam registered  | rob registered    |       | mary registered |       | carl registered |     |               |              |
    |         |         |                 |                 | alice registered  |       |                 |       |                 |     |               |              |
    ^|------------ window one ----------- + -------------- window two ----------------- + ------------- window three -------------- + ----------- window four ----------|^
    |            1 registration           |               3 registrations               |              2 registrations              |            0 registrations         |
    |     1643760000000 - 1643760003000   |        1643760003000 - 1643760006000        |       1643760006000 - 1643760009000       |    1643760009000 - 1643760012000   |

    === Online Events Stream ===
    |----0----|----1----|--------2--------|--------3--------|---------4---------|---5---|--------6--------|---7---|--------8--------|--9--|------10-------|------11------|
    |         |         | bob online      |                 | sam online        |       | mary online     |       |                 |     | rob online    | carl online  |
    |         |         |                 |                 |                   |       |                 |       |                 |     | alice online  |              |
    ^|------------ window one ----------- + -------------- window two ----------------- + ------------- window three -------------- + ----------- window four ----------|^
    |            1 online                 |               1 online                      |              1 online                     |            3 online                |
    |     1643760000000 - 1643760003000   |        1643760005000 - 1643760006000        |       1643760006000 - 1643760009000       |    1643760009000 - 1643760012000   |
  */

  class CountByWindow extends WindowFunction[ServerEvent, String, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[ServerEvent], out: Collector[String]): Unit =
      out.collect(s"$key: $window, ${input.size}")
  }

  def demoCountByTypeByWindow(): Unit = {
    val finalStream = threeSecondsTumblingWindowByType.apply(new CountByWindow)
    finalStream.print()
    env.execute()
  }

  // alternative: process function for windows
  class CountByWindowV2 extends ProcessWindowFunction[ServerEvent, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[ServerEvent], out: Collector[String]): Unit =
      out.collect(s"$key: ${context.window}, ${elements.size}")
  }

  def demoCountByTypeByWindow_v2(): Unit = {
    val finalStream = threeSecondsTumblingWindowByType.process(new CountByWindowV2)
    finalStream.print()
    env.execute()
  }

  // one task processes all the data for a particular key

  /**
   * Sliding Windows
   */

  // how many players were registered every 3 seconds, UPDATED EVERY 1s?
  // [0s...3s] [1s...4s] [2s...5s] ...

  /*
  |----0----|----1----|--------2--------|--------3--------|---------4---------|---5---|--------6--------|---7---|--------8--------|--9--|------10-------|------11------|
  |         |         | bob registered  | sam registered  | sam online        |       | mary registered |       | carl registered |     | rob online    | carl online  |
  |         |         | bob online      |                 | rob registered    |       | mary online     |       |                 |     | alice online  |              |
  |         |         |                 |                 | alice registered  |       |                 |       |                 |     |               |              |
  ^|------------ window one ----------- +
                1 registration

            + ---------------- window two --------------- +
                                  2 registrations

                       + ------------------- window three ------------------- +
                                           4 registrations

                                         + ---------------- window four --------------- +
                                                          3 registrations

                                                          + ---------------- window five -------------- +
                                                                           3 registrations

                                                                               + ---------- window six -------- +
                                                                                           1 registration

                                                                                        + ------------ window seven ----------- +
                                                                                                    2 registrations

                                                                                                         + ------- window eight------- +
                                                                                                                  1 registration

                                                                                                                + ----------- window nine ----------- +
                                                                                                                        1 registration

                                                                                                                                   + ---------- window ten --------- +
                                                                                                                                              0 registrations
   */

  def demoSlidingAllWindows(): Unit = {
    val windowSize: Time = Time.seconds(3)
    val slidingTime: Time = Time.seconds(1)

    val slidingWindowsAll = eventStream.windowAll(SlidingEventTimeWindows.of(windowSize, slidingTime))

    // process the windowed stream with similar window functions
    val registrationCountByWindow = slidingWindowsAll.apply(new CountByWindowAll)

    // similar to the other example
    registrationCountByWindow.print()
    env.execute()
  }

  /**
   * Session windows = groups of events with NO MORE THAN a certain time gap in between them
   */
  // how many registration events do we have NO MORE THAN 1s apart?
  /*
    |----0----|----1----|--------2--------|--------3--------|---------4---------|---5---|--------6--------|---7---|--------8--------|--9--|------10-------|------11------|
    |         |         | bob registered  | sam registered  | sam online        |       | mary registered |       | carl registered |     | rob online    |              |
    |         |         | bob online      |                 | rob registered    |       | mary online     |       |                 |     | alice online  |              |
    |         |         |                 |                 | alice registered  |       |                 |       |                 |     | carl online   |              |

    after filtering:

    +---------+---------+-----------------+-----------------+-------------------+-------+-----------------+-------+-----------------+-----+---------------+--------------+
    |         |         | bob registered  | sam registered  | rob registered    |       | mary registered |       | carl registered |     |     N/A       |              |
    |         |         |                 |                 | alice registered  |       |                 |       |                 |     |               |              |
                        ^ ----------------- window 1 -------------------------- ^       ^ -- window 2 --- ^       ^ -- window 3 --- ^     ^ -- window 4 - ^
  */

  def demoSessionWindows(): Unit = {
    val groupBySessionWindows = eventStream.windowAll(EventTimeSessionWindows.withGap(Time.seconds(1)))

    // operate any kind of window function
    val countBySessionWindows = groupBySessionWindows.apply(new CountByWindowAll)

    // same things as before
    countBySessionWindows.print()
    env.execute()
  }

  /**
   * Global window
   */
  // how many registration events do we have every 10 events

  class CountByGlobalWindowAll extends AllWindowFunction[ServerEvent, String, GlobalWindow] {
    //                                                    ^ input      ^ output  ^ window type
    override def apply(window: GlobalWindow, input: Iterable[ServerEvent], out: Collector[String]): Unit = {
      val registrationEventCount = input.count(event => event.isInstanceOf[PlayerRegistered])
      out.collect(s"Window [$window] $registrationEventCount")
    }
  }

  def demoGlobalWindow(): Unit = {
    val globalWindowEvents = eventStream
      .windowAll(GlobalWindows.create())
      .trigger(CountTrigger.of[GlobalWindow](10))
      .apply(new CountByGlobalWindowAll)

    globalWindowEvents.print()
    env.execute()
  }

  /**
   * Exercise: what was the time window (continuous 2s) when we had THE MOST registration events?
   * - what kind of window functions should we use? ALL WINDOW FUNCTION
   * - what kind of windows should we use? SLIDING WINDOWS
   */
  class KeepWindowAndCountFunction extends AllWindowFunction[ServerEvent, (TimeWindow, Long), TimeWindow] {
    override def apply(window: TimeWindow, input: Iterable[ServerEvent], out: Collector[(TimeWindow, Long)]): Unit =
      out.collect((window, input.size))
  }

  def windowFunctionsExercise(): Unit = {
    val slidingWindows: DataStream[(TimeWindow, Long)] = eventStream
      .filter(_.isInstanceOf[PlayerRegistered])
      .windowAll(SlidingEventTimeWindows.of(Time.seconds(2), Time.seconds(1)))
      .apply(new KeepWindowAndCountFunction)

    val localWindows: List[(TimeWindow, Long)] = slidingWindows.executeAndCollect().toList
    val bestWindow: (TimeWindow, Long) = localWindows.maxBy(_._2)
    println(s"The best window is ${bestWindow._1} with ${bestWindow._2} registration events.")
  }

  def main(args: Array[String]): Unit = {
    windowFunctionsExercise()
  }
}
