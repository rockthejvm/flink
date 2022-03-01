package part2datastreams

import generators.shopping._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, PurgingTrigger}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Triggers {

  // Triggers -> WHEN a window function is executed

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  def demoCountTrigger(): Unit = {
    val shoppingCartEvents: DataStream[String] = env
      .addSource(new ShoppingCartEventsGenerator(500, 2)) // 2 events/second
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))) // 10 events/window
      .trigger(CountTrigger.of[TimeWindow](5)) // the window function runs every 5 elements
      .process(new CountByWindowAll) // runs twice for the same window

    shoppingCartEvents.print()
    env.execute()
  }
  /*
    12> Window [1646129900000 - 1646129905000] 2
    1> Window [1646129905000 - 1646129910000] 10
    2> Window [1646129910000 - 1646129915000] 10
    3> Window [1646129915000 - 1646129920000] 10
    4> Window [1646129920000 - 1646129925000] 10

    with trigger
    6> Window [1646130165000 - 1646130170000] 5 <- trigger running on the window 65000-70000 for the first time
    7> Window [1646130165000 - 1646130170000] 10 <- second trigger FOR THE SAME WINDOW
    8> Window [1646130170000 - 1646130175000] 5
    9> Window [1646130170000 - 1646130175000] 10
    10> Window [1646130175000 - 1646130180000] 5
    11> Window [1646130175000 - 1646130180000] 10
   */

  // purging trigger - clear the window when it fires

  def demoPurgingTrigger(): Unit = {
    val shoppingCartEvents: DataStream[String] = env
      .addSource(new ShoppingCartEventsGenerator(500, 2)) // 2 events/second
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))) // 10 events/window
      .trigger(PurgingTrigger.of(CountTrigger.of[TimeWindow](5))) // the window function runs every 5 elements, THEN CLEARS THE WINDOW
      .process(new CountByWindowAll) // runs twice for the same window

    shoppingCartEvents.print()
    env.execute()
  }

  /*
    with purging trigger

    12> Window [1646134290000 - 1646134295000] 5
    1> Window [1646134295000 - 1646134300000] 5
    2> Window [1646134295000 - 1646134300000] 5
    3> Window [1646134300000 - 1646134305000] 5
    4> Window [1646134300000 - 1646134305000] 5
    5> Window [1646134305000 - 1646134310000] 5
    6> Window [1646134305000 - 1646134310000] 5
   */

  /*
    Other triggers:
    - EventTimeTrigger - happens by default when the watermark is > window end time (automatic for event time windows)
    - ProcessingTimeTrigger - fires when the current system time > window end time (automatic for processing time windows)
    - custom triggers - powerful APIs for custom firing behavior
   */

  def main(args: Array[String]): Unit = {
    demoPurgingTrigger()
  }

}

// copied from Time Based Transformations
class CountByWindowAll extends ProcessAllWindowFunction[ShoppingCartEvent, String, TimeWindow] {
  override def process(context: Context, elements: Iterable[ShoppingCartEvent], out: Collector[String]): Unit = {
    val window = context.window
    out.collect(s"Window [${window.getStart} - ${window.getEnd}] ${elements.size}")
  }
}
