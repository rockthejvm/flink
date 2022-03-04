package part3state

import generators.shopping._
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object KeyedState {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val shoppingCartEvents = env.addSource(
    new SingleShoppingCartEventsGenerator(
      sleepMillisBetweenEvents = 100, // ~ 10 events/s
      generateRemoved = true
    )
  )

  val eventsPerUser: KeyedStream[ShoppingCartEvent, String] = shoppingCartEvents.keyBy(_.userId)

  def demoValueState(): Unit = {
    /*
      How many events PER USER have been generated?
     */

    val numEventsPerUserNaive = eventsPerUser.process(
      new KeyedProcessFunction[String, ShoppingCartEvent, String] { // instantiated ONCE PER KEY
        //                     ^ key   ^ event            ^ result

        var nEventsForThisUser = 0

        override def processElement(
                                     value: ShoppingCartEvent,
                                     ctx: KeyedProcessFunction[String, ShoppingCartEvent, String]#Context,
                                     out: Collector[String]
                                   ): Unit = {
          nEventsForThisUser += 1
          out.collect(s"User ${value.userId} - $nEventsForThisUser")
        }
      }
    )

    /*
      Problems with local vars
      - they are local, so other nodes don't see them
      - if a node crashes, the var disappears
     */

    val numEventsPerUserStream = eventsPerUser.process(
      new KeyedProcessFunction[String, ShoppingCartEvent, String] {

        // can call .value to get current state
        // can call .update(newValue) to overwrite
        var stateCounter: ValueState[Long] = _ // a value state per key=userId

        override def open(parameters: Configuration): Unit = {
          // initialize all state
          stateCounter = getRuntimeContext // from RichFunction
            .getState(new ValueStateDescriptor[Long]("events-counter", classOf[Long]))
        }

        override def processElement(
                                     value: ShoppingCartEvent,
                                     ctx: KeyedProcessFunction[String, ShoppingCartEvent, String]#Context,
                                     out: Collector[String]
                                   ) = {
          val nEventsForThisUser = stateCounter.value()
          stateCounter.update(nEventsForThisUser + 1)
          out.collect(s"User ${value.userId} - ${nEventsForThisUser + 1}")
        }
      }
    )

    numEventsPerUserStream.print()
    env.execute()
  }

  // ListState
  def demoListState(): Unit = {
    // store all the events per user id
    val allEventsPerUserStream = eventsPerUser.process(
      new KeyedProcessFunction[String, ShoppingCartEvent, String] {
        // create state here
        /*
          Capabilities
          - add(value)
          - addAll(list)
          - update(new list) - overwriting
          - get()
         */
        var stateEventsForUser: ListState[ShoppingCartEvent] = _ // once per key
        // you need to be careful to keep the size of the list BOUNDED

        // initialization of state here
        override def open(parameters: Configuration): Unit =
          stateEventsForUser = getRuntimeContext.getListState(
            new ListStateDescriptor[ShoppingCartEvent]("shopping-cart-events", classOf[ShoppingCartEvent])
          )

        override def processElement(
                                     event: ShoppingCartEvent,
                                     ctx: KeyedProcessFunction[String, ShoppingCartEvent, String]#Context,
                                     out: Collector[String]
                                   ) = {
          stateEventsForUser.add(event)
          // import the Scala converters for collections
          // Scala 2.12
          import scala.collection.JavaConverters._ // implicit converters (extension methods)
          // Scala 2.13 & Scala 3
          // import scala.jdk.CollectionConverters._

          val currentEvents: Iterable[ShoppingCartEvent] = stateEventsForUser.get() // does not return a plain List, but a Java Iterable
            .asScala // convert to a Scala Iterable

          out.collect(s"User ${event.userId} - [${currentEvents.mkString(", ")}]")
        }
      }
    )

    allEventsPerUserStream.print()
    env.execute()
  }

  // MapState
  def demoMapState(): Unit = {
    // count how many events PER TYPE were ingested PER USER
    val streamOfCountsPerType = eventsPerUser.process(
      new KeyedProcessFunction[String, ShoppingCartEvent, String] {
        // Scala collection converters
        import scala.collection.JavaConverters._ // implicit converters (extension methods)

        // create the state
        var stateCountsPerEventType: MapState[String, Long] = _ // keep this bounded

        // initialize the state
        override def open(parameters: Configuration): Unit = {
          stateCountsPerEventType = getRuntimeContext.getMapState(
            new MapStateDescriptor[String, Long](
              "per-type-counter",
              classOf[String],
              classOf[Long]
            )
          )
        }

        override def processElement(
                                     event: ShoppingCartEvent,
                                     ctx: KeyedProcessFunction[String, ShoppingCartEvent, String]#Context,
                                     out: Collector[String]
                                   ) = {
          // fetch the type of the event
          val eventType = event.getClass.getSimpleName
          // updating the state
          if (stateCountsPerEventType.contains(eventType)) {
            val oldCount = stateCountsPerEventType.get(eventType)
            val newCount = oldCount + 1
            stateCountsPerEventType.put(eventType, newCount)
          } else {
            stateCountsPerEventType.put(eventType, 1)
          }

          // push some output
          out.collect(s"${ctx.getCurrentKey} - ${stateCountsPerEventType.entries().asScala.mkString(", ")}")
        }
      }
    )

    streamOfCountsPerType.print()
    env.execute()
  }

  // clear the state manually
  // clear the state at a regular interval

  def demoListStateWithClearance(): Unit = {
    val allEventsPerUserStream = eventsPerUser.process(
      new KeyedProcessFunction[String, ShoppingCartEvent, String] {
        import scala.collection.JavaConverters._ // implicit converters (extension methods)

        // if more than 10 elements, clear the list
        var stateEventsForUser: ListState[ShoppingCartEvent] = _

        // initialization of state here
        override def open(parameters: Configuration): Unit = {
          val descriptor = new ListStateDescriptor[ShoppingCartEvent]("shopping-cart-events", classOf[ShoppingCartEvent])
          // time to live = cleared if it's not modified after a certain time
          descriptor.enableTimeToLive(
            StateTtlConfig.newBuilder(Time.hours(1)) // clears the state after 1h
              .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // specify when the timer resets
              .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
              .build()
          )

          stateEventsForUser = getRuntimeContext.getListState(descriptor)

        }

        override def processElement(
                                     event: ShoppingCartEvent,
                                     ctx: KeyedProcessFunction[String, ShoppingCartEvent, String]#Context,
                                     out: Collector[String]
                                   ) = {
          stateEventsForUser.add(event)
          val currentEvents = stateEventsForUser.get().asScala.toList
          if (currentEvents.size > 10)
            stateEventsForUser.clear() // clearing is not done immediately

          out.collect(s"User ${event.userId} - [${currentEvents.mkString(", ")}]")
        }
      }
    )

    allEventsPerUserStream.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    demoListStateWithClearance()
  }
}
