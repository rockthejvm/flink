package part3state

import generators.shopping._
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object BroadcastState {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val shoppingCartEvents = env.addSource(new SingleShoppingCartEventsGenerator(100))
  val eventsByUser = shoppingCartEvents.keyBy(_.userId)

  // issue a warning if quantity > threshold
  def purchaseWarnings(): Unit = {
    val threshold = 2

    val notificationsStream = eventsByUser
      .filter(_.isInstanceOf[AddToShoppingCartEvent])
      .filter(_.asInstanceOf[AddToShoppingCartEvent].quantity > threshold)
      .map(event => event match {
        case AddToShoppingCartEvent(userId, sku, quantity, _) =>
          s"User $userId attempting to purchase $quantity items of $sku when threshold is $threshold"
        case _ => ""
      })

    notificationsStream.print()
    env.execute()
  }

  // ... if the threshold CHANGES over time?
  // thresholds will be BROADCAST

  def changingThresholds(): Unit = {
    val thresholds: DataStream[Int] = env.addSource(new SourceFunction[Int] {
      override def run(ctx: SourceFunction.SourceContext[Int]) =
        List(2,0,4,5,6,3).foreach { newThreshold =>
          Thread.sleep(1000)
          ctx.collect(newThreshold)
        }

      override def cancel() = ()
    })

    // broadcast state is ALWAYS a map
    val broadcastStateDescriptor = new MapStateDescriptor[String, Int]("thresholds", classOf[String], classOf[Int])
    val broadcastThresholds: BroadcastStream[Int] = thresholds.broadcast(broadcastStateDescriptor)

    val notificationsStream = eventsByUser
      .connect(broadcastThresholds)
      .process(new KeyedBroadcastProcessFunction[String, ShoppingCartEvent, Int,      String] {
        //                                       ^ key   ^ first event      ^ broadcast  ^ output
        val thresholdsDescriptor = new MapStateDescriptor[String, Int]("thresholds", classOf[String], classOf[Int])

        override def processBroadcastElement(
                                              newThreshold: Int,
                                              ctx: KeyedBroadcastProcessFunction[String, ShoppingCartEvent, Int, String]#Context,
                                              out: Collector[String]
                                            ) = {
          println(s"Threshold about to be changed -- $newThreshold")
          // fetch the broadcast state = distributed variable
          val stateThresholds = ctx.getBroadcastState(thresholdsDescriptor)
          // update the state
          stateThresholds.put("quantity-threshold", newThreshold)
        }


        override def processElement(
                                     event: ShoppingCartEvent,
                                     ctx: KeyedBroadcastProcessFunction[String, ShoppingCartEvent, Int, String]#ReadOnlyContext,
                                     out: Collector[String]
                                   ) = {
          event match {
            case AddToShoppingCartEvent(userId, sku, quantity, time) =>
              val currentThreshold: Int = ctx.getBroadcastState(thresholdsDescriptor).get("quantity-threshold")
              if (quantity > currentThreshold)
                out.collect(s"User $userId attempting to purchase $quantity items of $sku when threshold is $currentThreshold")
            case _ =>
          }
        }
      })


    notificationsStream.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    changingThresholds()
  }
}
