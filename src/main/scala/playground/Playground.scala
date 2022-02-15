package playground

import org.apache.flink.streaming.api.scala._

/**
 * Probably the simplest Flink application imaginable.
 * Run this app when you first download the repository of the course.
 * If the app compiles, runs and prints something, then Flink is installed in your project and you're good to go.
 *
 * Feel free to modify this app as you see fit. Practice and play with the concepts you learn in the course.
 */
object Playground {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.fromElements(1 to 1000: _*)
    data.print()
    env.execute()
  }
}
