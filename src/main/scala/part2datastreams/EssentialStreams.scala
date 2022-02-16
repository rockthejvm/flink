package part2datastreams

import org.apache.flink.streaming.api.scala._

object EssentialStreams {

  def applicationTemplate(): Unit = {
    // execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // in between, add any sort of computations
    import org.apache.flink.streaming.api.scala._ // import TypeInformation for the data of your DataStreams
    val simpleNumberStream: DataStream[Int] = env.fromElements(1,2,3,4)

    // perform some actions
    simpleNumberStream.print()

    // at the end
    env.execute() // trigger all the computations that were DESCRIBED earlier
  }

  // transformations
  def demoTransformations(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val numbers: DataStream[Int] = env.fromElements(1,2,3,4,5)

    // checking parallelism
    println(s"Current parallelism: ${env.getParallelism}")
    // set different parallelism
    env.setParallelism(2)
    println(s"New parallelism: ${env.getParallelism}")

    // map
    val doubledNumbers: DataStream[Int] = numbers.map(_ * 2)

    // flatMap
    val expandedNumbers: DataStream[Int] = numbers.flatMap(n => List(n, n + 1))

    // filter
    val filteredNumbers: DataStream[Int] = numbers
      .filter(_ % 2 == 0)
      /* you can set parallelism here*/.setParallelism(4)

    val finalData = expandedNumbers.writeAsText("output/expandedStream") // directory with 12 files
    // set parallelism in the sink
    finalData.setParallelism(3)

    env.execute()
  }

  def main(args: Array[String]): Unit = {
    demoTransformations()
  }
}
