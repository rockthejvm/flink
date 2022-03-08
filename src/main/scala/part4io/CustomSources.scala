package part4io

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

import java.io.PrintStream
import java.net.{ServerSocket, Socket}
import java.util.Scanner
import scala.util.Random

object CustomSources {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // source of numbers, randomly generated
  class RandomNumberGeneratorSource(minEventsPerSeconds: Double)
  extends RichParallelSourceFunction[Long] {

    // create local fields/methods
    val maxSleepTime = (1000 / minEventsPerSeconds).toLong
    var isRunning: Boolean = true

    // called ONCE, when the function is instantiated
    // SourceFunction/RichSourceFunction runs on a (single) dedicated thread

    // Parallel function is called ONCE PER THREAD, each instance has its own thread
    override def run(ctx: SourceFunction.SourceContext[Long]): Unit =
      while (isRunning) {
        val sleepTime = Math.abs(Random.nextLong() % maxSleepTime)
        val nextNumber = Random.nextLong()
        Thread.sleep(sleepTime)

        // push something to the output
        ctx.collect(nextNumber)
      }

    // called at application shutdown
    // contract: the run method should stop immediately
    override def cancel(): Unit =
      isRunning = false

    // capability of lifecycle methods - initialize state ...
    override def open(parameters: Configuration): Unit =
      println(s"[${Thread.currentThread().getName}] starting source function")
    override def close(): Unit =
      println(s"[${Thread.currentThread().getName}] closing source function")

    // can hold state - ValueState, ListState, MapState
  }

  def demoSourceFunction(): Unit = {
    val numbersStream: DataStream[Long] = env.addSource(new RandomNumberGeneratorSource(10)).setParallelism(10)
    numbersStream.print()
    env.execute()
  }

  /**
   * Create a source function that reads data from a socket.
   */

  class SocketStringSource(host: String, port: Int) extends RichSourceFunction[String] {
    // whenever you manage a resource, use a RichSourceFunction
    var socket: Socket = _
    var isRunning = true

    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
      val scanner = new Scanner(socket.getInputStream)
      while (isRunning && scanner.hasNextLine) {
        ctx.collect(scanner.nextLine())
      }
    }

    override def cancel(): Unit =
      isRunning = false

    override def open(parameters: Configuration): Unit =
      socket = new Socket(host, port)

    override def close(): Unit =
      socket.close()
  }

  def demoSocketSource(): Unit = {
    val socketStringStream = env.addSource(new SocketStringSource("localhost", 12345))
    socketStringStream.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    demoSocketSource()
  }
}

/*
  - start DataSender
  - start Flink
  - DataSender -> Flink
 */

object DataSender {
  def main(args: Array[String]): Unit = {
    val serverSocket = new ServerSocket(12345)
    println("Waiting for Flink to connect...")

    val socket = serverSocket.accept()
    println("Flink connected. Sending data...")

    val printer = new PrintStream(socket.getOutputStream)
    printer.println("Hello from the other side...")
    Thread.sleep(3000)
    printer.println("Almost ready...")
    Thread.sleep(500)
    (1 to 10).foreach { i =>
      Thread.sleep(200)
      printer.println(s"Number $i")
    }

    println("Data sending completed.")
    serverSocket.close()
  }
}