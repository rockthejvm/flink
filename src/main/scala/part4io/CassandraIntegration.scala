package part4io

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.cassandra.CassandraSink

object CassandraIntegration {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  case class Person(name: String, age: Int)

  // write data to Cassandra
  def demoWriteDataToCassandra(): Unit = {
    val people = env.fromElements(
      Person("Daniel", 99),
      Person("Alice", 12),
      Person("Julie", 14),
      Person("Mom", 54),
    )

    // we can only write TUPLES to Cassandra
    val personTuples: DataStream[(String, Int)] = people.map(p => (p.name, p.age))

    // write the data
    CassandraSink.addSink(personTuples) // builder pattern
      .setQuery("insert into rtjvm.people(name, age) values (?, ?)")
      .setHost("localhost")
      .build()

    env.execute()
  }


  def main(args: Array[String]): Unit = {
    demoWriteDataToCassandra()
  }
}
