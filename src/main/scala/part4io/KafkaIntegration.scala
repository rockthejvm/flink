package part4io

import org.apache.commons.lang3.CharSet
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema, SimpleStringSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

object KafkaIntegration {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // read simple data (strings) from a Kafka topic
  def readStrings(): Unit = {
    val kafkaSource = KafkaSource.builder[String]()
      .setBootstrapServers("localhost:9092")
      .setTopics("events")
      .setGroupId("events-group")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val kafkaStrings: DataStream[String] = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")

    // use the DS
    kafkaStrings.print()
    env.execute()
  }

  // read custom data
  case class Person(name: String, age: Int)
  class PersonDeserializer extends DeserializationSchema[Person] {
    override def deserialize(message: Array[Byte]): Person = {
      // format: name,age
      val string = new String(message)
      val tokens = string.split(",")
      val name = tokens(0)
      val age = tokens(1)
      Person(name, age.toInt)
    }

    override def isEndOfStream(nextElement: Person): Boolean = false

    override def getProducedType: TypeInformation[Person] = implicitly[TypeInformation[Person]]
  }

  def readCustomData(): Unit = {
    val kafkaSource = KafkaSource.builder[Person]()
      .setBootstrapServers("localhost:9092")
      .setTopics("people")
      .setGroupId("people-group")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new PersonDeserializer)
      .build()

    val kafkaPeople: DataStream[Person] = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")

    // use the DS
    kafkaPeople.print()
    env.execute()
  }

  // write custom data
  // need serializer
  class PersonSerializer extends SerializationSchema[Person] {
    override def serialize(person: Person): Array[Byte] =
      s"${person.name},${person.age}".getBytes("UTF-8")
  }

  def writeCustomData(): Unit = {
    val kafkaSink = new FlinkKafkaProducer[Person](
      "localhost:9092", // bootstrap server
      "people", // topic
      new PersonSerializer
    )

    val peopleStream = env.fromElements(
      Person("Alice", 10),
      Person("Bob", 11),
      Person("Charlie", 12),
    )

    peopleStream.addSink(kafkaSink)
    peopleStream.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    writeCustomData()
  }
}
