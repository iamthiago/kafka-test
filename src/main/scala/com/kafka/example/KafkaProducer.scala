package com.kafka.example

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Producer
import akka.kafka.{ProducerMessage, ProducerSettings}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object KafkaProducer extends App {

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val dispatcher = system.dispatcher

  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers("localhost:9092")
    .withProperty(ProducerConfig.ACKS_CONFIG, "1")
    .withProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")

  val r = scala.util.Random

  Source(1 to 5000000)
    .map { id =>
      ProducerMessage.Message(
        new ProducerRecord[String, String]("test", id.toString, r.nextInt(100).toString),
        id
      )
    }
    .via(Producer.flow(producerSettings))
    .map { result =>
      val record = result.message.record
      println(s"${record.topic}/${record.partition} ${result.offset}: ${record.value}" + s"(${result.message.passThrough})")
      result
    }
    .runWith(Sink.ignore)

  /*for (_ <- 1 to 100) {
    Source(List(1))
      .map { id =>
        ProducerMessage.Message(
          new ProducerRecord[String, String]("test", id.toString, null),
          id
        )
      }
      .via(Producer.flow(producerSettings))
      .map { result =>
        val record = result.message.record
        println(s"${record.topic}/${record.partition} ${result.offset}: ${record.value}" + s"(${result.message.passThrough})")
        result
      }
      .runWith(Sink.ignore)
  }*/


}
