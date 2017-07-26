package com.kafka.example

import akka.actor.ActorSystem
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ConsumerSettings, ProducerMessage, ProducerSettings, Subscriptions}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

object KafkaProducerConsumer extends App {

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val dispatcher = system.dispatcher

  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer).withBootstrapServers("localhost:9092")

  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("my-jmx-consumer")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val r = scala.util.Random

  val f1 = Source(1 to 5000000)
    .map { id =>
      ProducerMessage.Message(
        new ProducerRecord[String, String]("test-jmx", id.toString, r.nextInt(100).toString),
        id
      )
    }
    .via(Producer.flow(producerSettings))
    .toMat(Sink.ignore)(Keep.right)
    .run()

  val f2 = Consumer
    .plainSource(consumerSettings, Subscriptions.topics("test-jmx"))
    .toMat(Sink.foreach(msg => println(s"${msg.key()} - ${msg.value()}")))(Keep.right)
    .run()

  f1.flatMap(_ => f2)
}
