package com.kafka.example

import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.util.{Failure, Success}

object KafkaProducerConsumer extends App {

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val dispatcher = system.dispatcher

  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer).withBootstrapServers("localhost:9092")

  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("group1")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val list = List(
    (1, "6"),
    (2, "7"),
    (3, "8"),
    (4, "9"),
    (5, "10")
  )

  /*val f1 = Source(list)
    .map { tuple =>
      val (id, msg) = tuple
      new ProducerRecord[String, String]("unique-id", id.toString, msg)
    }
    .runWith(Producer.plainSink(producerSettings))*/

  var map = scala.collection.mutable.Map[String, (Long, String)]()

  Consumer
    .plainSource(consumerSettings, Subscriptions.topics("unique-id"))
    .map { msg =>
      if (map.contains(msg.key())) {
        val (oldTimestamp, _) = map(msg.key)
        if (msg.timestamp() > oldTimestamp) {
          map.+=(msg.key() -> (msg.timestamp(), msg.value()))
        }

      } else {
        map.+=(msg.key() -> (msg.timestamp(), msg.value()))
      }
    }
    .toMat(Sink.ignore)(Keep.right)
    .run()
    .onComplete {
      case Success(_) =>
        map.foreach(println)

      case Failure(e) =>
        println(s"ERROR: $e")
    }

  //f1.flatMap(_ => f2)
}
