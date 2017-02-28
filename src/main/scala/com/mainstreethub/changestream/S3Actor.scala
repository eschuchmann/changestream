package com.mainstreethub.changestream

import akka.actor.Actor
import changestream.events.MutationWithInfo
import com.typesafe.config.{Config, ConfigFactory}

class S3Actor (config: Config = ConfigFactory.load().getConfig("changestream")) extends Actor {
  val bucket = config.getString("aws.s3.bucket")
  val prefix = config.getString("aws.s3.prefix")

  def receive = {
    case MutationWithInfo(mutation, _, _, Some(message: String)) =>
      println(message)
      sender() ! akka.actor.Status.Success(message)
    case _ =>
      sender() ! akka.actor.Status.Failure(new Exception("Received invalid message"))
  }
}
