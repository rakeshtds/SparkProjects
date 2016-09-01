package com.pb.hackathon.sparkstreaming

import com.typesafe.config.{ConfigFactory, Config}
/**
 * Created by shashidhar on 7/3/16.
 */
object Configuration {

  private lazy val config:Config = readConfig()


      def sparkConfig = config.getConfig("streaming-app.spark-config")

      def streamingConfig =  config.getConfig("streaming-app.streaming-config")

      private def readConfig(): Config ={
        ConfigFactory.load("application")
      
      }

}
