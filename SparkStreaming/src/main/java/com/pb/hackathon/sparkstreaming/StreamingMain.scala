package com.pb.hackathon.sparkstreaming

import java.text.SimpleDateFormat

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ Row, SQLContext }
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import java.sql.{ PreparedStatement, Connection, DriverManager }
import java.util.concurrent.atomic.AtomicInteger
import org.apache.spark.streaming.{ Seconds, StreamingContext }

import dispatch._, Defaults._
import scala.util.{ Success, Failure }

object StreamingMain {

  val sparkConfig = Configuration.sparkConfig

  val streamingConfig = Configuration.streamingConfig

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def calculateStateStats(parsedDstream: DStream[OrderRecord], windowInterval: Int, slideInterval: Int) = {
    val stateAggregation = parsedDstream.map(eachRec => ((dateFormat.format(eachRec.dateTime), eachRec.state), 1))

    stateAggregation.map({
      case ((date, state), count) => StateWiseStats(date, state, count)
    }).saveToCassandra("sparkstreaming", "statestats", SomeColumns("date", "state", "count"))
  }

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()

    val appname = "SparkStreamingHackathon"
    val batchTime = 5.toInt
    val windowTime = 10.toInt
    val slideTime = 10.toInt
    val topics = "hackathon"
    val brokers = "localhost:9092"
    //  val cassandraHost = "152.144.214.174"

    sparkConf.setMaster("local[2]")

    sparkConf.setAppName(appname)
    sparkConf.setExecutorEnv("spark.driver.memory", "2g")

    //   sparkConf.set("spark.cassandra.connection.host", cassandraHost)
    //   sparkConf.set("spark.cassandra.connection.keep_alive_ms", "50000")

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    val ssc = new StreamingContext(sparkConf, Seconds(batchTime))

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    val data = messages.map(_._2)
    data.foreachRDD { rdd =>

      {
        val repartitionedRDD = rdd.repartition(3)
        repartitionedRDD.foreachPartition(func)
        //embedded function  
        def func(records: Iterator[String]) {
          var conn: Connection = null
          var stmt: PreparedStatement = null
          try {
            val dburl = "jdbc:mysql://152.144.214.59/hackathon";
            val user = "hackathon";
            val password = "hackathon"
            Class.forName("com.mysql.jdbc.Driver").newInstance()
            conn = DriverManager.getConnection(dburl, user, password)

            val parsedDstream = records.map(SchemaParser.parse(_)).filter(_ != None).map(_.get)

            parsedDstream.foreach(rec => {

              val svc = url("http://api.bigdatadev.pitneycloud.com:80/fusion/geocode/"+rec.street1.replace(" ", "%20")+"%2C"+"%2C"+rec.city.replace(" ", "%20")+"%2C"+rec.state.replace(" ", "%20")+"%2C"+rec.postalcode.replace(" ", "%20")+"%2C"+rec.country.replace(" ", "%20")).as_!("pbuser", "hackathon")
              val response: Future[String] = Http.configure(_ setFollowRedirects true)(svc OK as.String)

              response onComplete {
                case Success(content) => {
                  println("Successful response" + content)
                }
                case Failure(t) => {
                  println("An error has occured: " + t.getMessage)
                }
              }

              val sql = "insert into statestats(tdate,state,count) values (?,?,?)";
              stmt = conn.prepareStatement(sql);
              stmt.setString(1, rec.dateTime.toString())
              stmt.setString(2, rec.state)
              stmt.setInt(3, 1)
              stmt.executeUpdate();
            })
          } catch {
            case e: Exception => e.printStackTrace()
          } finally {
            if (stmt != null) {
              stmt.close()
            }
            if (conn != null) {
              conn.close()
            }
          }
        }

      }
    }

    //val parsedDstream = data.map(SchemaParser.parse(_)).filter(_ != None).map(_.get)

    // calculateStateStats(parsedDstream, windowTime, slideTime)

    ssc.start()
    ssc.awaitTermination()

  }

}

