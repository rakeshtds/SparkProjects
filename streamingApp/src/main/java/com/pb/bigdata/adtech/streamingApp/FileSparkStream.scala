package com.pb.bigdata.adtech.streamingApp

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{ Time, Seconds, StreamingContext }
import org.apache.spark.util.IntParam
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types.MetadataBuilder
import java.util.Calendar
import org.apache.spark.sql.types.StringType
import java.sql.Timestamp
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField

object FileSparkStream {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("FileSparkStream")
    val sc = new SparkContext(conf)
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val SQLContext = new org.apache.spark.sql.SQLContext(sc)
    hiveContext.sql("""create temporary function geocode as 'com.pb.fusion.hive.geocoding.AddressGeocode'""");
    hiveContext.sql("""create temporary function udaf as 'com.pb.fusion.hive.pointinpolygon.FindNearestUDAF'""");
    hiveContext.sql("""create temporary function geohash as 'com.pb.fusion.hive.pointinpolygon.GetGeoHash'""");

    val mld_geohash = hiveContext.sql("select * from ad_tech.mld_geohash");
   // mld_geohash.cache()
    mld_geohash.registerTempTable("mld_geohash")
    hiveContext.cacheTable("mld_geohash")

    val demographics = hiveContext.sql("select * from ad_tech.demographics")
    demographics.registerTempTable("demographics")
    hiveContext.cacheTable("demographics")

    val ssc = new StreamingContext(sc, Seconds(10))
    val inputData = ssc.textFileStream("/domain/li/data/address/")

    inputData.foreachRDD((rdd: RDD[String], time: Time) => {
      // Get the singleton instance of SQLContext
      val df = hiveContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("/domain/li/data/address/usaddress.csv")
      df.registerTempTable("newtable")
      val df2 = hiveContext.sql("select *, geocode(addressline1,addressline2,city,state,zip,'USA') as geocode from newtable")
      val flatDF = hiveContext.read.json(df2.map { case Row(c0: Int, c1: String, c2: String, c3: String, c4: String, c5: Int, c6: String, c7: String) => s"""{"ID": "$c0","addline1": "$c1","addline2": "$c2","city": "$c3","state": "$c4","zip": "$c5","country": "$c6", "geocode": $c7}""" })

      val DF3 = flatDF.select(flatDF("ID"), flatDF("city"), flatDF("state"), flatDF("geocode.LAT") as "Latitude", flatDF("geocode.LON") as "Longitude", flatDF("geocode.GeoScore") as "GeoScore")
      DF3.registerTempTable("geocode_output")
      hiveContext.cacheTable("geocode_output")
    })

    ssc.start()
    ssc.awaitTermination()

  }

}


