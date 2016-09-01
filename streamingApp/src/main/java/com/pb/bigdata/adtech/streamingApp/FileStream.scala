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
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.HiveContext

object FileStream {
  case class USAAddress(ID: Integer, addressline1: String, addressline2: String, city: String, state: String, zip: Integer, county: String)

  def parseAddress(Addr: String): USAAddress = {
    val p = Addr.split(",")
    USAAddress(p(0).toInt, p(1), p(2), p(3), p(4), p(5).toInt, p(6))
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FileStreamFromApp")
    //conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
      val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
  
      hiveContext.sql("""create temporary function geocode as 'com.pb.fusion.hive.geocoding.AddressGeocode'""");
      hiveContext.sql("""create temporary function udaf as 'com.pb.fusion.hive.pointinpolygon.FindNearestUDAF'""");
      hiveContext.sql("""create temporary function geohash as 'com.pb.fusion.hive.pointinpolygon.GetGeoHash'""");
  
      val mld_geohash = hiveContext.sql("select * from ad_tech.mld_geohash");
      mld_geohash.registerTempTable("mld_geohash")
     // hiveContext.cacheTable("mld_geohash")
      hiveContext.sql("select count(*) from mld_geohash").show()
  
      val demographics = hiveContext.sql("select * from ad_tech.demographics")
      /*demographics.repartition(demographics("PBKEY"))
      demographics.count()
      demographics.persist()*/
      demographics.registerTempTable("demographics")
     // hiveContext.cacheTable("demographics")
      hiveContext.sql("select count(*) from demographics").show()

    val ssc = new StreamingContext(sc, Seconds(30))
    val inputData = ssc.textFileStream("hdfs://152.144.62.96:8020/domain/li/data/address/")

    print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
    val addressStream = inputData.map(parseAddress)

    addressStream.foreachRDD(rdd => {
      print("++++++++++++++++++++++++++++++++++++++++++++")

      if (!rdd.isEmpty) {
        print("********************************************************************************************************************************************************************************************************")

        val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
        import sqlContext.implicits._
        import org.apache.spark.sql.functions._

        rdd.toDF().registerTempTable("newtable")

        val df2 = hiveContext.sql("select *, geocode(addressline1,addressline2,city,state,zip,'USA') as geocode from newtable")
        val flatDF = hiveContext.read.json(df2.map { case Row(c0: Int, c1: String, c2: String, c3: String, c4: String, c5: Int, c6: String, c7: String) => s"""{"ID": "$c0","addline1": "$c1","addline2": "$c2","city": "$c3","state": "$c4","zip": "$c5","country": "$c6", "geocode": $c7}""" })

        flatDF.select(flatDF("ID"), flatDF("city"), flatDF("state"), flatDF("geocode.LAT") as "Latitude", flatDF("geocode.LON") as "Longitude", flatDF("geocode.GeoScore") as "GeoScore").registerTempTable("geocode_output")

        val smallDF = hiveContext.sql("select *, geohash(Latitude,Longitude,11) as geohash11,geohash(Latitude,Longitude,5) as geohash5 from geocode_output")
        //.registerTempTable("geohash_output")
        
        import org.apache.spark.sql.functions.broadcast
        val br = mld_geohash.join(broadcast(smallDF),mld_geohash("geohash").substr(1, 5) === smallDF("geohash5"))
        
        
        br.cache()
        val df_new = hiveContext.sql("select a.Latitude, a.Longitude,geohash11, udaf(a.Latitude,a.Longitude,b.lat,b.lon,b.pbkey) from geohash_output a, ad_tech.mld_geohash b where geohash5 = substr(b.geohash,1,5) group by geohash11, a.Latitude, a.Longitude");
        // df_new.repartition(df_new("geohash"))
        //        df_new.cache()

        val flatDF2 = hiveContext.read.json(df_new.map { case Row(c0: String, c1: String, c2: String, c3: String) => s"""{"Latitude": "$c0","Longitude": "$c1","geohash": "$c2","gs": $c3}""" })

        val newDF = flatDF2.select(flatDF2("Latitude"), flatDF2("Longitude"), flatDF2("geohash"), flatDF2("gs.latitude") as "Lat", flatDF2("gs.Longitude") as "Lon", flatDF2("gs.id") as "id", flatDF2("gs.Distance") as "Distance")

        newDF.join(demographics, newDF("id") === demographics("PBKEY")).show(10)

        //.registerTempTable("joinedTable")

        //hiveContext.sql("select * from joinedTable jt,ad_tech.demographics demo where jt.id = demo.PBKEY").show(10)
        /* val joinfinancialAssestDF = hiveContext.sql("select * from ad_tech.financialAssest a, joinedDemography b where b.BKG_KEY = a.CODE ");

        joinfinancialAssestDF.show(10)
*/
      }
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
