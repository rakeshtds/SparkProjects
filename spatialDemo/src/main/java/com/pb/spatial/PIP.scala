package com.pb.spatial

//import magellan.{ Point, Polygon, PolyLine }

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext
import com.databricks.spark.csv.util.TextFile
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import com.vividsolutions.jts.geom.Coordinate
import com.vividsolutions.jts.geom.Envelope
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.geom.LinearRing
import com.vividsolutions.jts.geom.Polygon
import com.vividsolutions.jts.geom.PrecisionModel
import com.vividsolutions.jts.precision.GeometryPrecisionReducer
import com.vividsolutions.jts.io.WKTReader
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory
import com.vividsolutions.jts.geom.Point

import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.rdd.RDD
import org.apache.spark.{ Logging, SparkConf, SparkContext }

import scala.collection.mutable

import com.databricks.spark.csv.util.TextFile

case class PIPFeatures(lon: String, lat: String, MVID: String, Population: String, Households: String, CAMEO_USA: String, CAMEO_USAG: String, CAMEO_INTL: String, PTile_STATE_PresChildren: String, PTile_NAT_PresChildren: String, PTile_STATE_FinStress: String, PTile_NAT_Finstress: String, IncomeFocus_decile: String)

object PIP {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
      .setAppName("Spark PiP")
      .setMaster("local[2]")
      .registerKryoClasses(Array(
        classOf[Feature],
        classOf[FeaturePoint],
        classOf[FeaturePolygon],
        classOf[Point],
        classOf[Polygon],
        classOf[RowCol]))

    val propFileName = if (args.length == 0) "application.properties" else args(0)
    AppProperties.loadProperties(propFileName, sparkConf)

    val t1 = System.currentTimeMillis()
    val sc = new SparkContext(sparkConf)
    try {
      val conf = sc.getConf
      val sqlContext = new SQLContext(sc)

      val pointsrcDF = sqlContext.read.format("com.databricks.spark.csv").option("inferschema", "true").option("delimiter", "\t").load(conf.get("points.path"))

      val data_full_df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferschema", "true").option("delimiter", ",").load(conf.get("polygons.path"))

      val input = "MVID,Population,Households,CAMEO_USA,CAMEO_USAG,CAMEO_INTL,PTile_STATE_PresChildren,PTile_NAT_PresChildren,PTile_STATE_FinStress,PTile_NAT_Finstress,IncomeFocus_decile"

      val schemaInputString = "lon,lat," + input

      val latlonArr = "C0,C1".split(",")

      val pointRDD: RDD[Feature] = pointsrcDF.flatMap(PipAttributes.pointRDDEval(_, latlonArr))

      val geomFact = new GeometryFactory(new PrecisionModel(1000000.0))
      lazy val reader = new WKTReader(geomFact)

      val minLon = -180.0
      val maxLon = 180.0
      val minLat = -90.0
      val maxLat = 90.0

      val envp = new Envelope(minLon, maxLon, minLat, maxLat)
      val inputArr = input.split(",")
      val polygonRDD: RDD[Feature] = data_full_df
        .flatMap(line => {
          try {
            val geom = reader.read(line.getAs[String]("WKT"))
            if (geom.getEnvelopeInternal.intersects(envp)) {

              Some(FeaturePolygon(geom, inputArr.map(l => { line.getAs(l).toString })))
            } else
              None
          } catch {
            case _: Throwable => None
          }
        })

      val schemaSize = schemaInputString.split(",").size
      val rowRDD = pointRDD.
        union(polygonRDD).
        flatMap(_.toRowCols(4.0)).
        groupByKey().
        flatMap(PipAttributes.pointInPolygon(_)).
        map(_.mkString(",")).map(_.split(","))
        .map(PipAttributes.outputSchema(_, schemaSize))

      val schema = PipAttributes.schemaRet(schemaInputString)

      val pipDF = sqlContext.createDataFrame(rowRDD, schema)

      pipDF.registerTempTable("PIPFeatures")
      pointsrcDF.registerTempTable("pointsrcDF")

      val joinedDF = sqlContext.sql(" SELECT  C0 as lon,C1 as lat,PIPFeatures." + input + " FROM PIPFeatures LEFT OUTER JOIN pointsrcDF ON pointsrcDF.C0 = PIPFeatures.lon and pointsrcDF.C1 = PIPFeatures.lat")
      joinedDF.registerTempTable("joinedTable")
      sqlContext.sql("Select * from PIPFeatures").show()
      pipDF.show()
    } finally {
      sc.stop()
    }
    val t2 = System.currentTimeMillis()

    print("Processing time %d sec".format((t2 - t1) / 1000))
  }
}