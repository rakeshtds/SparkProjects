package com.pb.spatial

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
object PipAttributes {

  def pointInPolygon(t: (RowCol, Iterable[Feature])) = {

    val iter = t._2

    val points = mutable.Buffer[FeaturePoint]()
    val polygons = mutable.Buffer[FeaturePolygon]()

    val preparedGeometryFactory = new PreparedGeometryFactory()
    iter.foreach(elem => {
      elem match {
        case point: FeaturePoint => points += point
        case polygon: FeaturePolygon => {
          polygon.prepare(preparedGeometryFactory)
          polygons += polygon
        }
      }
    })

    points.flatMap(point => {
      for {
        polygon <- polygons
        if polygon.contains(point.geom)
      } yield {
        point.attr ++ polygon.attr
      }
    })
  }

  val geomFact = new GeometryFactory(new PrecisionModel((1000000.0).toDouble))
  val minLon = (-180.0).toDouble
  val maxLon = (180.0).toDouble
  val minLat = (-90.0).toDouble
  val maxLat = (90.0).toDouble
  val envp = new Envelope(minLon, maxLon, minLat, maxLat)

  lazy val reader = new WKTReader(geomFact)

  def pointRDDEval(t: Row, latlonArr: Array[String]) = {
    try {
      val lon = t(0).toString().toDouble
      val lat = t(1).toString().toDouble
      val geom = geomFact.createPoint(new Coordinate(lon, lat))
      if (geom.getEnvelopeInternal.intersects(envp)) {
        Some(FeaturePoint(geom, latlonArr.map(l => { t.getAs(l).toString })))
      } else
        None
    } catch {
      case _: Throwable => None
    }

  }

  def polygonRDDEval(line: Row, inputArr: Array[String]) = {
    try {

      val geom = reader.read(line.getAs[String]("WKT"))
      if (geom.getEnvelopeInternal.intersects(envp)) {

        Some(FeaturePolygon(geom, inputArr.map(l => { line.getAs(l).toString })))
      } else
        None
    } catch {
      case _: Throwable => None
    }
  }

  import scala.collection.mutable.ListBuffer

  var buf = new ListBuffer[String]()
  def outputSchema(line: Array[String], schemaSize: Int): Row = {
    buf.clear()
    for (a <- 0 to schemaSize - 1) {

      buf += line(a)
    }
    Row.fromSeq(buf)
  }
  
  def schemaRet(schemaInputString:String):StructType={
         StructType(
          schemaInputString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
      }

}