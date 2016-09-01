package com.pb.hackathon.sparkstreaming

import java.sql.Timestamp
import java.text.SimpleDateFormat
import scala.util.Try
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Calendar


object SchemaParser {

  def parse(eachRow: String): Option[OrderRecord] = {
    

    val columns = eachRow.split('|')

    Try {
      if (columns.length == 14) {
        
        Option(OrderRecord(createDate(),columns(0), columns(1), columns(2), columns(3),columns(4), columns(5), columns(6), columns(7) ,columns(8), columns(9), columns(10),columns(11), columns(12), columns(13)))
      } else {
        None
      }
    }.getOrElse(None)
  }

  def createDate() = {
   new java.sql.Timestamp(System.currentTimeMillis())
  }

  def createDelay(input: String): Double = {
    val delay_regex = """[^\d|.]*([0-9\\.]+)\s*(ms|.*)""".r

    input match {
      case delay_regex(value, unit) => {
        if (unit.equalsIgnoreCase("ms")) {
          value.toDouble
        } else {
          0
        }
      }
    }
  }
}