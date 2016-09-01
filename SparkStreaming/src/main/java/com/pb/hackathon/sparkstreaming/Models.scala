package com.pb.hackathon.sparkstreaming

import java.sql.{ Date, Timestamp }
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.commons.lang3.time.DateUtils

case class OrderRecord(dateTime: Timestamp,
    ordernumber:String,
    firstname:String,
    lastname:String,
    street1:String,
    street2:String,
    street3:String,
                       city: String,
                       state: String,
                       country: String,
                       postalcode:String,
                       phonenumber1:String,
                       email:String,
                       merchantName: String,
                       categoryname: String)

                       
                      
case class CountryWiseStats(date: String, country: String, count: BigInt)

case class StateWiseStats(date: String, state: String, count: BigInt)

case class CityWiseStats(date: String, city: String, status: String, count: BigInt)

object test {
  val pattern = "yyyy-MM-dd HH:mm:ss"

  def recentHours(hour: Int, inputDate: Date, input: Int): Boolean =
    {
      val cal = Calendar.getInstance()
      cal.setTimeInMillis(System.currentTimeMillis())
      val presentHour = cal.get(Calendar.HOUR_OF_DAY)
      val minHours = presentHour - input
      val presentDate = new Date(cal.getTimeInMillis)
      if (DateUtils.isSameDay(presentDate, inputDate)) {
        if (hour <= presentHour && hour >= minHours) {
          true
        } else {
          false
        }
      } else {
        false
      }
    }

  def main(args: Array[String]) {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val date: Date = new Date(sdf.parse("2016-03-07").getTime)
    println(recentHours(8, date, 12))
  }
}