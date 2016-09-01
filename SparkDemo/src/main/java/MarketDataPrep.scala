
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkFiles
import org.apache.spark.sql.functions.date_sub
import org.apache.spark.sql.functions.from_unixtime
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.IntegerType
object MarketDataPrep {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MarketDataPrep").setMaster("local[2]").setExecutorEnv("spark.driver.memory", "2g")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    val marketDF = sqlContext.read.format("com.databricks.spark.csv").option("inferschema", "true")
      .option("header", "true").load("D://MarketData/marketData.csv").withColumnRenamed("HH_T", "totalHHH").
      withColumnRenamed("CSP01_MIO", "food").withColumnRenamed("CSP02_MIO", "alcohol").withColumnRenamed("CSP03_MIO", "tobacco")
      .withColumnRenamed("CSP04_MIO", "clothing").withColumnRenamed("CSP05_MIO", "footware").withColumnRenamed("CSP06_MIO", "furniture").withColumnRenamed("CSP07_MIO", "houseTextile")
      .withColumnRenamed("CSP08_MIO", "houseApp").withColumnRenamed("CSP09_MIO", "houseUtensils").withColumnRenamed("CSP10_MIO", "tools").withColumnRenamed("CSP11_MIO", "maintenance")
      .withColumnRenamed("CSP12_MIO", "medical").withColumnRenamed("CSP13_MIO", "electronics").withColumnRenamed("CSP14_MIO", "culture").withColumnRenamed("CSP15_MIO", "toys")
      .withColumnRenamed("CSP16_MIO", "cultureServices").withColumnRenamed("CSP17_MIO", "newsPaper").withColumnRenamed("CSP18_MIO", "catering").withColumnRenamed("CSP19_MIO", "personalCare")
      .withColumnRenamed("CSP20_MIO", "jewellery")
    marketDF.select(marketDF("ADMINCODE"), marketDF("NAME"), marketDF("totalHHH"), marketDF("MALE"), marketDF("FEMALE"), (marketDF("alcohol") + marketDF("tobacco")) as "AlcTob", (marketDF("clothing") +
      marketDF("footware")) as "CloFoot", (marketDF("furniture") + marketDF("houseTextile") + marketDF("houseApp") + marketDF("houseUtensils") + marketDF("tools") +
      marketDF("maintenance")) as "HHProducts", marketDF("medical"), marketDF("electronics"), (marketDF("culture") + marketDF("toys") + marketDF("cultureServices")) as "recreationalServices",
      marketDF("newsPaper"), marketDF("catering"), marketDF("personalCare"), marketDF("jewellery"))

      .coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("D://MarketDataPreptotal/")

  }
}