
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkFiles
import org.apache.spark.sql.functions.round
import org.apache.spark.sql.functions._
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
object MarketDataExtract {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MarketDataPrep").setMaster("local[2]").setExecutorEnv("spark.driver.memory", "2g")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    val marketDF = sqlContext.read.format("com.databricks.spark.csv").option("inferschema", "true")
      .option("header", "true").load("D://MarketData/marketData.csv")
    val AlcTob = sqlContext.read.format("com.databricks.spark.csv").option("inferschema", "true")
      .option("header", "true").load("D://MarketDataModelNew/AlcTob.csv")
    val catering = sqlContext.read.format("com.databricks.spark.csv").option("inferschema", "true")
      .option("header", "true").load("D://MarketDataModelNew/catering.csv")
    val CloFoot = sqlContext.read.format("com.databricks.spark.csv").option("inferschema", "true")
      .option("header", "true").load("D://MarketDataModelNew/CloFoot.csv")
    val electronics = sqlContext.read.format("com.databricks.spark.csv").option("inferschema", "true")
      .option("header", "true").load("D://MarketDataModelNew/electronics.csv")
    val HHProducts = sqlContext.read.format("com.databricks.spark.csv").option("inferschema", "true")
      .option("header", "true").load("D://MarketDataModelNew/HHProducts.csv")
    val jewellery = sqlContext.read.format("com.databricks.spark.csv").option("inferschema", "true")
      .option("header", "true").load("D://MarketDataModelNew/jewellery.csv")
    val medical = sqlContext.read.format("com.databricks.spark.csv").option("inferschema", "true")
      .option("header", "true").load("D://MarketDataModelNew/medical.csv")
    val newsPaper = sqlContext.read.format("com.databricks.spark.csv").option("inferschema", "true")
      .option("header", "true").load("D://MarketDataModelNew/newsPaper.csv")
    val personalCare = sqlContext.read.format("com.databricks.spark.csv").option("inferschema", "true")
      .option("header", "true").load("D://MarketDataModelNew/personalCare.csv")
    val recreationalServices = sqlContext.read.format("com.databricks.spark.csv").option("inferschema", "true")
      .option("header", "true").load("D://MarketDataModelNew/recreationalServices.csv")

    val joinDF = marketDF.as("marketDF").join(AlcTob.as("AlcTob"), "ADMINCODE").join(catering.as("catering"), "ADMINCODE").join(CloFoot.as("CloFoot"), "ADMINCODE")
      .join(electronics.as("electronics"), "ADMINCODE").join(HHProducts.as("HHProducts"), "ADMINCODE").join(jewellery.as("jewellery"), "ADMINCODE").join(medical.as("medical"), "ADMINCODE")
      .join(newsPaper.as("newsPaper"), "ADMINCODE").join(personalCare.as("personalCare"), "ADMINCODE").join(recreationalServices.as("recreationalServices"), "ADMINCODE")

    joinDF.select("ADMINCODE", "CTRYCODE", "marketDF.NAME", "P_T", "P_PRM", "HH_T", "marketDF.MALE", "marketDF.FEMALE", "PP_MIO", "PP_PRM", "PP_EURO", "PP_CI", "CSP01_MIO", "CSP01_PRM", "CSP01_EURO", "CSP01_CI", "CSP02_MIO", "CSP02_PRM", "CSP02_EURO", "CSP02_CI", "CSP03_MIO", "CSP03_PRM", "CSP03_EURO", "CSP03_CI", "CSP04_MIO", "CSP04_PRM", "CSP04_EURO", "CSP04_CI", "CSP05_MIO", "CSP05_PRM", "CSP05_EURO", "CSP05_CI", "CSP06_MIO", "CSP06_PRM", "CSP06_EURO", "CSP06_CI", "CSP07_MIO", "CSP07_PRM", "CSP07_EURO", "CSP07_CI", "CSP08_MIO", "CSP08_PRM", "CSP08_EURO", "CSP08_CI", "CSP09_MIO", "CSP09_PRM", "CSP09_EURO", "CSP09_CI", "CSP10_MIO", "CSP10_PRM", "CSP10_EURO", "CSP10_CI", "CSP11_MIO", "CSP11_PRM", "CSP11_EURO", "CSP11_CI", "CSP12_MIO", "CSP12_PRM", "CSP12_EURO", "CSP12_CI", "CSP13_MIO", "CSP13_PRM", "CSP13_EURO", "CSP13_CI", "CSP14_MIO", "CSP14_PRM", "CSP14_EURO", "CSP14_CI", "CSP15_MIO", "CSP15_PRM", "CSP15_EURO", "CSP15_CI", "CSP16_MIO", "CSP16_PRM", "CSP16_EURO", "CSP16_CI", "CSP17_MIO", "CSP17_PRM", "CSP17_EURO", "CSP17_CI", "CSP18_MIO", "CSP18_PRM", "CSP18_EURO", "CSP18_CI", "CSP19_MIO", "CSP19_PRM", "CSP19_EURO", "CSP19_CI", "CSP20_MIO", "CSP20_PRM", "CSP20_EURO", "CSP20_CI", "AlcTobclusterId", "cateringclusterId", "CloFootclusterId", "electronicsclusterId", "HHProductsclusterId", "jewelleryclusterId", "medicalclusterId", "recreationalServicesclusterId", "personalCareclusterId", "newsPaperclusterId")

      .coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("D://MarketDataclus/")

    /*val totalExep = marketDF.select(marketDF("NAME") as "Name",round((marketDF("food")+marketDF("alcohol")+marketDF("tobacco")+marketDF("clothing")+
         marketDF("footware")+marketDF("furniture")+marketDF("houseTextile")+marketDF("houseApp")+marketDF("houseUtensils")+marketDF("tools")+
         marketDF("maintenance")+marketDF("medical")+marketDF("electronics")+marketDF("culture")+marketDF("toys")+marketDF("cultureServices")+
         marketDF("newsPaper")+marketDF("catering")+marketDF("personalCare")+marketDF("jewellery")),2) as "totalexep")
     .coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("D://MarketDataPreptotal/")*/

  }
}