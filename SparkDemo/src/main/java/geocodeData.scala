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

object geocodeData {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GeocodeApplication").setMaster("local[1]").setExecutorEnv("spark.driver.memory", "2g")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

     sqlContext.read.format("com.databricks.spark.csv").option("inferschema", "true").load("D://databorderfree/blacklist_emails.csv")
    .withColumnRenamed("C0", "BAD_SHIPPING_EMAIL")
    
     
    val custList_2015_df = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferschema","true").load("D://databorderfree/Unhashed_Emails2.csv")
    custList_2015_df.printSchema()

  }

}