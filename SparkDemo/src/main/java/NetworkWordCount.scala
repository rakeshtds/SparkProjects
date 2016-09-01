
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.hadoop.hdfs.server.common.Storage
import org.apache.spark.storage.StorageLevel
object NetworkWordCount {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(10))
    val lines = ssc.socketTextStream("152.144.214.59", 9999, StorageLevel.MEMORY_AND_DISK)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()

  }

}