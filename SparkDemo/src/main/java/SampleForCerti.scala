
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.KeyValueTextInputFormat
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.hadoop.mapreduce.OutputFormat
object SampleForCerti {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("newSample")
    val sc = new SparkContext(conf)

    /*val line = sc.textFile("C://infoPB.txt")

    def convertpair(s: String): (String, Int) = {
      (s, 1)
    }

    val words = line.flatMap(line => line.split(" ")).map(convertpair).reduceByKey(_ + _)
    words.collect()*/

    val input = sc.parallelize(List(1, 2, 3, 4))

    val as = List((101, "A"), (102, "B"), (103, "C"))
    val abos = sc.parallelize(as)
    val rdd1 = sc.parallelize(List((1, 2), (2, 3), (3, 4), (3, 5),(3, 2), (4, 5)))
    val rdd = sc.parallelize(List((3, 2), (4, 5)))
    val rdd2 = sc.parallelize(List(((1, "Z"), 111), ((1, "ZZ"), 111), ((2, "Y"), 222), ((3, "X"), 333)))

  
    /* val result = input.aggregate((0, 0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))	
    val avg = result._1 / result._2.toDouble*/

    /* val newrdd = rdd1.reduceByKey((x,y) => x.concat(y))
    newrdd.foreach(x=>print(x))*/

   rdd1.sortByKey().foreach(x => println(x))
   
 

 /*   val path = "/user/cloudera/scalaspark/departmentsSeq"
    val dataRDD = sc.textFile("/user/cloudera/sqoop_import/departments")
    dataRDD.map(x => (new Text(x.split(",")(0)), new Text(x.split(",")(1)))).
      saveAsNewAPIHadoopFile(path, classOf[Text], classOf[IntWritable], classOf[SequenceFileOutputFormat[Text, IntWritable]])

    sc.newAPIHadoopFile("")
    val input1 = sc.hadoopFile[Text, Text, KeyValueTextInputFormat]("").map {
      case (x, y) => (x.toString, y.toString)
    }

    val data = sc.sequenceFile("", classOf[Text], classOf[IntWritable]).
      map { case (x, y) => (x.toString, y.get()) }

    dataRDD.map(x => (new Text(x.split(",")(0)), new Text(x.split(",")(1))))
      .saveAsHadoopFile(path, classOf[Text], classOf[Text], classOf[OutputFormat[Text, Text]])

    val dataRDD3 = sc.hadoopFile(args(1), classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
      sc.defaultMinPartitions).map(pair => pair._2.toString)
*/
  }

}