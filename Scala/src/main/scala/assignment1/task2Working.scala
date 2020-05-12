package assignment1

import java.io.{File, PrintWriter}

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 *
 * @project DataMining
 * @author deepakjha on 2/11/20
 */
object task2Working {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val startTime = System.currentTimeMillis() / 1000
    val reviewJson = "/Users/deepakjha/Documents/USC/Sem2/INF553_DataMining/Assignments/DM-Assignments-3.6/Assignment1/yelp_dataset/review.json" //args(0)
    val output = "/Users/deepakjha/Documents/USC/Sem2/INF553_DataMining/Assignments/DM-Assignments-3.6/Assignment1/output_task2_scala.json" //args(1)
    val nPartitions: Int = 100 //args(2)

    //    val reviewJson = args(0)
    //    val output = args(1)
    //    val nPartitions: Int = args(2).toInt

    val sparkConf = new SparkConf().setAppName("INF553").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.textFile(reviewJson).map(x => JsonParser.toJson(x)).map(x => (x.business_id, 1)).persist()

    val defaultNumPartitions = rdd.getNumPartitions
    val defaultNumItems = rdd.mapPartitions(iter => Array(iter.size).iterator, preservesPartitioning = true).collect()
    val defaultStartTime = System.currentTimeMillis() / 1000
    val reviewsBusinessRdd = rdd.reduceByKey((x, y) => x + y).takeOrdered(10)(Ordering[(Int, String)].on(x => (-x._2, x._1)))
    val defaultTime = System.currentTimeMillis() / 1000 - defaultStartTime

    val customRdd = rdd.partitionBy(new HashPartitioner(nPartitions))
    val customNumPartitions = customRdd.getNumPartitions
    val customNumItems = customRdd.mapPartitions(iter => Array(iter.size).iterator, preservesPartitioning = true).collect()
    val customStartTime = System.currentTimeMillis() / 1000
    val customBusinessReviews = customRdd.reduceByKey((x, y) => x + y).takeOrdered(10)(Ordering[(Int, String)].on(x => (-x._2, x._1)))
    val customTime = System.currentTimeMillis() / 1000 - customStartTime

    var defaultItemList: String = "["
    for (item <- defaultNumItems) {
      defaultItemList = defaultItemList + String.valueOf(item) + ", "
    }
    defaultItemList = defaultItemList.trim().substring(0, defaultItemList.length - 2) + "]"

    var customItemList: String = "["
    for (item <- customNumItems) {
      customItemList = customItemList + String.valueOf(item) + ", "
    }
    customItemList = customItemList.trim().substring(0, customItemList.length - 2) + "]"

    val pw = new PrintWriter(new File(output))
    pw.write("{\"default\": " +
      "{\"n_partition\": " + defaultNumPartitions + ", " +
      "\"n_items\": " + defaultItemList + ", " +
      "\"exe_time\": " + defaultTime + "}, " +
      "\"customized\": " +
      "{\"n_partition\": " + customNumPartitions + ", " +
      "\"n_items\": " + customItemList + ", " +
      "\"exe_time\": " + customTime + "}}")
    pw.close()

    sc.stop()
    print("Elapsed time-", System.currentTimeMillis() / 1000 - startTime)
    //    43s
  }

  object JsonParser {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.registerModule(DefaultScalaModule)

    def toJson(s: String): Review = {
      val review = mapper.readValue(s, classOf[Review])
      review
    }
  }

  case class Review(
                     business_id: String
                   )

}
