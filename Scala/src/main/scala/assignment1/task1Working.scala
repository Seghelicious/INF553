package assignment1

import java.io.{File, PrintWriter}

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @project DataMining
 * @author deepakjha on 2/11/20
 */
object task1Working {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val startTime = System.currentTimeMillis() / 1000
    val reviewJson = "/Users/deepakjha/Documents/USC/Sem2/INF553_DataMining/Assignments/DM-Assignments-3.6/Assignment1/yelp_dataset/review.json"
    val output = "/Users/deepakjha/Documents/USC/Sem2/INF553_DataMining/Assignments/DM-Assignments-3.6/Assignment1/output_task1_scala.json"

    //    val reviewJson = args(0)
    //    val output = args(1)

    val sparkConf = new SparkConf().setAppName("INF553").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.textFile(reviewJson).repartition(8).map(x => JsonParser.toJson(x)).persist()

    // The total number of reviews
    val nReviews = rdd.count()

    // The number of reviews in 2018
    val nReviews2018 = rdd.filter(x => (x.date.substring(0, 4) == "2018")).count()

    // The number of distinct users who wrote reviews
    val reviewsUserRdd = rdd.map(x => (x.user_id, 1)).reduceByKey((x, y) => x + y)
    val nUsers = reviewsUserRdd.count()

    // The top 10 users who wrote the largest numbers of reviews and the number of reviews they wrote
    val top10Users = reviewsUserRdd.takeOrdered(10)(Ordering[(Int, String)].on(x => (-x._2, x._1)))

    // The number of distinct businesses that have been reviewed
    val reviewsBusinessRdd = rdd.map(x => (x.business_id, 1)).reduceByKey((x, y) => x + y)
    val nBusinesses = reviewsBusinessRdd.count()

    // The top 10 businesses that had the largest numbers of reviews and the number of reviews they had
    val top10Businesses = reviewsBusinessRdd.takeOrdered(10)(Ordering[(Int, String)].on(x => (-x._2, x._1)))

    var top10UsersStr: String = ""
    for (user <- top10Users) {
      top10UsersStr = top10UsersStr + "[\"" + user._1 + "\", " + user._2 + "],";
    }
    top10UsersStr = top10UsersStr.trim().substring(0, top10UsersStr.length - 1)

    var top10BusinessesStr: String = ""
    for (business <- top10Businesses) {
      top10BusinessesStr = top10BusinessesStr + "[\"" + business._1 + "\", " + business._2 + "],";
    }
    top10BusinessesStr = top10BusinessesStr.trim().substring(0, top10BusinessesStr.length - 1)

    val pw = new PrintWriter(new File(output))
    pw.write("{\"n_review\": " + nReviews.toString + ", \"n_review_2018\": " + nReviews2018.toString +
      ", \"n_user\": " + nUsers + ", \"top10_user\": [" + top10UsersStr + "], \"n_business\":" +
      nBusinesses + ", \"top10_business\": [" + top10BusinessesStr + "]}")
    pw.close()

    sc.stop()
    print("Elapsed time-", System.currentTimeMillis() / 1000 - startTime)
    //    40s
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
                     user_id: String,
                     business_id: String,
                     date: String
                   )

}