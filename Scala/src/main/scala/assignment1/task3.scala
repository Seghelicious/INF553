package assignment1

import java.io.{File, PrintWriter}

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks._


object task3 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val reviewJson = args(0)
    val businessJson = args(1)
    val outputA = args(2)
    val outputB = args(3)

    val sparkConf = new SparkConf().setAppName("INF553").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val reviewsRdd = sc.textFile(reviewJson).map(x => JsonParser.toReviewJson(x)).repartition(8).map(x => (x.business_id, x.stars))
    val businessRdd = sc.textFile(businessJson).map(x => JsonParser.toBusinessJson(x)).repartition(8).map(x => (x.business_id, x.city))
    val cityRating = businessRdd.join(reviewsRdd).map(x => (x._2._1, x._2._2)).groupByKey().map(x => (x._1, x._2.sum / x._2.size))

    val m1StartTime = System.currentTimeMillis() / 1000
    var outputDataA = cityRating.collect()
    outputDataA = outputDataA.sortBy(x => (-x._2, x._1))
    var count = 0
    breakable {
      for (rating <- outputDataA) {
        if (count == 10)
          break
        count += 1
        println(rating)
      }
    }
    val m1 = System.currentTimeMillis() / 1000 - m1StartTime

    val m2StartTime = System.currentTimeMillis() / 1000
    cityRating.takeOrdered(10)(Ordering[(Float, String)].on(x => (-x._2, x._1))).foreach(println)
    val m2 = System.currentTimeMillis() / 1000 - m2StartTime

    val outputDataB = "{\"m1\": " + m1 + ", \"m2\": " + m2 + "}"

    var dataA = "city,stars\n"
    for (line <- outputDataA) {
      dataA += line._1 + "," + line._2 + "\n"
    }
    val pw1 = new PrintWriter(new File(outputA))
    pw1.write(dataA)
    pw1.close()

    val pw2 = new PrintWriter(new File(outputB))
    pw2.write(outputDataB)
    pw2.close()

    sc.stop()
  }

  object JsonParser {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.registerModule(DefaultScalaModule)

    def toReviewJson(s: String): Review = {
      val json = mapper.readValue(s, classOf[Review])
      json
    }

    def toBusinessJson(s: String): Business = {
      val json = mapper.readValue(s, classOf[Business])
      json
    }
  }

  case class Review(
                     business_id: String,
                     stars: Float
                   )

  case class Business(
                       business_id: String,
                       city: String
                     )

}
