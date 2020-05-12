package assignment2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @project DM-Assignments
 * @author deepakjha on 2/15/20
 */
object task1 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val startTime = System.currentTimeMillis() / 1000
    val caseTask: Int = args(0).toInt
    val support: Int = args(1).toInt
    val inputFile = "/Users/deepakjha/Documents/USC/Sem2/INF553_DataMining/Assignments/DM-Assignments-3.6/Assignment1/yelp_dataset/review.json"
    val outputFile = "/Users/deepakjha/Documents/USC/Sem2/INF553_DataMining/Assignments/DM-Assignments-3.6/Assignment1/output_task1_scala.json"

    //    val caseTask: Int = args(0).toInt
    //    val support: Int = args(1).toInt
    //    val inputFile = args(2)
    //    val outputFile = args(3)

    val phase1Candidates: List[String] = List()

    val sparkConf = new SparkConf().setAppName("INF553").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.textFile(inputFile)

    val columnNames = rdd.first()

    val basketRdd = rdd.filter(x => !x.equals(columnNames)).map(x => getRequiredBucket(x, caseTask)).map(r => {
      val x = r.split(",")
      (x(0), x(1))
    }).groupByKey().mapValues(x=>x.toSet).map(x => x._1).persist()

    val basketList = basketRdd.collect()
    val basketCount = basketList.length

    val phase1Map = basketRdd.mapPartitions(x => apriori(x, support))

  }


  def getRequiredBucket(row: String, caseTask: Int): String = {
    val line = row.split(",")
    if (caseTask == 1) {
      line(0) + "," + line(1)
    } else {
      line(1) + "," + line(0)
    }
  }

  def apriori(x: Iterator[List[String]], support: Int): Iterator[(List[String], Int)] = {
//    chunk = list(baskets)
//    chunk_support = support * (len(chunk) / basket_count)
//    items = []
//    for basket in chunk:
//    for item in basket:
//      items.append(item)
//
//    #frequent itemsets of size 1
//    frequent_items = filter_by_support(create_count_dict(items, chunk_support), chunk_support, False)
//    size = 2
//    while len(frequent_items) != 0:
//      frequent_items = find_frequent_candidates(chunk, chunk_support, frequent_items, size)
//    size += 1
//
//    return [phase1_candidates]



    var chunk = scala.collection.mutable.ListBuffer.empty[List[String]]
    var items = scala.collection.mutable.ListBuffer.empty[String]
    var singleCount = scala.collection.mutable.HashMap.empty[String, Int]

    //    var returns = scala.collection.mutable.ListBuffer.empty[(List[String], Int)]
//    val chunkSupport = support.toFloat/p

    while (x.hasNext) {
      var basket = x.next;
      chunk += basket
      for (item <- basket){
        items += item
      }
    }

    // return Iterator[U]
    Iterator.empty
  }

}
