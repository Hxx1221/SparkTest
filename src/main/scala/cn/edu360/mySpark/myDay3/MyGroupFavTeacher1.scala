package cn.edu360.mySpark.myDay3

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MyGroupFavTeacher1 {
  def main(args: Array[String]): Unit = {
    val topN = args(1).toInt
    val conf: SparkConf = new SparkConf().setAppName("Alex").setMaster("local[4]")
    val sc: SparkContext = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile(args(0))
    val subTeacherAndOne: RDD[((String, String), Int)] = lines.map(line => {
      val index: Int = line.lastIndexOf("/")
      val teacher: String = line.substring(index + 1)
      val httpHost: String = line.substring(0, index)
      val subject: String = new URL(httpHost).getHost.split("[.]")(0)
      ((subject, teacher), 1)
    })
    val reduce: RDD[((String, String), Int)] = subTeacherAndOne.reduceByKey(_ + _)
    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduce.groupBy(_._1._1)
    val unit: RDD[(String, List[((String, String), Int)])] = grouped.mapValues(_.toList.sortBy(_._2).reverse.take(topN))

    val tuples: Array[(String, List[((String, String), Int)])] = unit.collect()
    println(tuples.toBuffer)
    sc.stop()
  }


}
