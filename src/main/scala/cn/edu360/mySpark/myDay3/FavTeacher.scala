package cn.edu360.mySpark.myDay3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FavTeacher {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("FavTeacher").setMaster("local[2]")

    val sc: SparkContext = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile(args(0))

    val unit: RDD[(String, Int)] = lines.map(line => {
      val i: Int = line.lastIndexOf("/")
      val str: String = line.substring(i + 1)
      (str, 1)
    })
    val reduced: RDD[(String, Int)] = unit.reduceByKey((x, y) => {
      x + y
    })

    val value: RDD[(String, Int)] = reduced.sortBy(_._2,false)
    val tuples: Array[(String, Int)] = value.collect()
    System.out.println(tuples.toBuffer)
    sc.stop()

  }
}
