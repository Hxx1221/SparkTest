package cn.edu360.mySpark.myDay3

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MyGroupFavTeacher2 {


  def main(args: Array[String]): Unit = {

    val topN: Int = args(1).toInt

    val subjects: Array[String] = Array("bigdata","javaee","php")


    val conf: SparkConf = new SparkConf().setAppName("Alex").setMaster("local[4]")
    val sc = new SparkContext(conf)

    sc.setCheckpointDir("hdfs://106.12.29.156:9000/ck2")

    val lines: RDD[String] = sc.textFile(args(0))

    //整理数据
    val sbjectTeacherAndOne: RDD[((String, String), Int)] = lines.map(line => {
      val index = line.lastIndexOf("/")
      val teacher = line.substring(index + 1)
      val httpHost = line.substring(0, index)
      val subject = new URL(httpHost).getHost.split("[.]")(0)
      ((subject, teacher), 1)
    })

    //聚合，将学科和老师联合当做key
    val reduced: RDD[((String, String), Int)] = sbjectTeacherAndOne.reduceByKey(_+_)

    reduced.checkpoint()

    for (sb<-subjects)
      {
        val filtered: RDD[((String, String), Int)] = reduced.filter(_._1._1==sb)

        val tuples: Array[((String, String), Int)] = filtered.sortBy(_._2,false).take(topN)

        println(tuples.toBuffer)

      }

    sc.stop()



  }
}