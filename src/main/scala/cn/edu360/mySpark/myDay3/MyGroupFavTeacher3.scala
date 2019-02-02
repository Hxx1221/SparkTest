package cn.edu360.mySpark.myDay3

import java.net.URL

import cn.edu360.day3.SubjectParitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object MyGroupFavTeacher3 {


  def main(args: Array[String]): Unit = {

    val topN: Int = args(1).toInt

    val conf: SparkConf = new SparkConf().setAppName("Alex").setMaster("local[2]")


    val sc = new SparkContext(conf)
    val lines = sc.textFile(args(0))
    //整理数据
    val sbjectTeacherAndOne: RDD[((String, String), Int)] = lines.map(line => {
      val index = line.lastIndexOf("/")
      val teacher = line.substring(index + 1)
      val httpHost = line.substring(0, index)
      val subject = new URL(httpHost).getHost.split("[.]")(0)
      ((subject, teacher), 1)
    })
    val myParitioner = new MySubjectParitioner(sbjectTeacherAndOne.map(_._1._1).distinct().collect())
    val sbjectTeacher = sbjectTeacherAndOne.reduceByKey(myParitioner,_+_)
 //   val sbjectTeacherPartitioner = sbjectTeacher.partitionBy(myParitioner)
    val unit = sbjectTeacher.mapPartitions(it => {
      it.toList.sortBy(_._2).reverse.take(topN).iterator
    })
    val tuples: Array[((String, String), Int)] = unit.collect()
    println(tuples.toBuffer)
  }


}

class MySubjectParitioner(abs: Array[String]) extends Partitioner {


   val stringToInt = new mutable.HashMap[String, Int]()
  var index = 0
  for (key <- abs) {
    stringToInt.put(key, index)
    index += 1
  }


  override def numPartitions: Int = abs.length

  override def getPartition(keys: Any): Int = {
    val key = keys.asInstanceOf[(String,String)]._1
     stringToInt(key)
  }
}