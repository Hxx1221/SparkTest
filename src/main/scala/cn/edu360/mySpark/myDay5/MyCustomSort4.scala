package cn.edu360.mySpark.myDay5

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MyCustomSort4 {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Alex").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //排序规则：首先按照颜值的降序，如果颜值相等，再按照年龄的升序
    val users = Array("laoduan 30 99", "laozhao 29 9999", "laozhang 28 98", "laoyang 28 99")

    //将Driver端的数据并行化变成RDD
    val lines: RDD[String] = sc.parallelize(users)


    //切分整理数据
    val tpRDD: RDD[(String, Int, Int)] = lines.map(line => {
      val fields = line.split(" ")
      val name = fields(0)
      val age = fields(1).toInt
      val fv = fields(2).toInt
      (name, age, fv)
    })


    //    import MySortRules.XianRouRule
    //    val unit = tpRDD.sortBy(x=>XianRou(x._2,x._3))

    //通过元组特性进行排序
   // val unit = tpRDD.sortBy(x => (-x._3, x._2))

    implicit val rules = Ordering[(Int,Int)].on[(String, Int, Int)](x=>(-x._3, x._2))

    val unit = tpRDD.sortBy(x => x)

    println(unit.collect().toBuffer)
    sc.stop()

  }
}


case class XianRou(val age: Int, val fv: Int)