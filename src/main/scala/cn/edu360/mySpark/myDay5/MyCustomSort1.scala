package cn.edu360.mySpark.myDay5

import java.io.Serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MyCustomSort1 {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("").setMaster("local[4]")
    val sc = new SparkContext(conf)
    //规则排序，首先按照颜值排序，颜值相等，就按照年龄排序
    val users = Array("laoduan 30 99", "laozhao 29 9999", "laozhang 28 98", "laoyang 28 99")


    val lines = sc.parallelize(users)
    val userRDD: RDD[User] = lines.map(line => {
      val fields = line.split(" ")
      val name = fields(0)
      val age = fields(1).toInt
      val fv = fields(2).toInt
      new User(name, age, fv)
    })
    val sorted = userRDD.sortBy(u => u)
    val r = sorted.collect()

    println(r.toBuffer)
    sc.stop()
  }

}


class User(val name: String, val age: Int, val fv: Int) extends Ordered[User] with Serializable {
  override def compare(that: User): Int = {
    if (this.fv == that.fv) {
      this.age - that.age
    } else {
      -(this.fv - that.fv)
    }

  }

  override def toString = s"User($name, $age, $fv)"
}