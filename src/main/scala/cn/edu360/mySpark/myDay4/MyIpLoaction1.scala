package cn.edu360.mySpark.myDay4

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MyIpLoaction1 {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Alex").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val tuples = MyIpUtils.readFileByIp("F:\\BaiduNetdiskDownload\\ip\\ip.txt")
    val boadcast: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(tuples)
    val accessLines = sc.textFile("F:\\BaiduNetdiskDownload\\ip\\access.log")
    val func = (line: String) => {
      val strings = line.split("[|]")
      val ip = strings(1)
      val ipNum = MyIpUtils.ip2Long(ip)
      val value = boadcast.value
      var province = "未知"
      val index = MyIpUtils.binarySearch(value, ipNum)
      if (index != -1) {
        province = value(index)._3
      }
      (province, 1)
    }

    val provinceAndOne: RDD[(String, Int)] = accessLines.map(func)

    val reduceBykey: RDD[(String, Int)] = provinceAndOne.reduceByKey(_ + _)


    val r = reduceBykey.collect()

    println(r.toBuffer)

    sc.stop()


  }


}
