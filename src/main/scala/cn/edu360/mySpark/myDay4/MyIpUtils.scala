package cn.edu360.mySpark.myDay4

import scala.io.Source

object MyIpUtils {
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  def readFileByIp(path: String): Array[(Long, Long, String)] = {

    val lines: Iterator[String] = Source.fromFile(path).getLines()


    val tuples: Array[(Long, Long, String)] = lines.map(line => {
      val strings = line.split("[|]")

      val ipStart = strings(2).toLong
      val ipEnd: Long = strings(3).toLong

      val str = strings(6)

      (ipStart, ipEnd, str)

    }).toArray
    tuples
  }


  def binarySearch(ipLines: Array[(Long, Long, String)], ipNum: Long):Int = {
    var lows = 0
    var high = ipLines.length - 1
    while (lows <= high) {
      var zj = (lows + high) / 2
      val start = ipLines(zj)._1
      val end = ipLines(zj)._2
      if (start <= ipNum && ipNum <= end) {
      return  zj
      }
      if (ipNum > end) {
        lows = zj + 1
      } else {
        high = zj - 1
      }
    }
    -1
  }
}
