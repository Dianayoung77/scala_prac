package sparkCoredemo.RDDGet

import org.apache.spark.{SparkConf, SparkContext}

object RddCreate {


  def main(args: Array[String]): Unit = {
    //创建sc的配置对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkcore")
    //创建sc对象
    val sc = new SparkContext(conf)
    val list =List(1,2,3,4,5,6,7,8,9,10)
    //创建rdd
    val rdd1 = sc.makeRDD(list)
    val rdd2 = sc.makeRDD(list)
    rdd1.saveAsTextFile("input")
    rdd2.saveAsTextFile("input1")

  }

}
