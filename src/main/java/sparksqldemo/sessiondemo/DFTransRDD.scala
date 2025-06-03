package sparksqldemo.sessiondemo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.scalap.scalasig.ClassFileParser.fields

class DFTransRDD {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("DFTransRDD")
    val sc = new SparkContext(conf)
  val rdd = sc.makeRDD(List((1,"zhangsan",20),(2,"lisi",30),(3,"wangwu",40)))
    val rdd1 = rdd.map(t=>(t._1,t._2))
    rdd1.collect().foreach(println)}}
