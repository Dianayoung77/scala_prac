package sparkCoredemo.RDDGet

import org.apache.spark.SparkConf

object product {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("product").setMaster("local[*]")



  }
  //将数据级转为哦RDD

}


