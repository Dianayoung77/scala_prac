package sparkCoredemo.RDDGet

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object kvdemo {
  def main(args: Array[String]): Unit = {

      val conf=new SparkConf().setAppName("WC").setMaster("local[*]")
          val sc=new SparkContext(conf)
          val rdd =sc.makeRDD(Array((1,"aaa"),(2,"bbb"),(3,"ccc"),(4,"ddd"),("a","eee")))
            rdd.partitionBy(new MyPartitioner())
    rdd.mapPartitionsWithIndex((index,iter)=>{
      iter.map((index,_))
    })
    //注册自己的分区类
    class MyPartitioner extends Partitioner{
      override def numPartitions: Int = 2//设置分区数
      //给出具体的分区逻辑
      override def getPartition(key: Any): Int = {
        //如果是字符串放在0区，如果是数字放在1区。使用scala的模式匹配
        key match{
          case x:String=>0
          case x:Int=>x%numPartitions
          case _=>0
        }



      }
    }



    rdd.saveAsTextFile("output3")
  }


}
