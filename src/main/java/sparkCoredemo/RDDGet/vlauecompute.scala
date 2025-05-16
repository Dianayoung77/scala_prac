

import org.apache.spark.rdd

object vlauecompute {
  def main(args: Array[String]): Unit = {
    //读取数据
  val conf=new org.apache.spark.SparkConf().setAppName("WC").setMaster("local[*]")
    val sc=new org.apache.spark.SparkContext(conf)
    val rdd1 =sc.makeRDD(1 to 4)
    val rdd2 =sc.makeRDD(4 to 8)



    //交集使用
 //   rdd1.intersection(rdd2)
//    //并集使用
//    rdd1.union(rdd2)//窄依赖算子
//
//    //差集使用
 //   rdd1.subtract(rdd2)
//    //拉链使
//    rdd1.zip(rdd2)
//
    val rdd3=sc.makeRDD(Array(1,2,3,4),3)
    val rdd4=sc.makeRDD(Array(7,8,9,10),3)
    val rdd5=rdd3.zip(rdd4)
    rdd5.collect().foreach(println)
    Thread.sleep(Long.MaxValue)

  }

}
