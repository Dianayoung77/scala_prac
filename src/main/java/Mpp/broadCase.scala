package Mpp

import org.apache.spark.{SparkConf, SparkContext}

class broadCase {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("broad").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd=sc.makeRDD(List("25/05/23 16:01:51 INFO TaskSetManager: Finished task 0.0 in stage 4.0 (TID 8) in 15 ms on LAPTOP-33VL88NN (executor driver) (1/2)",
      "25/05/23 16:01:51 INFO TaskSetManager: Finished task 1.0 in stage 4.0 (TID 9) in 12 ms on LAPTOP-33VL88NN (executor driver) (2/2)",
      "25/05/23 16:01:51 INFO TaskSchedulerImpl: Removed TaskSet 4.0, whose tasks have all completed, from pool"

    ),3)
    val logLevel =sc.broadcast("INFO")
    val rdd1=rdd.filter(line=>line.contains(logLevel.value))
    rdd1.foreach(println)
    //广播变量
    val logLevel1 =sc.broadcast(List("INFO","ERROR"))
    val rdd2=rdd.filter(line=>logLevel1.value.contains(line.split(" ")(7)))
    rdd2.foreach(println)
    sc.stop()



  }

}
