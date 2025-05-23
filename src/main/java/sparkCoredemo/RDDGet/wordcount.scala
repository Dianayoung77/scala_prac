package sparkCoredemo.RDDGet

import org.apache.spark.{SparkConf, SparkContext}

object wordcount {
  def main(args: Array[String]): Unit = {
      val conf=new SparkConf().setAppName("WC").setMaster("local[*]")
          val sc=new SparkContext(conf)
          val rdd=sc.makeRDD(List(("hello",12),("spark",11),("hello",10),("scala",9)))
//    val rdd1=rdd.groupByKey().collect().foreach(println)//变成（key,seq(v1,v2）
    val rdd2=rdd.map(t=>(t._1,t._2)).collect().foreach(println)

    //reducebykey,按照key完成value值，聚合
    val rdd3=rdd.reduceByKey(_+_).collect().foreach(println)
    //reducebykey,按照key完成value值，聚合

  }

}
