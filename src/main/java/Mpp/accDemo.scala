package Mpp

import org.apache.spark.{SparkConf, SparkContext}

class accDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("acc").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //需求：统计a出现的次数
    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",4),("b",5)))

    //reduceByKey    shuffle  效率低
    val rdd1 = rdd.reduceByKey(_+_)
    //    var sum = 0
    //    rdd.collect().foreach{
    //      case ("a",count) =>
    //        sum += count
    //        println(sum)
    //      case _ => {}
    //    }
    //结论：普通变量执行从driver端发给execu端，
    // 在executor计算完成后，不会返回变量值给driver端
    //变量无法共享，各个execu之间无法共享变量
    //    println(("a",sum))

    //spark为了这个问题，专门设计了一个累加器

    //实现一个累加器啊
    val acc = sc.longAccumulator("sum")

    rdd.foreach{
      case ("a",count) =>
        //使用累加器进行累加  acc.add(count)
        acc.add(count)
      //不要再execu端进行获取累加器的的值，因为不准确
      // println(acc.value)
      //累加器 叫 分布式共享只写变量
      case _ => {}
    }


  }

}
