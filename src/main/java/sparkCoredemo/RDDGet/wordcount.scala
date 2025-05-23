package sparkCoredemo.RDDGet

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable

object wordcount {
  // 1. 创建自定义Map类型累加器
  class MapAccumulator extends AccumulatorV2[(String, Int), mutable.Map[String, Int]] {
    private val _map = mutable.Map[String, Int]()
    
    override def isZero: Boolean = _map.isEmpty
    override def copy(): AccumulatorV2[(String, Int), mutable.Map[String, Int]] = new MapAccumulator
    override def reset(): Unit = _map.clear()
    override def add(v: (String, Int)): Unit = _map.update(v._1, _map.getOrElse(v._1, 0) + v._2)
    override def merge(other: AccumulatorV2[(String, Int), mutable.Map[String, Int]]): Unit = 
      other.value.foreach { case (k, v) => _map.update(k, _map.getOrElse(k, 0) + v) }
    override def value: mutable.Map[String, Int] = _map
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WC").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    // 2. 注册自定义累加器
    val acc = new MapAccumulator
    sc.register(acc, "wordCountAcc")
    
    val rdd = sc.makeRDD(List(("hello",12),("spark",11),("hello",10),("scala",9)))
    
    // 3. 使用累加器替代reduceByKey
    rdd.foreach { case (word, count) =>
      acc.add((word, count))  // 在行动操作中累加
    }
    
    // 4. 直接输出累加器结果
    println("WordCount结果:")
    acc.value.foreach(println)
    
    sc.stop()
  }
}