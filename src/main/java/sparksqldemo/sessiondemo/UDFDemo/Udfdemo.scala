package sparksqldemo.sessiondemo.UDFDemo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.UDTRegistration

object Udfdemo {


  //自定义一个udf函数，addname,再指定数据前面添加字符串name
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().appName("Udfdemo").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val df = spark.read.json("C:\\Users\\Diana\\IdeaProjects\\spark_demo\\src\\main\\java\\2.txt")
    spark.udf.register("addname",(x:String)=>"name"+x)
    spark.udf.register("addname2",addname _)
    spark.sql("select addname(name) from df").show()


    //加斜杠和不加斜杠输出结果不同


  }
  def addname(x:String):String={
    "name"+x
  }
}
