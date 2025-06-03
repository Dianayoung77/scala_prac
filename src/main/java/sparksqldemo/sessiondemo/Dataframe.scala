package sparksqldemo.sessiondemo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Dataframe {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SessionDemo").setMaster("local[*]")
    val spark = SparkSession.builder().appName("SessionDemo").master("local[*]").getOrCreate()
    //聪慧spark数据源进行创建
    //从已存在的RDD进行创建


    val df = spark.read.json("C:\\Users\\Diana\\IdeaProjects\\spark_demo\\src\\main\\java\\2.txt")
    df.describe()
    df.show()






  }





}
