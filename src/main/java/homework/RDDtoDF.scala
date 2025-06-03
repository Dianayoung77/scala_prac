package homework

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object RDDtoDF {
  def main(args: Array[String]): Unit = {
    // 创建 SparkSession
    val spark = SparkSession.builder()
      .appName("RDD to DataFrame")
      .master("local[*]")  // 本地模式运行
      .getOrCreate()

    // 读取文本文件创建 RDD
    val employeeRDD = spark.sparkContext.textFile("C:\\Users\\Diana\\IdeaProjects\\spark_demo\\src\\main\\java\\homework\\employee.txt")

    // 定义 Schema
    val schemaString = "id name age"
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    // 转换 RDD 为 Row 对象
    val rowRDD = employeeRDD.map(_.split(","))
      .map(attributes => Row(attributes(0).trim, attributes(1).trim, attributes(2).trim))

    // 创建 DataFrame
    val employeeDF = spark.createDataFrame(rowRDD, schema)

    // 格式化输出
    employeeDF.rdd.map(row =>
      s"id:${row(0)},name:${row(1)},age:${row(2)}"
    ).foreach(println)

    spark.stop()
  }
}