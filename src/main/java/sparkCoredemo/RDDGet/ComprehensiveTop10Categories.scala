package sparkCoredemo.RDDGet

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ComprehensiveTop10Categories {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Comprehensive Top 10 Categories")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // 定义符合数据格式的schema
    val schema = StructType(Array(
      StructField("date", StringType),
      StructField("user_id", LongType),
      StructField("session_id", StringType),
      StructField("page_id", LongType),
      StructField("action_time", StringType),
      StructField("search_keyword", StringType),
      StructField("click_category_id", LongType),
      StructField("click_product_id", LongType),
      StructField("order_category_ids", StringType),
      StructField("order_product_ids", StringType),
      StructField("pay_category_ids", StringType),
      StructField("pay_product_ids", StringType),
      StructField("city_id", LongType)
    ))

    // 使用单个下划线作为分隔符
    val rawData = spark.read
      .schema(schema)
      .option("sep", "_")
      .csv("C:\\Users\\Diana\\IdeaProjects\\spark_demo\\src\\main\\java\\user_visit_action.txt")

    // 数据验证：检查加载的数据结构
    println("数据结构验证:")
    rawData.printSchema()
    rawData.show(1, false)

    // 预处理：转换时间字段格式
    val parsedData = rawData.withColumn(
      "action_time",
      to_timestamp($"action_time", "yyyy-MM-dd HH:mm:ss")
    )

    // 将DataFrame转换为RDD
    val dataRDD = parsedData.rdd

    // 提取点击行为中的品类ID（单值，无需展开）
    val clickRDD = dataRDD
      .filter(row => row.getAs[Long]("click_category_id") != -1)
      .map(row => (row.getAs[Long]("click_category_id"), 1L))

    // 提取下单行为中的品类ID（多值，需展开并过滤"null"）
    // 提取下单行为中的品类ID（修复后）
    val orderRDD = dataRDD
      .filter(row => row.getAs[String]("order_category_ids") != null)
      .flatMap(row => {
        val categoryIds = row.getAs[String]("order_category_ids").split(",")
        categoryIds
          .filter(id => id.nonEmpty && id != "null")  // 明确参数类型
          .map(id => (id.toLong, 1L))
      })

    // 提取支付行为中的品类ID（修复后）
    val payRDD = dataRDD
      .filter(row => row.getAs[String]("pay_category_ids") != null)
      .flatMap(row => {
        val categoryIds = row.getAs[String]("pay_category_ids").split(",")
        categoryIds
          .filter(id => id.nonEmpty && id != "null")  // 明确参数类型
          .map(id => (id.toLong, 1L))
      })

    // 合并所有品类ID的RDD
    val allCategoriesRDD = clickRDD.union(orderRDD).union(payRDD)

    // 使用reduceByKey统计品类出现次数，并按次数排序
    val categoryCountsRDD = allCategoriesRDD
      .reduceByKey(_ + _)          // 按品类ID聚合次数
      .map { case (categoryId, count) => (count, categoryId) }  // 交换键值对，便于按次数排序
      .sortByKey(false)            // 降序排序
      .map { case (count, categoryId) => (categoryId, count) }  // 恢复原始键值对顺序

    // 取Top 10热门品类
    val top10Categories = categoryCountsRDD.take(10)

    // 显示结果
    println("\n数据样例验证:")
    parsedData.select($"action_time", $"click_category_id", $"order_category_ids").show(3, false)

    println("\n最终统计结果:")
    top10Categories.foreach { case (categoryId, count) =>
      println(s"品类ID: $categoryId, 出现次数: $count")
    }

    spark.stop()
  }
}