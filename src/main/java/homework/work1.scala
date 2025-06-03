package homework

import org.apache.spark.SparkConf

object work1 {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.{SparkSession, DataFrame}
    import org.apache.spark.sql.functions._

    // 1. 创建SparkSession并读取JSON文件
    val conf = new SparkConf().setAppName("EmployeeDataAnalysis").setMaster("local[*]")
    val spark = SparkSession.builder().appName("EmployeeDataAnalysis").master("local[*]").getOrCreate()

    // 从employee.json创建DataFrame
    val employeeDF: DataFrame = spark.read.json("C:\\Users\\Diana\\IdeaProjects\\spark_demo\\src\\main\\java\\homework\\employee.json")

    // 2. 执行查询操作
    employeeDF.show()

    //  去除重复数据[11,13](@ref)
    employeeDF.distinct().show()

    //  排除id字段[13,14]
    employeeDF.drop("id").show()

    //  筛选age>20的记录
    employeeDF.filter(col("age") > 20).show()

    //  按name分组
    employeeDF.groupBy("name").count().show()

    //按name升序排列
    employeeDF.orderBy("name").show()

    //  取前3行
    employeeDF.limit(3).show()

    // 重命名name列
    employeeDF.select(col("name").alias("username")).show()

    // 计算age平均值
    employeeDF.agg(avg("age")).show()

    // 查询age最小值
    employeeDF.agg(min("age")).show()

    // 关闭SparkSession
    spark.stop()


  }

}
