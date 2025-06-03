package homework

import org.apache.spark.sql.{SaveMode, SparkSession}
import java.util.Properties

object MySQLDataFrameDemo {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("MySQL DataFrame Operations")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // 数据库配置
    val jdbcUrl = "jdbc:mysql://192.168.10.11:3306/sparktest"
    val jdbcUsername = "root"
    val jdbcPassword = "000000"

    // 创建新数据DataFrame
    val newData = Seq(
      (3, "Mary", "F", 26),
      (4, "Tom", "M", 23)
    ).toDF("id", "name", "gender", "age")

    // 写入MySQL
    newData.write
      .mode(SaveMode.Append)
      .option("driver", "com.mysql.jdbc.Driver")
      .jdbc(jdbcUrl, "employee", new Properties() {
        {
          put("user", jdbcUsername)
          put("password", jdbcPassword)
        }
      })

    // 读取完整数据
    val fullDF = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", "employee")
      .option("user", jdbcUsername)
      .option("password", jdbcPassword)
      .option("driver", "com.mysql.jdbc.Driver")
      .load()

    // 计算并打印统计结果
    fullDF.agg(
      org.apache.spark.sql.functions.max("age").as("max_age"),
      org.apache.spark.sql.functions.sum("age").as("total_age")
    ).show()

    spark.stop()
  }
}