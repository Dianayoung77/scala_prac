//package sparksqldemo.sessiondemo.UDFDemo
//
//import org.apache.spark.sql.{SparkSession, Encoder, Encoders}
//import org.apache.spark.sql.functions.udaf // 添加缺失的导入
//
//object UdafDemo { // 修正命名
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder()
//      .appName("UdafDemo") // 统一命名
//      .master("local[*]")
//      .getOrCreate()
//
//    val df = spark.read.json("your_path.json")
//    df.createTempView("user")
//
//    // 正确注册UDAF
//    spark.udf.register("avgAge", udaf(new AvgAge))
//
//    // 使用示例
//    spark.sql("SELECT avgAge(age) FROM user").show()
//    spark.stop()
//  }
//
//  // 设为私有解决访问警告
//  private case class AvgBuffer(var sum: Long, var count: Long)
//
//  private class AvgAge extends Aggregator[Long, AvgBuffer, Double] {
//    override def zero: AvgBuffer = AvgBuffer(0L, 0L)
//
//    override def reduce(b: AvgBuffer, a: Long): AvgBuffer = {
//      b.sum += a
//      b.count += 1
//      b
//    }
//
//    override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
//      b1.sum += b2.sum
//      b1.count += b2.count
//      b1
//    }
//
//    override def finish(reduction: AvgBuffer): Double =
//      reduction.sum.toDouble / reduction.count
//
//    override def bufferEncoder: Encoder[AvgBuffer] =
//      Encoders.product[AvgBuffer]
//
//    override def outputEncoder: Encoder[Double] =
//      Encoders.scalaDouble
//  }
//}
