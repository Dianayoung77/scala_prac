package sparksqldemo.sessiondemo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DatasetCreate {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SessionDemo").setMaster("local[*]")
    val spark = SparkSession.builder().appName("SessionDemo").master("local[*]").getOrCreate()


    //强校验类型数据集合
    //1.使用基本类型序列集合
    //2.使用样例类序列集合
    //3.RDD转换
    import spark.implicits._
    val ds =Seq(1,2,3,4,5).toDS()
    ds.show()


    val ds2 = Seq(user(1,"zhangsan"),user(2,"lisi")).toDS()
    ds2.show()


    //1.dsl




  }


  case class user(age:Int,name:String)
  //序列转换很少


}
