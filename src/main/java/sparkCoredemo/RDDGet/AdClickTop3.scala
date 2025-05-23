import org.apache.spark.{SparkConf, SparkContext}

object AdClickTop3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AdClickTop3").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 1. 读取原始数据
    val rawRDD = sc.textFile("C:\\Users\\Diana\\IdeaProjects\\spark_demo\\src\\main\\java\\sparkCoredemo\\RDDGet\\agent.log")

    // 2. 数据清洗转换 - 广告点击统计
    val adProvinceRDD = rawRDD.filter(_.split(",").length >= 5).map { line =>
      val fields = line.split(",")
      val province = fields(1).trim
      val ad = fields(4).trim
      ((province, ad), 1)
    }

    // 3. 统计省份-广告点击量
    val adClickCountRDD = adProvinceRDD.reduceByKey(_ + _) // ((省份, 广告), 总点击量)

    // 4. 重组数据结构
    val provinceAdRDD = adClickCountRDD.map {
      case ((province, ad), count) =>
        (province, (ad, count)) // (省份, (广告, 点击量))
    }

    // 5. 按省份分组
    val groupedProvinceAdRDD = provinceAdRDD.groupByKey()

    // 6. 获取各省广告点击Top3
    val provinceTop3Ads = groupedProvinceAdRDD.mapValues { iter =>
      iter.toList
        .sortBy(-_._2) // 按点击量降序排序
        .take(3)       // 取前3名
    }

    // 7. 数据清洗转换 - 城市点击统计
    val cityRDD = rawRDD.filter(_.split(",").length >= 3).map { line =>
      val fields = line.split(",")
      val province = fields(1).trim
      val city = fields(2).trim
      ((province, city), 1)
    }

    // 8. 统计省份-城市点击量
    val cityClickCountRDD = cityRDD.reduceByKey(_ + _) // ((省份, 城市), 总点击量)

    // 9. 重组数据结构
    val provinceCityRDD = cityClickCountRDD.map {
      case ((province, city), count) =>
        (province, (city, count)) // (省份, (城市, 点击量))
    }

    // 10. 按省份分组
    val groupedProvinceCityRDD = provinceCityRDD.groupByKey()

    // 11. 获取各省城市点击Top3
    val provinceTop3Cities = groupedProvinceCityRDD.mapValues { iter =>
      iter.toList
        .sortBy(-_._2) // 按点击量降序排序
        .take(3)       // 取前3名
    }

    // 12. 合并结果
    val finalResult = provinceTop3Ads.join(provinceTop3Cities)

    // 13. 打印输出
    finalResult.collect().foreach {
      case (province, (ads, cities)) =>
        println(s"=== 省份：$province ===")
        println("广告Top3：")
        ads.foreach { case (ad, count) => println(s"  $ad: $count 次") }
        println("城市Top3：")
        cities.foreach { case (city, count) => println(s"  $city: $count 次") }
        println()
    }

    sc.stop()
  }
}