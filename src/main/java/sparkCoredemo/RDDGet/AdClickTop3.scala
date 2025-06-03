import org.apache.spark.{SparkConf, SparkContext}

object AdClickAnalysisWithTopCities {

  case class ClickLog(province: String, city: String, ad: String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("AdClickAnalysisWithTopCities")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    // ===== 1. 数据加载与清洗=====
    val rawRDD = sc.textFile("C:\\Users\\Diana\\IdeaProjects\\spark_demo\\src\\main\\java\\agent.log")

    val cleanRDD = rawRDD.flatMap { line =>
      val fields = line.trim.split("\\s+")
      if (fields.length >= 5) {

        // 时间戳(0) 省份(1) 城市(2) 用户(3) 广告(4)
        Some(ClickLog(
          province = fields(1),
          city = fields(2),
          ad = fields(4)
        ))
      } else {
        None
      }
    }.persist()

    // ===== 2. 原需求：各省广告点击Top3=====
    val provinceAdTop3 = cleanRDD
      .map(log => ((log.province, log.ad), 1))        // map阶段
      .reduceByKey(_ + _)                            // reduceByKey
      .map { case ((p, a), c) => (p, (a, c)) }       // 重组数据结构
      .groupByKey()                                  // groupByKey
      .mapValues(_.toList.sortBy(-_._2).take(3))     // 排序取Top3

    // ===== 3. 新增需求：Top3省份中的城市Top3 =====
    // 3.1 找出总点击量Top3的省份（新需求前提）
    val top3Provinces = cleanRDD
      .map(log => (log.province, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .map(_._1)
      .take(3)

    // 3.2 使用广播变量优化过滤
    val bcTopProvinces = sc.broadcast(top3Provinces.toSet)

    // 3.3 城市级统计（嵌套分组排序）
    val cityAnalysis = cleanRDD
      .filter(log => bcTopProvinces.value.contains(log.province))  // 过滤Top省份
      .map(log => ((log.province, log.city, log.ad), 1))           // 构建三元组键
      .reduceByKey(_ + _)                                          // 城市广告聚合
      .map { case ((p, c, a), cnt) => ((p, c), (a, cnt)) }         // 重组结构
      .groupByKey()                                               // 按省+城市分组
      .mapValues(_.toList.sortBy(-_._2).take(3))                  // 城市内广告排序
      .map { case ((p, c), ads) => (p, (c, ads)) }               // 格式转换
      .groupByKey()                                              // 按省份聚合城市数据
      .mapValues(_.toList.sortBy(-_._2.map(_._2).sum))          // 按城市总点击排序
      .mapValues(_.take(3))                                     // 取城市Top3

    // ===== 4. 结果输出（）=====
    println("=== 原需求结果（各省广告Top3）===")
    provinceAdTop3.collect().foreach {
      case (province, ads) =>
        println(s"$province: ${ads.map(t => s"${t._1}(${t._2})").mkString(", ")}")
    }

    println("\n=== 新增需求结果（Top3省份的城市Top3）===")
    cityAnalysis.collect().foreach {
      case (province, cities) =>
        println(s"【$province】")
        cities.foreach { case (city, ads) =>
          println(s"  ${city}: ${ads.map(t => s"${t._1}(${t._2})").mkString(", ")}")
        }
    }

    sc.stop()
  }
}