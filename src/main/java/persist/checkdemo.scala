package persist

import org.apache.spark.{SparkConf, SparkContext}

object checkdemo {

    //check point:检查点：将rdd的中间结果报讯在硬盘中
    //是复活点，在中间阶段中，如果中间结果已经存在，则直接从检查点中恢复，
    // 不再执行中间步骤，重做血缘计算，减少开销
    //check point:检查点：将rdd的中间结果报讯在硬盘中
    //解决血缘链过长问题
    //1.存储路径
    //2.存储格式二进制
    //3.检查点切断血缘的关系，检查点的rdd和父rdd的依赖关系会被移除
    //4.
    def main(args: Array[String]): Unit = {

      val conf = new SparkConf()
      conf.setMaster("local").setAppName("cache1")
      val sc = new SparkContext(conf)
      val rdd = sc.textFile("C:\\Users\\Diana\\IdeaProjects\\spark_demo\\src\\main\\java\\1.txt")

      //提供一个路径用来存放checkpoint文件
      sc.setCheckpointDir("C:\\Users\\Diana\\IdeaProjects\\spark_demo\\src\\main\\java\\check")
      rdd.checkpoint()



      val wordRDD = rdd.flatMap(line => line.split(" "))
        .map(word => (word,1))

      val  Rerdd  = wordRDD.reduceByKey(_ + _)
      //查看血缘关系
      println(Rerdd.toDebugString)
      //    wordRDD.cache() //    等同于wordRDD.persist()  默认是MEMERY_ONLY

      //    wordRDD.persist(StorageLevel.MEMORY_AND_DISK)
      Rerdd.collect().foreach(println)
      //cache 缓存之后，查看血缘关系
      println(Rerdd.toDebugString)

      println("--------------------------------------------------")
      //再次触发action算子，查看缓存是否生效
      Rerdd.collect().foreach(println)
      println(Rerdd.toDebugString)

      Thread.sleep(1000000)
      sc.stop()

    }


}
