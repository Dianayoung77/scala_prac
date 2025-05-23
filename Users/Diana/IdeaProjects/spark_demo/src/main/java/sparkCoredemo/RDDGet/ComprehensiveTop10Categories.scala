
// 解决 Cannot resolve overloaded method 'println' 问题
println("数据结构验证:") // 确保此处 println 使用正确
println("\n数据样例验证:") // 确保此处 println 使用正确
println("\n最终统计结果:") // 确保此处 println 使用正确
top10Categories.foreach { case (categoryId, count) =>
  println(s"品类ID: $categoryId, 出现次数: $count") // 确保此处 println 使用正确
}

// 解决 Cannot resolve symbol '!=' 问题
val orderRDD = dataRDD
  .filter(row => row.getAs[String]("order_category_ids") != null) // 确保此处 '!=' 使用正确
  .flatMap(row => {
    val categoryIds = row.getAs[String]("order_category_ids").split(",")
    categoryIds
      .filter(id => id.nonEmpty && id != "null")  // 明确参数类型
      .map(id => (id.toLong, 1L))
  })


// 处理 Unused import statement 警告


// 处理 Name boolean parameters 警告
// 在相关行检查布尔参数的命名，提供更具体的名称（此处假设没有直接相关的布尔参数使用，具体需根据实际代码检查）

