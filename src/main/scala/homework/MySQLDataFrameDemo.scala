// 修改前的jdbcUrl可能类似：
// val jdbcUrl = "jdbc:mysql://localhost:3306/spark_demo"

// 修改后的jdbcUrl（方案1-自动创建数据库）：
val jdbcUrl = "jdbc:mysql://localhost:3306/spark_demo?createDatabaseIfNotExist=true"

// 或方案2-使用现有数据库（如test库）：
// val jdbcUrl = "jdbc:mysql://localhost:3306/test"