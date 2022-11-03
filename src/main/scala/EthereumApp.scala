import org.apache.spark.sql.{SparkSession, SaveMode, SQLImplicits}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object EthereumApp extends App {

  //starting spark
  val spark = SparkSession.builder()
    .config("spark.master", "local[*]")
    .appName("Ethereum app")
    .getOrCreate()
  /**
 // reading  first df
  val transactionsDF = spark.read
    .option("inferSchema", "true")
    .option("header","true")
    .option("delimiter", ",")
    .csv("BD/transactions.csv")

  // reading second df
  val blocksDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .option("delimiter", ",")
    .csv("BD/blocks.csv")

  //Complete DataFrame
  val joinExpr = transactionsDF.col("block_number") === blocksDF.col("number")
  val ethereum = transactionsDF.join(blocksDF, joinExpr)
    .drop(blocksDF.col("number"))
    .drop(blocksDF.col("hash"))
    .drop(blocksDF.col("nonce"))
    .drop(blocksDF.col("timestamp"))
    .drop(blocksDF.col("transaction_count"))
    .drop(blocksDF.col("sha3_uncles"))
    .drop(blocksDF.col("logs_bloom"))
    .drop(blocksDF.col("transactions_root"))
    .drop(blocksDF.col("state_root"))
    .drop(blocksDF.col("receipts_root"))
    .drop(blocksDF.col("extra_data"))

  ethereum.write
    .mode(SaveMode.Overwrite)
    .parquet("BD/ethereum.parquet")


 **/

  val ethereumDF = spark.read.load("BD/ethereum.parquet")

  //ethereumDF.printSchema()

  //println(ethereumDF.count())

  // Check for null values
  val nullReportDF = ethereumDF.select(
     ethereumDF.columns.map(
     x => count(
     when(col(x).isNull || col(x) === "" || col(x).isNaN, x)
     ).alias(x)
     ): _*
     )
     //creating report table
     nullReportDF.write
     .mode(SaveMode.Overwrite)
     .option("path", "Reports/null_table")
     .saveAsTable("null_table")

  // Check for duplicates
  val duplicateReportDF = ethereumDF.select(
     count("hash").alias("total_rows"),
     countDistinct("hash").alias("unique_rows")
     )
     //creating report table
     duplicateReportDF.write
     .mode(SaveMode.Overwrite)
     .option("path","Reports/duplicate_table")
     .saveAsTable("duplicate_table")




}

