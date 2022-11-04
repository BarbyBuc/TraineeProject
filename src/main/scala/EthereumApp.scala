import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object EthereumApp extends App {

  //starting spark
  val spark = SparkSession.builder()
    .config("spark.master", "local[*]")
    .appName("Ethereum app")
    .getOrCreate()
  /**    this part was commented after the first run
   *
   *
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


 */

  val ethereumDF = spark.read.load("BD/ethereum.parquet")

  //ethereumDF.printSchema()

  //ethereumDF.count()

  // Check for null values
  val nullReportDF = ethereumDF.select(
     ethereumDF.columns.map(
     x => count(
     when(col(x).isNull || col(x) === "" || col(x).isNaN, x)
     ).alias(x)
     ): _*
     )
  // Creating report table
  nullReportDF.write
     .mode(SaveMode.Overwrite)
     .option("path", "Reports/null_table")
     .saveAsTable("null_table")

  // Check for duplicates
  val duplicateReportDF = ethereumDF.select(
     count("hash").alias("total_rows"),
     countDistinct("hash").alias("unique_rows")
     )
  // Creating report table
  duplicateReportDF.write
     .mode(SaveMode.Overwrite)
     .option("path","Reports/duplicate_table")
     .saveAsTable("duplicate_table")

  // Transforming unixtime to timestamp
  val ethereumDFwithTime = ethereumDF.withColumn("block_timestamp", from_unixtime(col("block_timestamp")))
    .orderBy(col("block_timestamp"))

  val firstTimeDF = ethereumDFwithTime.select(col("block_timestamp"))

  val lastTime = firstTimeDF.tail(1).toList



  // Identifying type of transactions
  val contractsDF = ethereumDF.withColumn(
    "type", when(col("to_address").isNull, "Contract")
      .otherwise("Regular")
  )

  // Percentage of contracts transactions in the period
  val contractsPercentage = contractsDF.groupBy("type").agg(
    count("hash").alias("count"),
    round(count("hash") / contractsDF.count() * 100, 2).alias("percentage")
  )
  // Creating report table
  contractsPercentage.write
    .mode(SaveMode.Overwrite)
    .option("path", "Reports/contract_table")
    .saveAsTable("contract_table")

  // DF with only regular transactions
  val regularTransactionsDF = ethereumDF.na.drop()

  //regularTransactionsDF.count()

  // Count information
  val countsDF = regularTransactionsDF.agg(
    count("hash").alias("total_transactions"),
    countDistinct("block_number").alias("blocks"),
    countDistinct("from_address").alias("senders"),
    countDistinct("miner").alias("miners"),
    sum(when(col("nonce") === "0",1).otherwise(0).alias("new_wallets"))
  )
  // Creating reporting table
  countsDF.write
    .mode(SaveMode.Overwrite)
    .option("path","Reports/counts_table")
    .saveAsTable("counts_table")


  // Identifying token
  val tokensDF = regularTransactionsDF.withColumn(
    "token", when(col("value") === 0, "Other").otherwise("Ether")
  )

  // Calculate total spent gas
  val totalSpentGas = tokensDF.agg(sum("gas_price")).collect()(0).getLong(0)

  // Observations about tokens
  val tokensObservations = tokensDF.groupBy("token").agg(
    count("hash").alias("quantity"),
    round((count("hash") / tokensDF.count() * 100), 2).alias("%quantity"),
    sum("gas_price").alias("gas_spent"),
    round((sum("gas_price") / totalSpentGas * 100), 2).alias("%gas_spent")
  )
  // Creating report table
  tokensObservations.write
    .mode(SaveMode.Overwrite)
    .option("path", "Reports/tokens_table")
    .saveAsTable("tokens_table")


  import spark.implicits._

  // Miners list
  val minerList = tokensDF.select("miner").distinct().map(x => x.getString(0))
    .collect().toList

  // Identifying transactions from miners
  val minersTransactions = tokensDF.withColumn(
    "minerTr", col("from_address")
      .isin(minerList:_*))

  // Numbers about Miners transactions
  val minersTransactions2 = minersTransactions.groupBy("token","minerTr").agg(
    count("hash").alias("quantity"),
    round((count("hash") / minersTransactions.count() * 100), 2).alias("%quantity")
  ).orderBy("token","quantity")

  // Creating report table
  minersTransactions2.write
    .mode(SaveMode.Overwrite)
    .option("path","Reports/miners_table")
    .saveAsTable("miners_table")

  // Total ether transactions
  val totalEtherTransactions = minersTransactions2.where("token == 'Ether'")
    .agg(sum("quantity"))
    .collect()(0).getLong(0)

  // Miners ether transactions final numbers
  val minersEtherTransactions = minersTransactions2.where("token == 'Ether'")
    .withColumn("%final", round(col("quantity")/totalEtherTransactions*100,2))
    .drop("%quantity")

  // Creating report table
  minersEtherTransactions.write
    .mode(SaveMode.Overwrite)
    .option("path", "Reports/ether_miners_table")
    .saveAsTable("ether_miners_table")

}

