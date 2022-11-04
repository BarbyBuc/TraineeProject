# TraineeProject
## Repository for the Big Data Engineer Trainee Program 

This EDA is based in the dataset of Ethereum Blockchain from Kaggle 

~~~
https://www.kaggle.com/datasets/buryhuang/ethereum-blockchain
~~~
I unified the _blocks.csv_ and the _transactions.csv_ files of the dataset resulting in a DataFrame of 43431189 rows with the following 19 fields or columns:

    - hash: unique code for identification of cryptocurrencies transactions
    - nonce: number of transactions made by the sender prior this one
    - block_hash: hash of the block where this transaction was in
    - block_number: block number where this transaction was in
    - transaction_index: the transaction index position in the block
    - from_address: address of the sender
    - to_address: address of the receiver (null when its a contract creation transaction)
    - value: transferred value in wei (smallest denomination of ether)
    - gas: gas provided by the sender
    - gas_price: gas price provided by the sender in wei
    - input: the data sended with the transaction
    - block_timestamp: timestamp of the block where this transaction was in (in unix time) 
    - parent_hash: hash of the parent block
    - miner: the address of the beneficiary to whom the mining rewards were given
    - difficulty: the difficulty of the block (shows how many times the miner should calculate the hash function in order to find the block)
    - total_difficulty: total difficulty of the chain until this block
    - size: the size of this block in bytes
    - gas_limit: the maximum gas allowed in this block
    - gas_used: the total used gas by all transactions in this block

Each row corresponds to a single transaction in Ethereum blockchain

Every report was saved using this
~~~
  report.write
     .mode(SaveMode.Overwrite)
     .option("path","Reports/report_table")
     .saveAsTable("report_table")
~~~
---



#### Reading and formatting data
Reading _.csv_ as DF
~~~
val dataframe = spark.read
    .option("inferSchema", "true")
    .option("header","true")
    .option("delimiter", ",")
    .csv("[path]")
~~~

Then I performed an inner join and dropped the unnecessary, repeated columns finally reaching this schema:
~~~
root
 |-- hash: string (nullable = true)
 |-- nonce: integer (nullable = true)
 |-- block_hash: string (nullable = true)
 |-- block_number: integer (nullable = true)
 |-- transaction_index: integer (nullable = true)
 |-- from_address: string (nullable = true)
 |-- to_address: string (nullable = true)
 |-- value: decimal(24,0) (nullable = true)
 |-- gas: integer (nullable = true)
 |-- gas_price: long (nullable = true)
 |-- input: string (nullable = true)
 |-- block_timestamp: integer (nullable = true)
 |-- parent_hash: string (nullable = true)
 |-- miner: string (nullable = true)
 |-- difficulty: long (nullable = true)
 |-- total_difficulty: decimal(23,0) (nullable = true)
 |-- size: integer (nullable = true)
 |-- gas_limit: integer (nullable = true)
 |-- gas_used: integer (nullable = true)
~~~
In order to have a better performance and speed in my computer (4 cores and 8GB RAM) I transformed the _.csv_ in _.parquet_ files using this
~~~
dataframe.write
    .mode(SaveMode.Overwrite)
    .parquet("[path]")
~~~
And load the final DF
~~~
ethereumDF.spark.read.parquet("[path]")
~~~

---

#### Check for null and duplicates values

- Null values
>~~~
>ethereumDF.select(
>    ethereumDF.columns.map(
>      x => count(
>        when(col(x).isNull || col(x) === "" || col(x).isNaN, x)
>      ).alias(x)
>    ): _*
>  )
>+----+-----+----------+------------+-----------------+------------+----------+-----+---+---------+-----+---------------+-----------+-----+----------+----------------+----+---------+--------+
>|hash|nonce|block_hash|block_number|transaction_index|from_address|to_address|value|gas|gas_price|input|block_timestamp|parent_hash|miner|difficulty|total_difficulty|size|gas_limit|gas_used|
>+----+-----+----------+------------+-----------------+------------+----------+-----+---+---------+-----+---------------+-----------+-----+----------+----------------+----+---------+--------+
>|   0|    0|         0|           0|                0|           0|    136835|    0|  0|        0|    0|              0|          0|    0|         0|               0|   0|        0|       0|
>+----+-----+----------+------------+-----------------+------------+----------+-----+---+---------+-----+---------------+-----------+-----+----------+----------------+----+---------+--------+
>~~~
>[Null table report](Reports/null_table)
>
>
- Duplicate values
>~~~
>val duplicateReportDF = ethereumDF.select(
>   count("hash").alias("total_rows"),
>   countDistinct("hash").alias("unique_rows")
>)
>+----------+-----------+
>|total_rows|unique_rows|
>+----------+-----------+
>|  43431189|   43431189|
>+----------+-----------+
>~~~
>
> There isn't duplicated rows

---

### Manipulation and analysis of Data

- About time

>I converted the timestamp column from unix to timestamp and ordered df by this column to find the time period in which my data was collected
>
>~~~
>  val ethereumDFwithTime = ethereumDF.withColumn("block_timestamp", from_unixtime(col("block_timestamp")))
>    .orderBy(col("block_timestamp"))
>~~~
>
>
>After that I showed the first row :
> ~~~
>   val firstTimeDF = ethereumDFwithTime.select(col("block_timestamp"))
>   firstTimeDF.show(1)
> 
>   +-------------------+
>   |    block_timestamp|
>   +-------------------+
>   |2019-11-25 15:27:24|
>   +-------------------+
>   only showing top 1 row
>
> ~~~
> and printed the last one as a List to avoid doing a new DF descend ordered:
> ~~~
>   val lastTime = firstTimeDF.tail(1).toList
>
>   println(lastTime)
> 
>   List([2020-02-07 23:24:43])
> ~~~
- Type of transactions
> First I created a DF marking the type of transaction in other column
> ~~~
>   val contractsDF = ethereumDF.withColumn(
>       "type", when(col("to_address").isNull, "Contract")
>       .otherwise("Regular")
>   )
> ~~~
> After that, I found the contract percentage
> ~~~
>   val contractsPercentage = contractsDF.groupBy("contract").agg(
>       count("hash").alias("count"),
>       round(count("hash") / contractsDF.count() * 100,2).alias("percentage")
>   )
>   +--------+--------+----------+
>   |    type|   count|percentage|
>   +--------+--------+----------+
>   |Contract|  136835|      0.32|
>   | Regular|43294354|     99.68|
>   +--------+--------+----------+
> ~~~
> [Contract table report](Reports/contract_table)
>
> Now can focus in Regular transactions only dealing with the nulls
> ~~~
>   val regularTransactionsDF = ethereumDF.na.drop()
> ~~~
> 
> Count information about regular transactions:
> ~~~
>   val countsDF = regularTransactionsDF.agg(
>       count("hash").alias("total_transactions"),
>       countDistinct("block_number").alias("blocks"),
>       countDistinct("from_address").alias("senders"),
>       countDistinct("miner").alias("miners"),
>       sum(when(col("nonce") === "0",1).otherwise(0).alias("new_wallets"))
>   )
>
>   +------------------+------+-------+------+-----------------------------------------------------------+
>   |total_transactions|blocks|senders|miners|sum(CASE WHEN (nonce = 0) THEN 1 ELSE 0 END AS new_wallets)|
>   +------------------+------+-------+------+-----------------------------------------------------------+
>   |          43294354|424418|5005829|   183|                                                    3084376|
>   +------------------+------+-------+------+-----------------------------------------------------------+
> ~~~
> [Counts table report](Reports/counts_table)

- About Tokens

> The ethereum blockchain not only work with Ether, there are other tokens been send and received trough this chain.
> These are the numbers:
> ~~~
>    val tokensDF = regularTransactionsDF.withColumn(
>    "token", when(col("value") === 0, "Other").otherwise("Ether")
>    )
>    val totalSpentGas = tokensDF.agg(sum("gas_price")).collect()(0).getLong(0)
>    val tokensObservations = tokensDF.groupBy("token").agg(
>       count("hash").alias("quantity"),
>       round((count("hash") / tokensDF.count() * 100), 2).alias("%quantity"),
>       sum("gas_price").alias("gas_spent"),
>       round((sum("gas_price") / totalSpentGas * 100), 2).alias("%gas_spent")
>    )
>   +-----+--------+---------+------------------+----------+
>   |token|quantity|%quantity|         gas_spent|%gas_spent|
>   +-----+--------+---------+------------------+----------+
>   |Other|28181677|    65.09|335531761981078792|     65.77|
>   |Ether|15112677|    34.91|174595403097712092|     34.23|
>   +-----+--------+---------+------------------+----------+
> ~~~
>[Tokens table report](Reports/tokens_table)
>
>The movements on the chain are almost 2/3 of tokens different from ether
- About Miners
> I chose to focus in what amount of this transactions are from miners:
>~~~
>  val minersTransactions = tokensDF.withColumn(
>      "minerTr", col("from_address")
>      .isin(minerList:_*))
>~~~
> After identity what transactions were made by miners, I calculate the percentage
> ~~~
>   val minersTransactions2 = minersTransactions.groupBy("token","minerTr").agg(
>    count("hash").alias("quantity"),
>    round((count("hash") / minersTransactions.count() * 100), 2).alias("%quantity")
>   ).orderBy("token","quantity")
> 
>   +-----+-------+--------+---------+
>   |token|minerTr|quantity|%quantity|
>   +-----+-------+--------+---------+
>   |Ether|   true| 1981307|     4.58|
>   |Ether|  false|13131370|    30.33|
>   |Other|   true|      14|      0.0|
>   |Other|  false|28181663|    65.09|
>   +-----+-------+--------+---------+
> ~~~
>[Miners table report](Reports/miners_table)
>
>Finally I focus on the Ether Miners 
> ~~~
>   val totalEtherTransactions = minersTransactions2.where("token == 'Ether'")
>       .agg(sum("quantity"))
>       .collect()(0).getLong(0)
>   val minersEtherTransactions = minersTransactions2.where("token == 'Ether'")
>       .withColumn("%final", round(col("quantity")/totalEtherTransactions*100,2))
>       .drop("%quantity")
>   +-----+-------+--------+------+
>   |token|minerTr|quantity|%final|
>   +-----+-------+--------+------+
>   |Ether|   true| 1981307| 13.11|
>   |Ether|  false|13131370| 86.89|
>   +-----+-------+--------+------+
> ~~~
> [Ether Miners Report](Reports/ether_miners_table)


