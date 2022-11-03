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


