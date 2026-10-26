package com.ecommerce.analytics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object SparkOptimizations {
  def main(args: Array[String]): Unit = {

    //Initialisation de la SparkSession avec configuration du shuffle
    val spark = SparkSession.builder()
      .appName("Part5_OptimisationsSpark")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "10") 
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    //Chargement des données de base
    val dfTransactions = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/transactions.csv")

    val dfMerchants = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/merchants.csv")
      .withColumnRenamed("category", "merchant_category")


    //QUESTION 5.1 : Optimisation du stockage
    println("--- EXÉCUTION QUESTION 5.1 : Optimisation du stockage ---")
    
    // Utilisation de cache()
    dfTransactions.cache()
    val totalTrans = dfTransactions.count()
    println(s"Nombre de transactions (via cache) : $totalTrans")

    // Utilisation de persist
    val dfTransMerchantPersist = dfTransactions
      .join(dfMerchants, Seq("merchant_id"), "left")
      .withColumn("commission_calculee", col("amount") * col("commission_rate"))

    dfTransMerchantPersist.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val caGlobal = dfTransMerchantPersist.agg(sum("amount")).collect()(0).getDouble(0)
    println(s"Chiffre d'affaires global calculé : $caGlobal")

    //Utilisation de unpersist()
    dfTransactions.unpersist()
    dfTransMerchantPersist.unpersist()

    //QUESTION 5.2 : Optimisation des jointures
    println("--- EXÉCUTION QUESTION 5.2 : Optimisation des jointures ---")

    val dfTransBroadcast = dfTransactions
      .join(broadcast(dfMerchants), Seq("merchant_id"), "left")
      .withColumn("commission_calculee", col("amount") * col("commission_rate"))

    val rapportOptimise = dfTransBroadcast
      .groupBy("region", "merchant_category")
      .agg(
        sum("amount").as("ca_par_categorie"),
        count("transaction_id").as("nb_transactions")
      )
      .orderBy(col("ca_par_categorie").desc)

    println("=== APERÇU DES RÉSULTATS AVEC JOINTURE OPTIMISÉE (BROADCAST) ===")
    rapportOptimise.show(10, false)

    println("Toutes les optimisations Spark ont été exécutées avec succès.")
    spark.stop()
  }
}