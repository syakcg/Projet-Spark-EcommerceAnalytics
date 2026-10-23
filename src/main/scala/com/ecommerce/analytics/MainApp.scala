package com.ecommerce.analytics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.typesafe.config.ConfigFactory
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object MainApp {

  // Fonction utilitaire pour mesurer, afficher l'heure et la durée d'une étape
  def measureTime[T](stepName: String)(block: => T): T = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val startTime = LocalDateTime.now()
    println(s"\n[${startTime.format(formatter)}] 🚀 Début de l'étape : $stepName")
    
    val startMillis = System.currentTimeMillis()
    val result = block
    val endMillis = System.currentTimeMillis()
    
    val endTime = LocalDateTime.now()
    val durationSec = (endMillis - startMillis) / 1000.0
    println(s"[${endTime.format(formatter)}] ✅ Fin de l'étape : $stepName (Durée : ${durationSec} secondes)")
    result
  }

  def main(args: Array[String]): Unit = {
    var spark: SparkSession = null

    //Récupération de l'argument (par défaut "all" si aucun argument n'est fourni)
    val step = if (args.length > 0) args(0).toLowerCase else "all"

    //Validation de l'argument et message d'aide si inconnu
    val validSteps = Set("ingestion", "transformation", "analytics", "all")
    if (!validSteps.contains(step)) {
      println(s"\n[ATTENTION] Argument inconnu : '$step'")
      println("==================================================")
      println("=== GUIDE D'AIDE - Valeurs acceptées :         ===")
      println("==================================================")
      println("  sbt \"run ingestion\"      -> Exécute uniquement l'ingestion")
      println("  sbt \"run transformation\" -> Exécute uniquement la transformation")
      println("  sbt \"run analytics\"      -> Exécute l'analytique et la sauvegarde")
      println("  sbt \"run all\"            -> Exécute tout le pipeline (Défaut)")
      println("==================================================")
      return 
    }

    try {
      //Initialisation de la SparkSession
      val config = ConfigFactory.load()
      val appName = if (config.hasPath("app.name")) config.getString("app.name") else "EcommerceAnalyticsApp"
      val masterUrl = if (config.hasPath("spark.master")) config.getString("spark.master") else "local[*]"

      spark = SparkSession.builder()
        .appName(s"$appName [$step]")
        .master(masterUrl)
        .config("spark.sql.shuffle.partitions", "10")
        .getOrCreate()

      spark.sparkContext.setLogLevel("WARN")
      println(s"=== [INIT] Session Spark démarrée pour l'étape : $step ===")

      //Variables de travail du pipeline
      var dfTransactions: org.apache.spark.sql.DataFrame = null
      var dfMerchants: org.apache.spark.sql.DataFrame = null
      var dfUsers: org.apache.spark.sql.DataFrame = null
      var dfTransMerchant: org.apache.spark.sql.DataFrame = null

      //INGESTION
      if (step == "ingestion" || step == "all") {
        measureTime("Ingestion") {
          dfTransactions = spark.read.option("header", "true").option("inferSchema", "true").csv("src/main/resources/data/transactions.csv")
          dfMerchants = spark.read.option("header", "true").option("inferSchema", "true").csv("src/main/resources/data/merchants.csv").withColumnRenamed("category", "merchant_category")
          dfUsers = spark.read.option("multiline", "true").json("src/main/resources/data/users.json")
          println(s"-> Chargés : ${dfTransactions.count()} transactions, ${dfMerchants.count()} marchands, ${dfUsers.count()} utilisateurs.")
        }
      }

      if (step == "transformation" || step == "analytics") {
        dfTransactions = spark.read.option("header", "true").option("inferSchema", "true").csv("src/main/resources/data/transactions.csv")
        dfMerchants = spark.read.option("header", "true").option("inferSchema", "true").csv("src/main/resources/data/merchants.csv").withColumnRenamed("category", "merchant_category")
      }

      //TRANSFORMATION
      if (step == "transformation" || step == "all") {
        measureTime("Transformation") {
          val dfTransactionsValid = dfTransactions.filter(col("amount").isNotNull && col("amount") > 0)
          dfTransMerchant = dfTransactionsValid
            .join(dfMerchants, Seq("merchant_id"), "left")
            .withColumn("commission_calculee", col("amount") * col("commission_rate"))
          println(s"-> Données transformées et jointes. Lignes valides : ${dfTransMerchant.count()}")
        }
      }

      //ANALYTICS & SAUVEGARDE
      if (step == "analytics" || step == "all") {
        if (dfTransMerchant == null) {
          val dfValid = spark.read.option("header", "true").option("inferSchema", "true").csv("src/main/resources/data/transactions.csv").filter(col("amount").isNotNull && col("amount") > 0)
          val dfM = spark.read.option("header", "true").option("inferSchema", "true").csv("src/main/resources/data/merchants.csv").withColumnRenamed("category", "merchant_category")
          dfTransMerchant = dfValid.join(dfM, Seq("merchant_id"), "left").withColumn("commission_calculee", col("amount") * col("commission_rate"))
        }

        measureTime("Analytique & Sauvegarde") {
          val rapportFinal = dfTransMerchant
            .groupBy("merchant_id", "name", "region", "merchant_category")
            .agg(
              sum("amount").as("ca_total"),
              count("transaction_id").as("nb_transactions"),
              countDistinct("user_id").as("nb_clients_uniques"),
              avg("amount").as("montant_moyen"),
              sum("commission_calculee").as("commission_totale")
            )

          println("=== RÉSULTATS ANALYTIQUES (Top 10) ===")
          rapportFinal.show(10, false)

          val outputCsvPath = "src/main/resources/output/merchant_report_csv"
          val outputParquetPath = "src/main/resources/output/merchant_report_parquet"

          rapportFinal.write.option("header", "true").mode("overwrite").csv(outputCsvPath)
          rapportFinal.write.mode("overwrite").parquet(outputParquetPath)
          println(s"-> Résultats sauvegardés en CSV et Parquet dans src/main/resources/output/")
        }
      }

      println(s"\n=== [SUCCÈS] Exécution globale de l'étape '$step' réussie ===")

    } catch {
      case ex: Throwable =>
        println(s"\n[ERREUR CRITIQUE] Une exception est survenue : ${ex.getMessage}")
        ex.printStackTrace()
    } finally {
      if (spark != null) {
        spark.stop()
        println("=== [FIN] Session Spark fermée proprement ===")
      }
    }
  }
}