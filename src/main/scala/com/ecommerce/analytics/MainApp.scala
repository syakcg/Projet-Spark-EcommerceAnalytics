package com.ecommerce.analytics

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import com.typesafe.config.ConfigFactory
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import com.ecommerce.models._

object MainApp {

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

    val step = if (args.length > 0) args(0).toLowerCase else "all"

    val validSteps = Set("ingestion", "transformation", "rfm", "optimizations", "analytics", "all")
    if (!validSteps.contains(step)) {
      println(s"\n[ATTENTION] Argument inconnu : '$step'")
      return 
    }

    try {
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

      val ingestion = new DataIngestion()(spark)
      val transformation = new DataTransformation()(spark)

      var dsTransactions: Dataset[Transaction] = null
      var dsUsers: Dataset[User] = null
      var dsProducts: Dataset[Product] = null
      var dsMerchants: Dataset[Merchant] = null
      var dfTransFinal: DataFrame = null

      if (Set("ingestion", "transformation", "rfm", "optimizations", "analytics", "all").contains(step)) {
        dsTransactions = ingestion.loadTransactions()
        dsUsers = ingestion.loadUsers()
        dsProducts = ingestion.loadProducts()
        dsMerchants = ingestion.loadMerchants()
      }

      if (Set("transformation", "rfm", "optimizations", "analytics", "all").contains(step)) {
        val dfEnriched = transformation.enrichTransactionData(dsTransactions, dsUsers, dsProducts, dsMerchants)
        dfTransFinal = transformation.add7DaysRollingMetrics(dfEnriched)
      }

      if (step == "rfm" || step == "all") {
        measureTime("Analytique & Segmentation RFM (Membre C)") {
          println("--- EXÉCUTION DE LA SEGMENTATION RFM ET COHORTES ---")
          val rfmCalculated = dfTransFinal
            .groupBy("user_id")
            .agg(
              max("timestamp").as("last_purchase_date"),
              count("transaction_id").as("frequency"),
              sum("amount").as("monetary")
            )
          rfmCalculated.show(10, false)
        }
      }

      if (step == "optimizations" || step == "all") {
        measureTime("Optimisations Spark (Membre C)") {
          println("--- EXÉCUTION DES STRATÉGIES DE CACHE & BROADCAST ---")
          dfTransFinal.cache()
          val countTrans = dfTransFinal.count()
          println(s"[Optimisation Stockage] Nombre de transactions en cache : $countTrans")

          val dfBroadcastResult = dsTransactions
            .join(broadcast(dsMerchants.toDF()), Seq("merchant_id"), "left")
            .groupBy("region", "merchant_id")
            .agg(sum("amount").as("ca_region"))

          dfBroadcastResult.show(5, false)
          dfTransFinal.unpersist()
        }
      }

      if (step == "analytics" || step == "all") {
        measureTime("Rapport Global & Sauvegarde") {
          val rapportFinal = dfTransFinal
            .groupBy(dfTransFinal("region"), dfTransFinal("merchant_id"))
            .agg(
              sum("amount").as("ca_total"),
              count("transaction_id").as("nb_transactions"),
              avg("amount").as("montant_moyen")
            )

          println("=== RÉSULTATS FINAUX (Top 10) ===")
          rapportFinal.show(10, false)

          val outputCsvPath = "src/main/resources/output/merchant_report_csv"
          val outputParquetPath = "src/main/resources/output/merchant_report_parquet"

          rapportFinal.write.option("header", "true").mode("overwrite").csv(outputCsvPath)
          rapportFinal.write.mode("overwrite").parquet(outputParquetPath)
          println(s"-> Résultats exportés en CSV et Parquet dans src/main/resources/output/")
        }
      }

      println(s"\n=== [SUCCÈS] Exécution de l'étape '$step' terminée avec succès ===")

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