package com.ecommerce.analytics

import org.apache.spark.sql.SparkSession
import com.ecommerce.utils.ConfigLoader

object TestMemberA {
  def main(args: Array[String]): Unit = {

    // Initialisation de la SparkSession en mode local
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("TestMembreA")
      .master(ConfigLoader.sparkMaster)
      .getOrCreate()

    println("==================================================")
    println("=== TEST TECHNIQUE : MODULE MEMBRE A ===")
    println("==================================================")

    val ingestion = new DataIngestion()
    val validation = new DataValidation()

    // 1. Ingestion
    println("\n[1/3] Ingestion des datasets...")
    val rawTxs = ingestion.loadTransactions()
    val rawUsers = ingestion.loadUsers()
    val rawProducts = ingestion.loadProducts()
    val rawMerchants = ingestion.loadMerchants()

    println(s" - Transactions lues : ${rawTxs.count()}")
    println(s" - Utilisateurs lus   : ${rawUsers.count()}")
    println(s" - Produits lus       : ${rawProducts.count()}")
    println(s" - Marchands lus      : ${rawMerchants.count()}")

    // 2. Validation
    println("\n[2/3] Application des règles de validation...")
    val (validTxs, rejTxs) = validation.validateTransactions(rawTxs)
    val (validUsers, rejUsers) = validation.validateUsers(rawUsers)
    val (validProducts, rejProducts) = validation.validateProducts(rawProducts)
    val (validMerchants, rejMerchants) = validation.validateMerchants(rawMerchants)

    // 3. Rapport de qualité
    println("\n[3/3] Génération du rapport de qualité...")
    val qualityReport = validation.generateQualityReport(List(
      ("Transactions", rawTxs.count(), validTxs.count(), rejTxs.count(), rawTxs.toDF()),
      ("Utilisateurs", rawUsers.count(), validUsers.count(), rejUsers.count(), rawUsers.toDF()),
      ("Produits", rawProducts.count(), validProducts.count(), rejProducts.count(), rawProducts.toDF()),
      ("Marchands", rawMerchants.count(), validMerchants.count(), rejMerchants.count(), rawMerchants.toDF())
    ))

    println("\n==================================================")
    println("=== RAPPORT DE QUALITÉ DES DONNÉES ===")
    println("==================================================")
    qualityReport.show(false)

    println("\nExemple de rejets enregistrés pour les transactions :")
    rejTxs.select("transaction_id", "amount", "timestamp", "rejection_reason").show(5, false)

    spark.stop()
  }
}