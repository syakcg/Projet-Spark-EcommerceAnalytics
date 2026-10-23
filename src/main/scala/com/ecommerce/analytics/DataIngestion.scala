package com.ecommerce.analytics

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types._
import com.ecommerce.models._
import com.ecommerce.utils.ConfigLoader
import scala.util.{Try, Success, Failure}

/* Cette classe gère le chargement des 4 fichiers
d'entrée (transactions.csv, users.json, products.parquet et merchants.csv)
en sécurisant la lecture avec des blocs Try/Catch et en appliquant
un typage fort (Dataset[T]). */

class DataIngestion(implicit spark: SparkSession) {
  import spark.implicits._

  // 1. Ingestion des transactions (CSV avec schéma explicite)
  def loadTransactions(): Dataset[Transaction] = {
    Try {
      val schema = StructType(Array(
        StructField("transaction_id", StringType, true),
        StructField("user_id", StringType, true),
        StructField("product_id", StringType, true),
        StructField("merchant_id", StringType, true),
        StructField("amount", DoubleType, true),
        StructField("payment_method", StringType, true),
        StructField("timestamp", StringType, true)
      ))

      spark.read
        .option("header", "true")
        .schema(schema)
        .csv(ConfigLoader.transactionsPath)
        .as[Transaction]
    } match {
      case Success(ds) => ds
      case Failure(e)  => throw new RuntimeException(s"Erreur lors du chargement des transactions : ${e.getMessage}")
    }
  }

  // 2. Ingestion des utilisateurs (JSON)
  def loadUsers(): Dataset[User] = {
    Try {
      spark.read
        .json(ConfigLoader.usersPath)
        .as[User]
    } match {
      case Success(ds) => ds
      case Failure(e)  => throw new RuntimeException(s"Erreur lors du chargement des utilisateurs : ${e.getMessage}")
    }
  }

  // 3. Ingestion des produits (Parquet)
  def loadProducts(): Dataset[Product] = {
    Try {
      spark.read
        .parquet(ConfigLoader.productsPath)
        .as[Product]
    } match {
      case Success(ds) => ds
      case Failure(e)  => throw new RuntimeException(s"Erreur lors du chargement des produits : ${e.getMessage}")
    }
  }

  // 4. Ingestion des marchands (CSV avec inférence de schéma)
  def loadMerchants(): Dataset[Merchant] = {
    Try {
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(ConfigLoader.merchantsPath)
        .as[Merchant]
    } match {
      case Success(ds) => ds
      case Failure(e)  => throw new RuntimeException(s"Erreur lors du chargement des marchands : ${e.getMessage}")
    }
  }
}