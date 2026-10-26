package com.ecommerce.analytics

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import com.ecommerce.models._
import com.ecommerce.utils.ConfigLoader
import scala.util.{Try, Success, Failure}


class DataIngestion(implicit spark: SparkSession) {
  import spark.implicits._

  //Ingestion des transactions (CSV avec schéma explicite)
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

  //Ingestion des utilisateurs (JSON) avec cast explicite de l'âge
  def loadUsers(): Dataset[User] = {
    Try {
      spark.read
        .json(ConfigLoader.usersPath)
        .withColumn("age", col("age").cast("integer"))
        .as[User]
    } match {
      case Success(ds) => ds
      case Failure(e)  => throw new RuntimeException(s"Erreur lors du chargement des utilisateurs : ${e.getMessage}")
    }
  }

  //Ingestion des produits (Parquet)
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

  //Ingestion des marchands (CSV avec inférence de schéma)
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