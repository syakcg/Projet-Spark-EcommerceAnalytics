package com.ecommerce.analytics

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import com.ecommerce.models._
import com.ecommerce.utils.ConfigLoader

/* Cette classe vérifie les règles métier pour chaque dataset,
 capture les enregistrements invalides en leur associant la raison du rejet (rejection_reason),
 et génère le rapport de qualité global. */

class DataValidation(implicit spark: SparkSession) {
  import spark.implicits._

  // 1. Validation des transactions (amount > 0 ET timestamp de 14 caractères)
  def validateTransactions(ds: Dataset[Transaction]): (Dataset[Transaction], DataFrame) = {
    val isValid = col("amount") > 0 && length(col("timestamp")) === 14

    val valid = ds.filter(isValid)
    val rejected = ds.filter(!isValid).withColumn("rejection_reason",
      when(col("amount") <= 0, "Montant <= 0")
        .when(length(col("timestamp")) =!= 14, "Format timestamp invalide")
        .otherwise("Incohérence générale")
    )

    (valid, rejected.toDF())
  }

  // 2. Validation des utilisateurs (âge entre 16 et 100 ET annual_income > 0)
  def validateUsers(ds: Dataset[User]): (Dataset[User], DataFrame) = {
    val isValid = col("age").between(ConfigLoader.minAge, ConfigLoader.maxAge) && col("annual_income") > 0

    val valid = ds.filter(isValid)
    val rejected = ds.filter(!isValid).withColumn("rejection_reason",
      when(!col("age").between(ConfigLoader.minAge, ConfigLoader.maxAge), "Âge hors bornes (16-100)")
        .when(col("annual_income") <= 0, "Revenu <= 0")
        .otherwise("Incohérence utilisateur")
    )

    (valid, rejected.toDF())
  }

  // 3. Validation des produits (price > 0 ET rating entre 1.0 et 5.0)
  def validateProducts(ds: Dataset[Product]): (Dataset[Product], DataFrame) = {
    val isValid = col("price") > 0 && col("rating").between(ConfigLoader.minRating, ConfigLoader.maxRating)

    val valid = ds.filter(isValid)
    val rejected = ds.filter(!isValid).withColumn("rejection_reason",
      when(col("price") <= 0, "Prix <= 0")
        .when(!col("rating").between(ConfigLoader.minRating, ConfigLoader.maxRating), "Note hors intervalle (1-5)")
        .otherwise("Incohérence produit")
    )

    (valid, rejected.toDF())
  }

  // 4. Validation des marchands (commission_rate entre 0.0 et 1.0)
  def validateMerchants(ds: Dataset[Merchant]): (Dataset[Merchant], DataFrame) = {
    val isValid = col("commission_rate").between(0.0, 1.0)

    val valid = ds.filter(isValid)
    // Utilisation de lit(...) pour transformer la chaîne en Column Spark
    val rejected = ds.filter(!isValid).withColumn("rejection_reason", lit("Taux de commission invalide"))

    (valid, rejected.toDF())
  }

  // 5. Génération du Bilan de Qualité
  def generateQualityReport(metrics: List[(String, Long, Long, Long, DataFrame)]): DataFrame = {
    val rows = metrics.map { case (name, lues, valides, rejetees, dfRaw) =>
      val taux = if (lues > 0) BigDecimal((rejetees.toDouble / lues.toDouble) * 100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble else 0.0
      val nullCols = dfRaw.columns.map(c => sum(when(col(c).isNull, 1).otherwise(0))).reduce(_ + _)
      val totalNulls = dfRaw.select(nullCols).first().getAs[Long](0)

      QualityReportRow(name, lues, valides, rejetees, taux, totalNulls)
    }

    rows.toDF()
  }
}