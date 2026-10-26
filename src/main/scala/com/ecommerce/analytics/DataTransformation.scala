package com.ecommerce.analytics

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.ecommerce.models._

class DataTransformation(implicit spark: SparkSession) {
  import spark.implicits._

  def enrichTransactionData(
    txs: Dataset[Transaction],
    users: Dataset[User],
    products: Dataset[Product],
    merchants: Dataset[Merchant]
  ): DataFrame = {

    // 1. Application de l'UDF temporelle et aplatissement des colonnes
    val txsWithTime = txs
      .withColumn("time_features", TimeFeatures.extractTimeFeaturesUDF(col("timestamp")))
      .select(
        col("*"),
        col("time_features.hour").as("hour"),
        col("time_features.day_of_week").as("day_of_week"),
        col("time_features.month").as("month"),
        col("time_features.is_weekend").as("is_weekend"),
        col("time_features.day_period").as("day_period"),
        col("time_features.is_working_hours").as("is_working_hours")
      )
      .drop("time_features")

    // 2. Jointures d'enrichissement (Left Joins)
    val enriched = txsWithTime
      .join(users, Seq("user_id"), "left")
      .join(products, Seq("product_id"), "left")
      .join(merchants, Seq("merchant_id"), "left")

    // 3. Fenêtrages analytiques par utilisateur
    val userOrderWindow = Window.partitionBy("user_id").orderBy("timestamp")
    val userGlobalWindow = Window.partitionBy("user_id")

    val withWindowMetrics = enriched
      .withColumn("transaction_rank", rank().over(userOrderWindow))
      .withColumn("total_user_transactions", count("transaction_id").over(userGlobalWindow))

    // 4. Segmentation par tranche d'âge
    withWindowMetrics.withColumn("age_group",
      when(col("age") < 25, "Jeune")
      .when(col("age").between(25, 44), "Adulte")
      .when(col("age").between(45, 64), "Âge Moyen")
      .otherwise("Senior")
    )
  }

  // 5. Calcul du montant cumulé sur une fenêtre glissante de 7 jours (7 * 24 * 3600 = 604800 s)
  def add7DaysRollingMetrics(df: DataFrame): DataFrame = {
    val timestampInSec = to_timestamp(col("timestamp"), "yyyyMMddHHmmss").cast("long")
    val window7Days = Window.partitionBy("user_id").orderBy(timestampInSec).rangeBetween(-604800, 0)

    df.withColumn("timestamp_sec", timestampInSec)
      .withColumn("rolling_7d_amount", sum("amount").over(window7Days))
  }
}