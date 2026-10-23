package com.ecommerce.utils

// Utilisez _root_ devant com.typesafe
import _root_.com.typesafe.config.{Config, ConfigFactory}

object ConfigLoader {
  private val config: Config = ConfigFactory.load()

  val appName: String = config.getString("app.name")
  val sparkMaster: String = config.getString("app.spark.master")
  val shufflePartitions: Int = config.getInt("app.spark.shuffle.partitions")

  val transactionsPath: String = config.getString("app.data.input.transactions")
  val usersPath: String        = config.getString("app.data.input.users")
  val productsPath: String     = config.getString("app.data.input.products")
  val merchantsPath: String    = config.getString("app.data.input.merchants")
  val outputPath: String       = config.getString("app.data.output.path")

  val minAge: Int       = config.getInt("app.validation.user.min-age")
  val maxAge: Int       = config.getInt("app.validation.user.max-age")
  val minRating: Double = config.getDouble("app.validation.product.min-rating")
  val maxRating: Double = config.getDouble("app.validation.product.max-rating")
}