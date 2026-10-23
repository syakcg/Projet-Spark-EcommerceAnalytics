package com.ecommerce.analytics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Analytics {

  //Question 4.1, 4.2 et 4.3 appelées dans le main 
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder()
      .appName("Projet-Spark-EcommerceAnalytics")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //QUESTION 4.1 : Rapport détaillé par marchand
    val dfTransactions = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/transactions.csv")

    val dfMerchants = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/merchants.csv")
      .withColumnRenamed("category", "merchant_category")

    val dfUsers = spark.read
      .option("multiline", "true")
      .json("src/main/resources/data/users.json")

    val dfTransMerchant = dfTransactions
      .join(dfMerchants, Seq("merchant_id"), "left")
      .withColumn("commission_calculee", col("amount") * col("commission_rate"))

    val rapportBase = dfTransMerchant
      .groupBy("merchant_id", "name", "region", "merchant_category")
      .agg(
        sum("amount").as("ca_total"),
        count("transaction_id").as("nb_transactions"),
        countDistinct("user_id").as("nb_clients_uniques"),
        avg("amount").as("montant_moyen"),
        sum("commission_calculee").as("commission_totale")
      )

    val fenetreRegion = Window.partitionBy("region").orderBy(col("ca_total").desc)
    val fenetreCategorie = Window.partitionBy("merchant_category").orderBy(col("ca_total").desc)

    val rapportAvecClassement = rapportBase
      .withColumn("classement_region", rank().over(fenetreRegion))
      .withColumn("classement_categorie", rank().over(fenetreCategorie))

    val dfTransUser = dfTransactions.join(dfUsers, Seq("user_id"), "left")

    val dfTrancheAge = dfTransUser
      .withColumn("tranche_age",
        when(col("age") < 25, "Moins de 25 ans")
          .when(col("age") >= 25 && col("age") <= 40, "25-40 ans")
          .when(col("age") > 40 && col("age") <= 55, "40-55 ans")
          .otherwise("Plus de 55 ans")
      )
      .groupBy("merchant_id", "tranche_age")
      .agg(count("transaction_id").as("nb_ventes_tranche"))

    println("=== RAPPORT DETAILLE ET CLASSEMENT DES MARCHANDS ===")
    rapportAvecClassement.show(10, false)

    println("=== REPARTITION DES VENTES PAR TRANCHE D'AGE ===")
    dfTrancheAge.show(10, false)


    //QUESTION 4.2 : Analyse de cohortes utilisateurs
    CohortAnalytics.runCohortAnalysis(spark)

    //QUESTION 4.3 : Segmentation RFM des clients (Bonus)
    RFMAnalytics.runRFMAnalysis(spark)

    spark.stop()
  }
}

object CohortAnalytics {
  def runCohortAnalysis(spark: SparkSession): Unit = {
    import spark.implicits._

    // Chargement des transactions
    val dfTransactions = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/transactions.csv")

    //Préparation des dates et nettoyage
    val dfTrans = dfTransactions
      .withColumn("date", to_date(col("transaction_date"), "yyyy-MM-dd"))

    //Trouver le mois de première transaction pour chaque utilisateur (cohort_month)
    val dfFirstTrans = dfTrans
      .groupBy("user_id")
      .agg(min("date").as("first_date"))
      .withColumn("cohort_month", date_format(col("first_date"), "yyyy-MM"))
      .select("user_id", "cohort_month")

    //Associer chaque transaction à sa cohorte et calculer le period_index
    val dfCohortTrans = dfTrans
      .join(dfFirstTrans, Seq("user_id"), "inner")
      .withColumn("trans_year", year(col("date")))
      .withColumn("trans_month_num", month(col("date")))
      .withColumn("cohort_year", year(to_date(concat(col("cohort_month"), lit("-01")), "yyyy-MM-dd")))
      .withColumn("cohort_month_num", month(to_date(concat(col("cohort_month"), lit("-01")), "yyyy-MM-dd")))
      .withColumn("period_index", 
        (col("trans_year") - col("cohort_year")) * 12 + (col("trans_month_num") - col("cohort_month_num"))
      )

    //Calcul de la taille initiale de chaque cohorte
    val dfCohortSize = dfCohortTrans
      .filter(col("period_index") === 0)
      .groupBy("cohort_month")
      .agg(countDistinct("user_id").as("initial_size"))

    //Production de la matrice de rétention
    val dfRetentionRaw = dfCohortTrans
      .groupBy("cohort_month", "period_index")
      .agg(countDistinct("user_id").as("active_users"))

    val dfRetentionMatrix = dfRetentionRaw
      .join(dfCohortSize, Seq("cohort_month"), "inner")
      .withColumn("retention_rate_pct", round((col("active_users") * 100.0) / col("initial_size"), 2))
      .orderBy("cohort_month", "period_index")

    println("=== MATRICE DE RETENTION DES UTILISATEURS ===")
    dfRetentionMatrix.show(30, false)

    //Identification de la cohorte présentant la meilleure rétention à 3 mois
    println("=== MEILLEURE COHORTE A 3 MOIS (period_index = 3) ===")
    val bestCohort3Months = dfRetentionMatrix
      .filter(col("period_index") === 3)
      .orderBy(col("retention_rate_pct").desc)
      .limit(1)
    bestCohort3Months.show(false)

    //BONUS : Chiffre d'affaires par cohorte et période, ainsi que le revenu moyen par utilisateur
    val dfCohortRevenue = dfCohortTrans
      .groupBy("cohort_month", "period_index")
      .agg(
        sum("amount").as("total_revenue"),
        countDistinct("user_id").as("active_buyers")
      )
      .join(dfCohortSize, Seq("cohort_month"), "inner")
      .withColumn("arpu", round(col("total_revenue") / col("active_buyers"), 2))
      .orderBy("cohort_month", "period_index")

    println("=== BONUS : CHIFFRE D'AFFAIRES ET ARPU PAR COHORTE ===")
    dfCohortRevenue.show(30, false)
  }
}

object RFMAnalytics {
  def runRFMAnalysis(spark: SparkSession): Unit = {
    import spark.implicits._

    //Chargement des données
    val dfTransactions = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/transactions.csv")

    val dfUsers = spark.read
      .option("multiline", "true")
      .json("src/main/resources/data/users.json")

    //Trouver la date la plus récente du jeu de données global pour calculer la récence
    val maxDateRow = dfTransactions.select(max(to_date(col("transaction_date"), "yyyy-MM-dd"))).collect()(0)
    val maxDate = maxDateRow.getDate(0)

    //Calcul des trois indicateurs RFM de base par utilisateur
    val dfRFMBase = dfTransactions
      .withColumn("date", to_date(col("transaction_date"), "yyyy-MM-dd"))
      .groupBy("user_id")
      .agg(
        datediff(lit(maxDate), max("date")).as("recency"), 
        count("transaction_id").as("frequency"),           
        sum("amount").as("monetary")                       
      )

    //Attribution des scores de 1 à 5 via les quintiles
    // Note : Pour la récence, moins il y a de jours écoulés, meilleur est le score (donc tri descendant)
    // Pour la fréquence et le montant, plus c'est élevé, meilleur est le score (tri ascendant)
    val windowR = Window.orderBy(col("recency").desc)
    val windowF = Window.orderBy(col("frequency").asc)
    val windowM = Window.orderBy(col("monetary").asc)

    val dfRFMScores = dfRFMBase
      .withColumn("R_score", ntile(5).over(windowR))
      .withColumn("F_score", ntile(5).over(windowF))
      .withColumn("M_score", ntile(5).over(windowM))

    //Déduction des segments métier selon les scores RFM
    val dfRFMSegments = dfRFMScores.withColumn("rfm_segment",
      when(col("R_score") >= 4 && col("F_score") >= 4 && col("M_score") >= 4, "Champions")
        .when(col("F_score") >= 3 && col("M_score") >= 3, "Clients fidèles")
        .when(col("R_score") <= 2 && col("F_score") >= 3, "À risque")
        .when(col("R_score") <= 2 && col("F_score") <= 2, "Perdus")
        .when(col("R_score") >= 4 && col("F_score") <= 2, "Nouveaux")
        .otherwise("Réguliers")
    )

    println("=== APERCU DES SCORES ET SEGMENTS RFM ===")
    dfRFMSegments.show(20, false)

    //Jointure avec users.json pour récupérer le customer_segment déclaré
    val dfJoined = dfRFMSegments.join(dfUsers, Seq("user_id"), "left")

    //Production du tableau croisé entre le segment RFM et le customer_segment
    println("=== TABLEAU CROISE : SEGMENT RFM vs CUSTOMER_SEGMENT ===")
    val dfCrossTab = dfJoined.stat.crosstab("rfm_segment", "customer_segment")
    dfCrossTab.show(false)
  }
}