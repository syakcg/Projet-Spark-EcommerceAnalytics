package com.ecommerce.analytics

import org.apache.spark.sql.functions.udf
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.Try

// Structure de retour de l'UDF
case class TimeFeatureStruct(
  hour: Int,
  day_of_week: String,
  month: String,
  is_weekend: Int,
  day_period: String,
  is_working_hours: Int
)

object TimeFeatures {
  private val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")

  // Définition de l'UDF Spark
  val extractTimeFeaturesUDF = udf((ts: String) => {
    if (ts == null || ts.length != 14) {
      TimeFeatureStruct(-1, "Unknown", "Unknown", 0, "Unknown", 0)
    } else {
      Try {
        val dt = LocalDateTime.parse(ts, formatter)
        val hour = dt.getHour
        val dow = dt.getDayOfWeek.toString
        val month = dt.getMonth.toString
        val isWeekend = if (dow == "SATURDAY" || dow == "SUNDAY") 1 else 0

        val period = hour match {
          case h if h >= 6 && h < 12  => "Morning"
          case h if h >= 12 && h < 18 => "Afternoon"
          case h if h >= 18 && h < 22 => "Evening"
          case _                       => "Night"
        }

        val isWorkingHours = if (hour >= 9 && hour < 17 && isWeekend == 0) 1 else 0

        TimeFeatureStruct(hour, dow, month, isWeekend, period, isWorkingHours)
      }.getOrElse(TimeFeatureStruct(-1, "Unknown", "Unknown", 0, "Unknown", 0))
    }
  })
}