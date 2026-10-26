package com.ecommerce.utils

import org.apache.spark.sql.functions.udf
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.Try

// Structure contenant les caractéristiques temporelles extraites
case class TimeDetails(
  hour: Int,
  day_of_week: Int,
  month: Int,
  is_weekend: Boolean,
  day_period: String,
  is_working_hours: Boolean
)

object TimeFeatures {

  def extractTimeFeatures(timestampStr: String): TimeDetails = {
    val defaultTime = TimeDetails(0, 1, 1, false, "Nuit", false)
    if (timestampStr == null) return defaultTime
    
    // Tentative de parsing au format "yyyyMMddHHmmss" (utilisé dans le projet) ou format ISO
    val parsedOpt = Try {
      val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
      LocalDateTime.parse(timestampStr, formatter)
    }.orElse(Try {
      LocalDateTime.parse(timestampStr)
    })

    parsedOpt.map { dt =>
      val hour = dt.getHour
      val dayOfWeek = dt.getDayOfWeek.getValue // 1 (Lundi) à 7 (Dimanche)
      val month = dt.getMonthValue
      val isWeekend = dayOfWeek == 6 || dayOfWeek == 7
      
      val dayPeriod = hour match {
        case h if h >= 6 && h < 12  => "Matin"
        case h if h >= 12 && h < 18 => "Après-midi"
        case h if h >= 18 && h < 22 => "Soirée"
        case _                      => "Nuit"
      }
      
      val isWorkingHours = !isWeekend && hour >= 8 && hour < 18

      TimeDetails(hour, dayOfWeek, month, isWeekend, dayPeriod, isWorkingHours)
    }.getOrElse(defaultTime)
  }

  // UDF Spark exposée pour la transformation
  val extractTimeFeaturesUDF = udf((ts: String) => extractTimeFeatures(ts))
}