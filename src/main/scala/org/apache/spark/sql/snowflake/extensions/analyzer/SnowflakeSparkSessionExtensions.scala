package org.apache.spark.sql.snowflake.extensions.analyzer

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.snowflake.extensions.analyzer.extension.ResolveSnowflakeRelations
import org.slf4j.LoggerFactory

final class SnowflakeSparkSessionExtensions extends (SparkSessionExtensions => Unit) {
  
  private val log = LoggerFactory.getLogger(getClass)

  override def apply(extensions: SparkSessionExtensions): Unit = {
    log.info("Registering Snowflake JDBC fallback analyzer rule")
    extensions.injectResolutionRule(ResolveSnowflakeRelations(_))
  }
}
