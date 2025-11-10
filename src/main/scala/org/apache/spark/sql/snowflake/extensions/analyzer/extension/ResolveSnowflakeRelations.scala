package org.apache.spark.sql.snowflake.extensions.analyzer.extension

import net.snowflake.spark.snowflake.DefaultSource
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, UnresolvedRelation}
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogManager, Identifier, LookupCatalog}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.snowflake.catalog.FGACForbiddenException
import org.slf4j.LoggerFactory

case class ResolveSnowflakeRelations(
    spark: SparkSession) extends Rule[LogicalPlan] with LookupCatalog {

  protected val logger = LoggerFactory.getLogger(getClass)
  protected lazy val catalogManager: CatalogManager = spark.sessionState.catalogManager
  private val snowflakeSource = new DefaultSource()
  
  private val FGAC_JDBC_FALLBACK_ENABLED_KEY = "spark.snowflake.extensions.fgacJdbcFallback.enabled"

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!isFallbackEnabled) {
      return plan
    }
    
    plan transformUp {
      case u: UnresolvedRelation =>
        tryResolveUnresolvedRelation(u)
      
      case i: InsertIntoStatement =>
        i.table match {
          case u: UnresolvedRelation =>
            tryResolveUnresolvedRelation(u) match {
              case resolved if resolved ne u =>
                logger.debug(
                  "Resolving INSERT target {} via Snowflake JDBC fallback", getFullTableName(u))
                i.copy(table = resolved)
              case _ => i
            }
          case _ => i
        }
    }
  }
  
  private def isFallbackEnabled: Boolean = {
    spark.sessionState.conf.getConfString(FGAC_JDBC_FALLBACK_ENABLED_KEY, "false").toBoolean
  }
  
  private def getFullTableName(ident: Identifier): String = {
    if (ident.namespace().isEmpty) {
      ident.name()
    } else {
      s"${ident.namespace().mkString(".")}.${ident.name()}"
    }
  }
  
  private def getFullTableName(u: UnresolvedRelation): String = {
    u.multipartIdentifier.mkString(".")
  }
  
  private def tryResolveUnresolvedRelation(u: UnresolvedRelation): LogicalPlan = {
    u.multipartIdentifier match {
      case CatalogAndIdentifier(catalog, ident) =>
        if (shouldFallbackToSnowflake(catalog, ident)) {
          logger.debug("Resolving {} via Snowflake JDBC fallback", getFullTableName(u))
          createSnowflakeRelation(ident)
        } else {
          u
        }
      case _ => u
    }
  }
  
  private def shouldFallbackToSnowflake(
      catalog: org.apache.spark.sql.connector.catalog.CatalogPlugin,
      ident: org.apache.spark.sql.connector.catalog.Identifier): Boolean = {
    try {
      catalog.asInstanceOf[org.apache.spark.sql.connector.catalog.TableCatalog].loadTable(ident)
      false
    } catch {
      case _: FGACForbiddenException => true
      case _: NoSuchTableException => false
      case _: Throwable => false
    }
  }

  private def createSnowflakeRelation(ident: Identifier): LogicalPlan = {
    val fullName = getFullTableName(ident)
    try {
      val options = buildSnowflakeOptions(ident)
      val baseRelation = snowflakeSource.createRelation(spark.sqlContext, options)
      
      logger.info("Created Snowflake JDBC relation for table: {}", fullName)
      LogicalRelation(baseRelation, isStreaming = false)
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to create Snowflake relation for $fullName", ex)
        throw new RuntimeException(
          s"Failed to create Snowflake relation for $fullName", ex)
    }
  }

  private def buildSnowflakeOptions(ident: Identifier): Map[String, String] = {
    val fullTableName = getFullTableName(ident)
    val baseOptions = Map("dbtable" -> fullTableName)
    val snowflakeOptions = collectSnowflakeConfigs()
    
    baseOptions ++ snowflakeOptions
  }
  
  private def collectSnowflakeConfigs(): Map[String, String] = {
    val allConfs =
      spark.sparkContext.getConf.getAll.toMap ++
      spark.conf.getAll ++
      spark.sessionState.conf.getAllConfs
    
    allConfs
      .filter { case (key, _) => isSnowflakeConfigKey(key) }
      .map { case (key, value) => cleanConfigKey(key) -> value }
  }
  
  private def isSnowflakeConfigKey(key: String): Boolean = {
    val lowerKey = key.toLowerCase
    lowerKey.startsWith("spark.snowflake.") || lowerKey.startsWith("snowflake.")
  }
  
  private def cleanConfigKey(key: String): String = {
    val lowerKey = key.toLowerCase
    if (lowerKey.startsWith("spark.snowflake.")) {
      key.substring("spark.snowflake.".length)
    } else if (lowerKey.startsWith("snowflake.")) {
      key.substring("snowflake.".length)
    } else {
      key
    }
  }
}
