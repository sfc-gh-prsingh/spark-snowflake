/*
 * Copyright 2015-2025 Snowflake Computing
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.snowflake.catalog

import java.util.Locale

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

import net.snowflake.spark.snowflake.{DefaultSource, Utils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.{ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns, V1Scan}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, SupportsTruncate, V1Write, WriteBuilder}
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

import IcebergTimeTravel.{Branch, Main, Spec}

/**
 * A Snowflake table name from the parts of a Spark identifier. Each part becomes a Snowflake
 * identifier on its own (Utils.ensureQuoted): a plain name as Snowflake resolves it unquoted, in
 * upper case, a part already quoted as is, and anything else quoted exactly, so no part can
 * change the SQL it is placed in.
 */
private[catalog] final case class SnowflakeTableName(parts: Seq[String]) {
  require(parts.nonEmpty, "A table name has at least one part")

  def sql: String = parts.map(Utils.ensureQuoted).mkString(".")

  /** INFORMATION_SCHEMA of the table's database; the session's when the name has none. */
  def informationSchema: String =
    if (parts.length == 3) s"${Utils.ensureQuoted(parts.head)}.INFORMATION_SCHEMA"
    else "INFORMATION_SCHEMA"

  override def toString: String = sql
}

/**
 * A table SnowflakeFallbackCatalog reads and writes through Snowflake after Horizon returns 403
 * for it, so Snowflake applies the table's policies. Reads honor Iceberg's time travel read
 * options and spark.wap.branch; writes go to main only.
 *
 * Each read builds its Snowflake relation from its own options: as a V2 table it is not in
 * Spark's V1 relation cache, which would ignore them.
 *
 * @param params   Snowflake connection parameters, without dbtable or query
 * @param table    the table in Snowflake
 * @param spec     what the catalog loaded: the current table, or a version of it
 * @param loadRefs reads the table's refs (name -> type); by default from Snowflake
 */
private[catalog] class SnowflakeFallbackTable(
    tableName: String,
    val params: Map[String, String],
    val table: SnowflakeTableName,
    val spec: Spec,
    loadRefs: (Map[String, String], SnowflakeTableName) => Map[String, String] =
      SnowflakeFallbackTable.queryRefs)
  extends Table with SupportsRead with SupportsWrite {

  import SnowflakeFallbackTable._

  private def spark: SparkSession = SparkSession.active

  // At most one refs query per loaded table, and only for spark.wap.branch or VERSION AS OF
  private lazy val refs: Try[Map[String, String]] = Try(loadRefs(params, table))

  /**
   * What a read with these options reads: a time travel option, else (as Iceberg) the
   * spark.wap.branch branch once it exists, and VERSION AS OF '<name>' as a branch or tag.
   */
  private[catalog] def readSpec(options: Map[String, String]): Spec =
    IcebergTimeTravel.readSpec(spec, options) match {
      case Main =>
        IcebergTimeTravel.wapBranch(spark.conf).fold[Spec](Main) { wap =>
          val known = refs match {
            case Success(types) => types
            case Failure(e) =>
              throw new IllegalStateException(
                s"Cannot read the refs of $table to apply spark.wap.branch=$wap; unset " +
                  "spark.wap.branch to read the main branch", e)
          }
          if (known.get(wap).contains("BRANCH")) Branch(wap) else Main
        }
      case requested =>
        IcebergTimeTravel.resolve(requested, refs.getOrElse {
          // Unresolved, VERSION_REF still reads a refreshed branch or a tag
          log.warn(s"Cannot read the refs of $table; reading VERSION AS OF with VERSION_REF")
          Map.empty
        })
    }

  /** Connector parameters for reading or writing at `at`; other options pass through. */
  private def relationParams(options: Map[String, String], at: Spec): Map[String, String] =
    params ++ IcebergTimeTravel.withoutReadOptions(options) ++ at.readParameters(table)

  private def relation(parameters: Map[String, String]): SnowflakeRelation =
    new DefaultSource().createRelation(spark.sqlContext, parameters) match {
      case r: SnowflakeRelation => r
      case other =>
        throw new IllegalStateException(s"Unexpected Snowflake relation ${other.getClass.getName}")
    }

  private lazy val tableSchema: StructType =
    relation(relationParams(Map.empty, readSpec(Map.empty))).schema

  override def name(): String = tableName
  override def schema(): StructType = tableSchema
  override def capabilities(): java.util.Set[TableCapability] = Set(
    TableCapability.BATCH_READ, TableCapability.V1_BATCH_WRITE, TableCapability.TRUNCATE).asJava

  /** Connector parameters of a read with these options. */
  private[catalog] def scanParameters(options: Map[String, String]): Map[String, String] =
    relationParams(options, readSpec(options))

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new SnowflakeScanBuilder(relation(scanParameters(options.asCaseSensitiveMap().asScala.toMap)))

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new WriteBuilder with SupportsTruncate {
      private var truncating = false
      override def truncate(): WriteBuilder = { truncating = true; this }
      override def build(): V1Write = new V1Write {
        override def toInsertableRelation: InsertableRelation = new InsertableRelation {
          // Spark's V1 fallback always passes overwrite = false; truncate() carries INSERT OVERWRITE
          override def insert(data: DataFrame, overwrite: Boolean): Unit = {
            val conf = spark.conf
            IcebergTimeTravel.checkWrite(
              spec, IcebergTimeTravel.wapBranch(conf), IcebergTimeTravel.wapId(conf))
            val writeOptions = info.options().asCaseSensitiveMap().asScala.toMap
            relation(relationParams(writeOptions, Main)).insert(data, truncating || overwrite)
          }
        }
      }
    }

  override def toString: String = s"SnowflakeFallbackTable($tableName, $table, $spec)"
}

private[catalog] object SnowflakeFallbackTable {
  private[catalog] type SnowflakeRelation =
    BaseRelation with PrunedFilteredScan with InsertableRelation

  private val log = LoggerFactory.getLogger(classOf[SnowflakeFallbackTable])

  /** The table's refs from GET_TABLE_VERSION_REFS, as name -> type (BRANCH or TAG). */
  def queryRefs(params: Map[String, String], table: SnowflakeTableName): Map[String, String] = {
    val rows = Utils.runQuery(params, IcebergTimeTravel.refTypesQuery(table))
    Iterator.continually(rows).takeWhile(_.next())
      .map(r => r.getString(1) -> r.getString(2).toUpperCase(Locale.ROOT)).toMap
  }

  /** A V1 scan of the Snowflake relation: filters and columns are pushed into its query. */
  private[catalog] class SnowflakeScanBuilder(relation: SnowflakeRelation)
    extends ScanBuilder with SupportsPushDownFilters with SupportsPushDownRequiredColumns {

    private var required: StructType = relation.schema
    private var pushed: Array[Filter] = Array.empty

    // As for the V1 relation: Spark evaluates again only the filters Snowflake doesn't handle
    override def pushFilters(filters: Array[Filter]): Array[Filter] = {
      pushed = filters
      relation.unhandledFilters(filters)
    }

    override def pushedFilters(): Array[Filter] = pushed

    override def pruneColumns(requiredSchema: StructType): Unit = required = requiredSchema

    override def build(): V1Scan = {
      val columns = required
      val filters = pushed
      new V1Scan {
        override def readSchema(): StructType = columns
        override def toV1TableScan[T <: BaseRelation with TableScan](context: SQLContext): T =
          new BaseRelation with TableScan {
            override def sqlContext: SQLContext = context
            override def schema: StructType = columns
            override def needConversion: Boolean = relation.needConversion
            override def buildScan(): RDD[Row] = relation.buildScan(columns.fieldNames, filters)
          }.asInstanceOf[T]
        override def description(): String = s"Snowflake $relation"
      }
    }
  }
}
