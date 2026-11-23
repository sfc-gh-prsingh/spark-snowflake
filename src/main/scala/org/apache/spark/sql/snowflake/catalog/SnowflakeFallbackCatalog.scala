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

import net.snowflake.spark.snowflake.Parameters
import org.apache.iceberg.MetadataTableType
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.exceptions.ForbiddenException
import org.apache.iceberg.spark.SparkCatalog
import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, Table}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.Try

import IcebergTimeTravel.{Main, Refs, Spec}

private object SnowflakeFallbackCatalog {
  // Iceberg SparkCatalog's snapshot selectors in the table name; its own patterns are private
  val SnapshotIdSelector = "snapshot_id_(\\d+)".r
  val AtTimestampSelector = "at_timestamp_(\\d+)".r
  val BranchSelector = "branch_(.+)".r
  val TagSelector = "tag_(.+)".r
  // Options of this catalog that Iceberg must not see
  val SnowflakeOptions = Set("catalog-impl", "snowflake-database")
}

/**
 * Iceberg's SparkCatalog (e.g. Horizon IRC with type=rest) that reads a table through Snowflake
 * JDBC when Iceberg returns 403 for it, e.g. for a table with a Snowflake policy. Everything
 * else (branches, tags, procedures, writes) is plain Iceberg, so it needs only
 * IcebergSparkSessionExtensions, like SparkCatalog.
 */
class SnowflakeFallbackCatalog extends SparkCatalog {

  import SnowflakeFallbackCatalog._

  private val log = LoggerFactory.getLogger(getClass)
  private var catalogName: String = _
  private var snowflakeDatabase: Option[String] = None
  private var warehouse: Option[String] = None

  override protected def buildIcebergCatalog(
      name: String, options: CaseInsensitiveStringMap): Catalog =
    super.buildIcebergCatalog(name, configure(name, options))

  /**
   * Reads this catalog's own options and returns the options for Iceberg: without them, and
   * with the spark-snowflake User-Agent for Horizon.
   */
  protected def configure(
      name: String, options: CaseInsensitiveStringMap): CaseInsensitiveStringMap = {
    this.catalogName = name
    // Inserted unquoted into dbtable
    this.snowflakeDatabase =
      Option(options.get("snowflake-database")).map(_.trim).filter(_.nonEmpty)
    // Horizon IRC maps the Iceberg warehouse to the Snowflake database
    this.warehouse = Option(options.get("warehouse")).map(_.trim).filter(_.nonEmpty)
    log.info("Initializing SnowflakeFallbackCatalog: {}", name)

    // Format: spark-snowflake/<connector-version> spark/<spark-version>; header.User-Agent works
    // with all Iceberg versions
    val userAgent =
      s"spark-snowflake/${net.snowflake.spark.snowflake.Utils.VERSION} spark/$SPARK_VERSION"
    val icebergOptions = options.asScala
      .filterKeys(key => !SnowflakeOptions.contains(key.toLowerCase(Locale.ROOT)))
      .toMap ++
      Option(options.get("catalog-impl")).flatMap(icebergCatalogImpl(name, _))
        .map("catalog-impl" -> _) +
      ("header.User-Agent" -> userAgent)
    new CaseInsensitiveStringMap(icebergOptions.asJava)
  }

  /**
   * catalog-impl for Iceberg, which names its Catalog with it. Before this catalog extended
   * SparkCatalog, catalog-impl named the Spark catalog it delegated to: SparkCatalog is now
   * redundant, and any other Spark catalog can no longer be used.
   */
  private def icebergCatalogImpl(name: String, impl: String): Option[String] =
    Try(Class.forName(impl, false, Thread.currentThread().getContextClassLoader)).toOption match {
      case Some(c) if classOf[SparkCatalog].isAssignableFrom(c) =>
        log.warn(s"Catalog '$name': catalog-impl=$impl is no longer needed and is ignored; " +
          "SnowflakeFallbackCatalog is an Iceberg SparkCatalog")
        None
      case Some(c) if classOf[CatalogPlugin].isAssignableFrom(c) =>
        throw new IllegalArgumentException(
          s"Catalog '$name': SnowflakeFallbackCatalog no longer delegates to a Spark catalog " +
            s"(catalog-impl=$impl); it is an Iceberg SparkCatalog. Remove catalog-impl and " +
            "configure the Iceberg catalog with type (e.g. type=rest), or set catalog-impl to " +
            "an Iceberg Catalog class")
      case _ => Some(impl)
    }

  // Set while SparkCatalog's versioned loadTable runs: on some Iceberg versions it calls
  // loadTable(ident), which must then rethrow the 403 so the versioned load falls back with its
  // time travel spec rather than receive a plain fallback table
  private val inVersionedLoad = new ThreadLocal[Boolean] {
    override def initialValue(): Boolean = false
  }

  override def loadTable(ident: Identifier): Table =
    try {
      super.loadTable(ident)
    } catch {
      case ex: Throwable if inVersionedLoad.get() => throw ex
      case ex: Throwable if isForbiddenException(ex) =>
        metadataTableOfProtected(ident) match {
          // Snowflake has only the refs metadata table (GET_TABLE_VERSION_REFS)
          case Some(MetadataTableType.REFS) => fallbackToSnowflake(parentOf(ident), Refs, ex)
          case Some(_) => throw ex
          case None => fallbackToSnowflake(ident, Main, ex)
        }
      case ex: NoSuchTableException => loadProtectedSelector(ident).getOrElse(throw ex)
    }

  override def loadTable(ident: Identifier, version: String): Table =
    loadTimeTravel(ident, IcebergTimeTravel.fromVersion(version)) {
      super.loadTable(ident, version)
    }

  override def loadTable(ident: Identifier, timestamp: Long): Table =
    loadTimeTravel(ident, IcebergTimeTravel.TimestampMicros(timestamp)) {
      super.loadTable(ident, timestamp)
    }

  private def loadTimeTravel(ident: Identifier, spec: Spec)(load: => Table): Table = {
    val outer = inVersionedLoad.get()
    inVersionedLoad.set(true)
    try {
      load
    } catch {
      case ex: Throwable if !outer && isForbiddenException(ex) =>
        fallbackToSnowflake(ident, spec, ex)
    } finally {
      inVersionedLoad.set(outer)
    }
  }

  private def fallbackToSnowflake(ident: Identifier, spec: Spec, original: Throwable): Table = {
    log.info(s"403 Forbidden detected for ${ident}, attempting JDBC fallback")
    try {
      createFallbackTable(ident, spec)
    } catch {
      case fallbackEx: Throwable =>
        // rethrow original exception if fallback also fails
        log.error(s"Failed to create the Snowflake fallback table for ${ident}", fallbackEx)
        throw original
    }
  }

  private def parentOf(ident: Identifier): Identifier =
    Identifier.of(ident.namespace().init, ident.namespace().last)

  /**
   * The 403 of the table a metadata table or selector name belongs to (db.t for db.t.refs or
   * db.t.branch_x), if loading it fails with one. Iceberg reports those names of a 403 table as
   * the base table's 403 or as not found. A table is always in a namespace, so a name directly
   * in a single-level namespace (db.refs) is a table of its own and is not checked.
   */
  private def parentForbidden(ident: Identifier): Option[Throwable] =
    if (ident.namespace().length < 2) {
      None
    } else {
      try {
        super.loadTable(parentOf(ident))
        None
      } catch {
        case ex: Throwable if isForbiddenException(ex) => Some(ex)
        case _: Throwable => None
      }
    }

  /**
   * The Iceberg metadata table a 403 name is, when its base table is protected: Snowflake has no
   * table named db.t.refs to fall back to.
   */
  private def metadataTableOfProtected(ident: Identifier): Option[MetadataTableType] =
    Option(MetadataTableType.from(ident.name())).filter(_ => parentForbidden(ident).isDefined)

  /** A selector (db.t.branch_x, tag_x, snapshot_id_N, at_timestamp_N) of a 403 table. */
  private def loadProtectedSelector(ident: Identifier): Option[Table] =
    selectorSpec(ident.name()).flatMap { spec =>
      parentForbidden(ident).map(fallbackToSnowflake(parentOf(ident), spec, _))
    }

  /** The spec a selector name reads; None if it is not a selector or its number overflows. */
  private def selectorSpec(name: String): Option[Spec] = name match {
    case SnapshotIdSelector(id) => Try(id.toLong).toOption.map(IcebergTimeTravel.Snapshot)
    case AtTimestampSelector(ms) =>
      Try(Math.multiplyExact(ms.toLong, 1000L)).toOption.map(IcebergTimeTravel.TimestampMicros)
    case BranchSelector(ref) => Some(IcebergTimeTravel.fromBranch(ref))
    case TagSelector(ref) => Some(IcebergTimeTravel.fromTag(ref))
    case _ => None
  }

  /**
   * Horizon's 403: Iceberg's REST client throws ForbiddenException for it, which Spark or Iceberg
   * may wrap.
   *
   * TODO: Optimize 403 error handling to avoid unnecessary fallback attempts. A 403 due to
   * genuinely invalid credentials rather than FGAC restrictions also falls back, so it fails
   * twice; if Horizon IRC marks FGAC 403s in its error, the fallback could skip the others.
   */
  private def isForbiddenException(ex: Throwable): Boolean =
    Iterator.iterate(ex)(_.getCause).takeWhile(_ != null).take(16)
      .exists(_.isInstanceOf[ForbiddenException])

  private[snowflake] def resolveDatabase(sessionDatabase: Option[String]): Option[String] =
    snowflakeDatabase.orElse(warehouse.filter(_ => sessionDatabase.isEmpty))

  /**
   * Constructs the dbtable property value from a V2 Identifier.
   * This converts multi-part namespaces into Snowflake's dot-separated format.
   *
   * Examples:
   * - Identifier(["db", "schema"], "table") → "db.schema.table"
   * - Identifier(["schema"], "table") → "schema.table"
   * - Identifier([], "table") → "table"
   * - with database "db", Identifier(["schema"], "table") -> "db.schema.table";
   *   any other namespace length is rejected
   *
   * @param ident The V2 Identifier with namespace and table name
   * @param database The Snowflake database from resolveDatabase, if any
   * @return The fully qualified table name for the dbtable property
   */
  private[snowflake] def buildFullTableName(
      ident: Identifier, database: Option[String]): String = {
    require(database.isEmpty || ident.namespace().length == 1,
      s"Catalog '$catalogName' resolves Snowflake database '${database.get}', which requires " +
        s"a single-level namespace (schema.table), but got: $ident")
    val namespace = if (ident.namespace().nonEmpty) {
      Some((database.toSeq ++ ident.namespace()).mkString("."))
    } else {
      None
    }

    namespace match {
      case Some(ns) => s"$ns.${ident.name()}"
      case None => ident.name()
    }
  }

  // Hardening: ONLY the immutable SparkConf feeds connector options. spark.conf and
  // sessionState.conf are runtime SQLConf, settable via SET, and must not reach them.
  private lazy val snowflakeProps: Map[String, String] = {
    val prefixes = Seq("spark.snowflake.", "snowflake.")
    val props = SparkSession.active.sparkContext.getConf.getAll.flatMap { case (key, value) =>
      val lower = key.toLowerCase(Locale.ROOT)
      prefixes.find(lower.startsWith).map(prefix => lower.substring(prefix.length) -> value)
    }.toMap
    log.info(s"Snowflake properties for fallback tables " +
      s"(${props.size} keys): ${props.keys.mkString(", ")}")
    props
  }

  private def createFallbackTable(ident: Identifier, spec: Spec): Table = {
    val database = resolveDatabase(snowflakeProps.get(Parameters.PARAM_SF_DATABASE))
    new SnowflakeFallbackTable(
      tableName = (catalogName +: ident.namespace() :+ ident.name()).mkString("."),
      params = database.map(Parameters.PARAM_SF_DATABASE -> _).toMap ++ snowflakeProps,
      dbtable = buildFullTableName(ident, database),
      spec = spec)
  }

  // A protected table exists: Horizon answers 403 for it
  override def tableExists(ident: Identifier): Boolean = {
    try {
      super.tableExists(ident)
    } catch {
      case ex: Exception if isForbiddenException(ex) => true
      case _: Exception => false
    }
  }
}
