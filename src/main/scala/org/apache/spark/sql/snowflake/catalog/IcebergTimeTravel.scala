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
import org.apache.iceberg.SnapshotRef
import org.apache.iceberg.spark.{SparkReadOptions, SparkSQLProperties}
import org.apache.spark.sql.RuntimeConfig
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.unsafe.types.UTF8String

/**
 * Iceberg time travel for tables that SnowflakeFallbackCatalog reads over JDBC after a 403:
 * maps snapshot / branch / tag / timestamp requests to Snowflake's AT clause.
 */
private[catalog] object IcebergTimeTravel {

  /** What a read of a fallback table reads: the current table, or a version of it. */
  sealed trait Spec {
    def clause: String
    def query(dbtable: String): String = s"SELECT * FROM $dbtable $clause"
    /** Connector parameters that read `dbtable` at this spec. */
    def readParameters(dbtable: String): Map[String, String] =
      Map(Parameters.PARAM_SF_QUERY -> query(dbtable))
  }

  /** The table's current state (Iceberg's main), read from dbtable without an AT clause. */
  case object Main extends Spec {
    def clause: String = ""
    override def query(dbtable: String): String = s"SELECT * FROM $dbtable"
    override def readParameters(dbtable: String): Map[String, String] =
      Map(Parameters.PARAM_SF_DBTABLE -> dbtable)
  }

  final case class Snapshot(id: Long) extends Spec {
    def clause: String = s"AT(VERSION => $id)"
  }

  final case class Tag(name: String) extends Spec {
    def clause: String = s"AT(VERSION_REF => '${quote(name)}')"
  }

  /**
   * A branch. Snowflake refreshes a branch of an unmanaged table that it has only recorded when a
   * query reads it with AT(BRANCH); AT(VERSION_REF) fails with "branch has not been refreshed".
   */
  final case class Branch(name: String) extends Spec {
    def clause: String = s"AT(BRANCH => '${quote(name)}')"
  }

  /**
   * VERSION AS OF '<name>': a branch or a tag, which `resolve` tells apart from the table's refs.
   * Unresolved, Snowflake's VERSION_REF reads either.
   */
  final case class Named(name: String) extends Spec {
    def clause: String = s"AT(VERSION_REF => '${quote(name)}')"
  }

  final case class TimestampMicros(micros: Long) extends Spec {
    def clause: String = s"AT(TIMESTAMP => TO_TIMESTAMP_LTZ($micros, 6))"
  }

  /** Iceberg's refs metadata table, from Snowflake's GET_TABLE_VERSION_REFS. */
  case object Refs extends Spec {
    def clause: String = "REFS"
    override def query(dbtable: String): String = {
      // The function resolves in a database's INFORMATION_SCHEMA; dbtable is unquoted
      val parts = dbtable.split('.')
      val infoSchema =
        if (parts.length == 3) s"${parts(0)}.INFORMATION_SCHEMA" else "INFORMATION_SCHEMA"
      """SELECT REFERENCE_NAME AS "name", REFERENCE_TYPE AS "type", VERSION_ID AS """ +
        """"snapshot_id", MAX_REFERENCE_AGE_IN_MS AS "max_reference_age_in_ms", """ +
        """MIN_SNAPSHOTS_TO_KEEP AS "min_snapshots_to_keep", MAX_SNAPSHOT_AGE_IN_MS AS """ +
        s""""max_snapshot_age_in_ms" FROM TABLE($infoSchema.GET_TABLE_VERSION_REFS(""" +
        s"""TABLE_NAME => '${quote(dbtable)}'))"""
    }
    // Iceberg's refs columns are lowercase; the connector would otherwise quote them ("name")
    override def readParameters(dbtable: String): Map[String, String] =
      super.readParameters(dbtable) + (Parameters.PARAM_KEEP_COLUMN_CASE -> "on")
  }

  /** Main for main (Snowflake rejects AT(VERSION_REF => 'main')); ref names are case-sensitive. */
  private def unlessMain(name: String)(spec: String => Spec): Spec =
    if (name == SnapshotRef.MAIN_BRANCH) Main else spec(name)

  def fromBranch(name: String): Spec = unlessMain(name)(Branch)

  def fromTag(name: String): Spec = unlessMain(name)(Tag)

  /** Spark's VERSION AS OF: a snapshot ID if numeric, otherwise a branch or tag (as Iceberg). */
  def fromVersion(version: String): Spec =
    if (version.nonEmpty && version.forall(_.isDigit)) Snapshot(version.toLong)
    else unlessMain(version)(Named)

  /** VERSION AS OF '<name>' as a branch or tag read, given the table's refs (name -> type). */
  def resolve(spec: Spec, refTypes: => Map[String, String]): Spec = spec match {
    case Named(name) if refTypes.get(name).contains("BRANCH") => Branch(name)
    case Named(name) => Tag(name)
    case other => other
  }

  // Literals: Iceberg 1.11 renamed these SparkReadOptions constants (LEGACY_SNAPSHOT_ID, ...), so
  // no one name compiles against both Iceberg versions this connector builds with
  private val SnapshotId = "snapshot-id"
  private val AsOfTimestamp = "as-of-timestamp"
  private val TagOption = "tag"
  private val BranchOption = SparkReadOptions.BRANCH
  // Spark's own read options; on 4.x Spark turns them into loadTable(ident, version|timestamp)
  private val VersionAsOf = SparkReadOptions.VERSION_AS_OF.toLowerCase(Locale.ROOT)
  private val TimestampAsOf = SparkReadOptions.TIMESTAMP_AS_OF.toLowerCase(Locale.ROOT)
  private val ReadOptions =
    Seq(SnapshotId, AsOfTimestamp, BranchOption, TagOption, VersionAsOf, TimestampAsOf)

  private def fromOption(key: String, value: String): Spec = key match {
    case SnapshotId => Snapshot(value.trim.toLong)
    case AsOfTimestamp => TimestampMicros(Math.multiplyExact(value.trim.toLong, 1000L))
    case BranchOption => fromBranch(value)
    case TagOption => fromTag(value)
    case VersionAsOf => fromVersion(value.trim)
    case TimestampAsOf =>
      val zone = DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone)
      TimestampMicros(DateTimeUtils.stringToTimestamp(UTF8String.fromString(value.trim), zone)
        .getOrElse(throw new IllegalArgumentException(s"Invalid timestampAsOf: $value")))
  }

  /**
   * What a read with these options reads, for a table the catalog loaded at `loaded`: a time
   * travel read option applies to a table loaded at its current state.
   */
  def readSpec(loaded: Spec, options: Map[String, String]): Spec = {
    val params = options.map { case (k, v) => k.toLowerCase(Locale.ROOT) -> v }
    ReadOptions.filter(params.contains) match {
      case Seq() => loaded
      // On 4.x, Spark passes VERSION/TIMESTAMP AS OF both to loadTable and as options
      case keys if loaded != Main && keys.forall(k => k == VersionAsOf || k == TimestampAsOf) =>
        loaded
      case Seq(key) if loaded == Main => fromOption(key, params(key))
      case keys =>
        throw new IllegalArgumentException(
          s"Cannot combine time travel options ${keys.mkString(", ")} with each other " +
            "or with VERSION AS OF / TIMESTAMP AS OF")
    }
  }

  /** Read options other than time travel ones, which the connector receives as is. */
  def withoutReadOptions(options: Map[String, String]): Map[String, String] =
    options.filter { case (k, _) => !ReadOptions.contains(k.toLowerCase(Locale.ROOT)) }

  private def quote(name: String): String = name.replace("'", "''")

  /** The table's refs as (name, type) rows. */
  def refTypesQuery(dbtable: String): String =
    s"""SELECT "name", "type" FROM (${Refs.query(dbtable)})"""

  /** spark.wap.branch, unless unset or main. */
  def wapBranch(conf: RuntimeConfig): Option[String] =
    conf.getOption(SparkSQLProperties.WAP_BRANCH)
      .filter(b => b.nonEmpty && b != SnapshotRef.MAIN_BRANCH)

  /** spark.wap.id, unless unset. */
  def wapId(conf: RuntimeConfig): Option[String] =
    conf.getOption(SparkSQLProperties.WAP_ID).filter(_.nonEmpty)

  /**
   * Branch writes have no Snowflake SQL equivalent, so they fail instead of writing main.
   * spec: what the written table was loaded at; only Main takes writes.
   */
  def checkWrite(spec: Spec, wapBranch: Option[String], wapId: Option[String]): Unit = {
    // Iceberg stages the snapshot of a spark.wap.id write without publishing it; Snowflake can't
    wapId.foreach { id =>
      throw new UnsupportedOperationException(
        s"spark.wap.id=$id is set, but writes to a policy-protected table go through Snowflake, " +
          "which cannot stage an unpublished snapshot. Unset spark.wap.id to write to the main " +
          "branch.")
    }
    wapBranch.foreach { branch =>
      throw new UnsupportedOperationException(
        s"spark.wap.branch=$branch is set, but writes to a policy-protected table go " +
          "through Snowflake, which cannot write to an Iceberg branch. Unset spark.wap.branch " +
          "to write to the main branch.")
    }
    if (spec != Main) {
      throw new UnsupportedOperationException(
        "Cannot write to an Iceberg branch, tag or snapshot of a table read through " +
          "Snowflake (policy-protected table)")
    }
  }
}
