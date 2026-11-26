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

import java.nio.file.Files

import org.apache.iceberg.exceptions.{ForbiddenException, ValidationException}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsRead, TableCatalog, V1Table}
import org.apache.spark.sql.snowflake.catalog.IcebergTimeTravel._
import org.apache.spark.sql.snowflake.catalog.LocalSparkSuite.fallbackTable
import org.scalatest.funsuite.AnyFunSuite

/** Time travel on a table that Horizon 403s (simulated by ForbiddingFallbackCatalog). */
class SnowflakeFallbackCatalogTimeTravelTest extends AnyFunSuite with LocalSparkSuite {

  private var snapshotId: Long = _

  override protected def configure(builder: SparkSession.Builder): SparkSession.Builder = {
    val path = Files.createTempDirectory("fallback-time-travel").toUri.toString
    def fallback(b: SparkSession.Builder, name: String) = b
      .config(s"spark.sql.catalog.$name", classOf[ForbiddingFallbackCatalog].getName)
      .config(s"spark.sql.catalog.$name.hadoop-warehouse", path)
      .config(s"spark.sql.catalog.$name.forbidden-tables", "db.secret,db.history")
      .config(s"spark.sql.catalog.$name.cache-enabled", "false")
    val withRaw = LocalSparkSuite.withIceberg(builder)
      .config("spark.sql.catalog.raw", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.raw.type", "hadoop")
      .config("spark.sql.catalog.raw.warehouse", path)
    fallback(fallback(withRaw, "fb").config("spark.sql.catalog.fb.warehouse", "MYDB"), "fbnowh")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS raw.db")
    spark.sql("CREATE TABLE raw.db.secret (id INT) USING iceberg")
    spark.sql("INSERT INTO raw.db.secret VALUES (1)")
    spark.sql("ALTER TABLE raw.db.secret CREATE BRANCH x")
    spark.sql("CREATE TABLE raw.db.open (id INT) USING iceberg")
    snapshotId = spark.sql("SELECT snapshot_id FROM raw.db.secret.snapshots").head.getLong(0)
  }

  private def catalog(name: String = "fb"): TableCatalog =
    spark.sessionState.catalogManager.catalog(name).asInstanceOf[TableCatalog]

  private val secret = Identifier.of(Array("db"), "secret")

  private def name(dotted: String): SnowflakeTableName = SnowflakeTableName(dotted.split('.').toSeq)

  private def specOf(ident: Identifier, name: String = "fb"): Spec =
    fallbackTable(catalog(name).loadTable(ident)).spec

  test("plain load falls back to the current table") {
    val table = fallbackTable(catalog().loadTable(secret))
    assert(table.spec == Main)
    assert(table.table == name("MYDB.db.secret"))
    assert(table.name() == "fb.db.secret")
  }

  test("fallback tables are V2 tables, outside Spark's V1 relation cache") {
    // The V1 cache ignores read options, and its key (QualifiedTableName) differs across Spark
    val table = catalog().loadTable(secret)
    assert(table.isInstanceOf[SupportsRead] && !table.isInstanceOf[V1Table])
  }

  test("VERSION AS OF a snapshot ID falls back with AT(VERSION)") {
    assert(fallbackTable(catalog().loadTable(secret, snapshotId.toString)).spec ==
      Snapshot(snapshotId))
  }

  test("VERSION AS OF a name falls back as a branch or tag, resolved when read") {
    assert(fallbackTable(catalog().loadTable(secret, "x")).spec == Named("x"))
  }

  test("VERSION AS OF 'main' and branch_main fall back to the current table") {
    assert(fallbackTable(catalog().loadTable(secret, "main")).spec == Main)
    assert(specOf(Identifier.of(Array("db", "secret"), "branch_main")) == Main)
  }

  test("TIMESTAMP AS OF falls back with AT(TIMESTAMP) in microseconds") {
    assert(fallbackTable(catalog().loadTable(secret, 1764547200000000L)).spec ==
      TimestampMicros(1764547200000000L))
  }

  test("time travel without a warehouse uses the namespace as is") {
    val table = fallbackTable(catalog("fbnowh").loadTable(secret, "x"))
    assert(table.table == name("db.secret"))
    assert(table.spec == Named("x"))
  }

  test("Iceberg selector names on a protected table fall back with the matching spec") {
    val cases = Seq(
      "branch_x" -> Branch("x"),
      "tag_t" -> Tag("t"),
      s"snapshot_id_$snapshotId" -> Snapshot(snapshotId),
      // A snapshot ID with 403 in it is still a selector, not a 403 (it was flaky with random IDs)
      "snapshot_id_5010424037564433155" -> Snapshot(5010424037564433155L),
      "at_timestamp_1764547200000" -> TimestampMicros(1764547200000000L))
    for ((name, spec) <- cases) {
      val table = fallbackTable(catalog().loadTable(Identifier.of(Array("db", "secret"), name)))
      assert(table.spec == spec, name)
      assert(table.table == this.name("MYDB.db.secret"), name)
    }
  }

  test("selector names whose number overflows are not found instead of failing to parse") {
    for (name <- Seq("snapshot_id_99999999999999999999", s"at_timestamp_${Long.MaxValue}")) {
      intercept[NoSuchTableException](catalog().loadTable(Identifier.of(Array("db", "secret"), name)))
    }
  }

  test("selector names of readable or missing tables keep the delegate's error") {
    intercept[ValidationException](
      catalog().loadTable(Identifier.of(Array("db", "open"), "branch_missing")))
    intercept[NoSuchTableException](
      catalog().loadTable(Identifier.of(Array("db", "missing"), "branch_x")))
  }

  test("the refs metadata table of a protected table is read from Snowflake") {
    for ((name, dbtable) <- Seq("fb" -> "MYDB.db.secret", "fbnowh" -> "db.secret")) {
      val table = fallbackTable(catalog(name).loadTable(Identifier.of(Array("db", "secret"), "refs")))
      assert(table.spec == Refs, name)
      assert(table.table == this.name(dbtable), name)
    }
  }

  test("other metadata tables of a protected table keep the 403 instead of reading db.secret.x") {
    for (name <- Seq("fb", "fbnowh"); meta <- Seq("snapshots", "history", "entries")) {
      intercept[ForbiddenException](
        catalog(name).loadTable(Identifier.of(Array("db", "secret"), meta)))
    }
  }

  test("a protected table named like a metadata table, directly in a schema, is its own table") {
    // db.history is a table, not the history of a table db
    val table = fallbackTable(catalog().loadTable(Identifier.of(Array("db"), "history")))
    assert(table.spec == Main)
    assert(table.table == name("MYDB.db.history"))
  }

  test("tableExists sees a protected table without reading it") {
    assert(catalog().tableExists(secret))
  }
}
