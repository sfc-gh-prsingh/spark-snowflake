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
package net.snowflake.spark.snowflake

import java.time.{LocalDateTime, ZoneId}

import net.snowflake.spark.snowflake.IcebergTimeTravel.{Ref, Snapshot, TimestampMicros}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class IcebergTimeTravelSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())
    spark = SparkSession.builder().master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private val fallbackTable = Map(
    "dbtable" -> "MYDB.db.secret",
    "sfdatabase" -> "MYDB",
    Parameters.PARAM_ICEBERG_FALLBACK -> "true")

  private def rewritten(options: (String, String)*): Map[String, String] =
    IcebergTimeTravel.rewriteReadParameters(fallbackTable ++ options)

  private def query(options: (String, String)*): Option[String] = rewritten(options: _*).get("query")

  private val epochMillis = 1764547200000L

  test("each time travel read option maps to its Snowflake AT clause") {
    val ts = "2025-12-01 00:00:00"
    val tsMicros = LocalDateTime.parse("2025-12-01T00:00:00")
      .atZone(ZoneId.of("UTC")).toInstant.toEpochMilli * 1000L
    val cases = Seq(
      ("snapshot-id" -> "123") -> "AT(VERSION => 123)",
      ("as-of-timestamp" -> epochMillis.toString) ->
        s"AT(TIMESTAMP => TO_TIMESTAMP_LTZ(${epochMillis * 1000L}, 6))",
      ("branch" -> "audit") -> "AT(VERSION_REF => 'audit')",
      ("tag" -> "v1") -> "AT(VERSION_REF => 'v1')",
      ("versionAsOf" -> "123") -> "AT(VERSION => 123)",
      ("versionAsOf" -> "audit") -> "AT(VERSION_REF => 'audit')",
      ("timestampAsOf" -> ts) -> s"AT(TIMESTAMP => TO_TIMESTAMP_LTZ($tsMicros, 6))")
    for ((option, clause) <- cases) {
      val params = rewritten(option)
      assert(params.get("query").contains(s"SELECT * FROM MYDB.db.secret $clause"), option)
      assert(!params.contains("dbtable"), option)
      assert(!params.contains(option._1.toLowerCase), s"$option must not reach JDBC")
      assert(params.get(Parameters.PARAM_ICEBERG_FALLBACK).contains("true"))
    }
  }

  test("branch and tag names are quoted as Snowflake string literals") {
    assert(query("branch" -> "it's") ==
      Some("SELECT * FROM MYDB.db.secret AT(VERSION_REF => 'it''s')"))
  }

  test("VERSION AS OF picks a snapshot ID only for numeric versions") {
    assert(IcebergTimeTravel.fromVersion("42") == Snapshot(42L))
    assert(IcebergTimeTravel.fromVersion("v42") == Ref("v42"))
    assert(IcebergTimeTravel.fromVersion("") == Ref(""))
  }

  test("a fallback table without time travel options keeps dbtable") {
    assert(rewritten() == fallbackTable)
  }

  test("non-fallback Snowflake tables are not rewritten") {
    val plain = Map("dbtable" -> "T", "branch" -> "x", "snapshot-id" -> "1")
    assert(IcebergTimeTravel.rewriteReadParameters(plain) == plain)
  }

  test("Spark's own AS OF options are dropped once the catalog applied them") {
    val applied = fallbackTable - "dbtable" + ("query" -> "SELECT * FROM t AT(VERSION => 1)")
    assert(IcebergTimeTravel.rewriteReadParameters(applied + ("versionAsOf" -> "1")) == applied)
  }

  test("conflicting time travel requests fail") {
    intercept[IllegalArgumentException](rewritten("branch" -> "x", "tag" -> "t"))
    val applied = fallbackTable - "dbtable" + ("query" -> "SELECT * FROM t AT(VERSION => 1)")
    intercept[IllegalArgumentException](
      IcebergTimeTravel.rewriteReadParameters(applied + ("branch" -> "x")))
  }

  test("invalid option values fail") {
    intercept[NumberFormatException](rewritten("snapshot-id" -> "abc"))
    intercept[IllegalArgumentException](rewritten("timestampAsOf" -> "not a timestamp"))
  }

  private val creds = Map("sfurl" -> "acct.snowflakecomputing.com:443", "sfuser" -> "u",
    "sfpassword" -> "p")
  private val schema = StructType(Seq(StructField("id", IntegerType)))

  private def relation(params: Map[String, String]): SnowflakeRelation =
    new DefaultSource().createRelation(spark.sqlContext, params ++ creds, schema)
      .asInstanceOf[SnowflakeRelation]

  private def selectSql(rel: SnowflakeRelation): String = {
    val method = rel.getClass.getDeclaredMethods.find(_.getName.endsWith("standardStatement")).get
    method.setAccessible(true)
    method.invoke(rel, Array("id"), Array.empty[Filter])
      .asInstanceOf[SnowflakeSQLStatement].statementString
  }

  test("DefaultSource sends the AT clause in the SELECT for fallback tables") {
    val rel = relation(fallbackTable + ("snapshot-id" -> "123"))
    assert(rel.params.table.isEmpty)
    assert(selectSql(rel) ==
      """SELECT "ID" FROM (SELECT * FROM MYDB.db.secret AT(VERSION => 123))""")
    assert(!rel.params.sfExtraOptions.contains("snapshot-id"))
    assert(!rel.params.sfExtraOptions.contains(Parameters.PARAM_ICEBERG_FALLBACK))
  }

  test("DefaultSource reads a fallback table without options from dbtable") {
    val rel = relation(fallbackTable)
    assert(rel.params.table.map(_.toString).contains("MYDB.db.secret"))
    assert(rel.params.query.isEmpty)
  }

  test("writes to a branch, tag or snapshot of a fallback table fail") {
    val atSnapshot = relation(fallbackTable + ("branch" -> "x")).params
    intercept[UnsupportedOperationException](IcebergTimeTravel.checkWrite(atSnapshot, None))
  }

  test("writes to a fallback table with spark.wap.branch set fail instead of writing main") {
    val main = relation(fallbackTable).params
    IcebergTimeTravel.checkWrite(main, None)
    IcebergTimeTravel.checkWrite(main, Some(""))
    val e = intercept[UnsupportedOperationException](
      IcebergTimeTravel.checkWrite(main, Some("audit")))
    assert(e.getMessage.contains("spark.wap.branch=audit"))
  }

  test("writes to other Snowflake tables are not affected by spark.wap.branch") {
    val plain = relation(Map("dbtable" -> "T")).params
    IcebergTimeTravel.checkWrite(plain, Some("audit"))
  }
}
