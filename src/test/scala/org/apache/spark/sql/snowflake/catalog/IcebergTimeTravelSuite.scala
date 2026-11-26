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

import java.time.{LocalDateTime, ZoneId}

import net.snowflake.spark.snowflake.{DefaultSource, Parameters}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, V1Write}
import org.apache.spark.sql.snowflake.catalog.IcebergTimeTravel._
import org.apache.spark.sql.sources.{EqualTo, Filter}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class IcebergTimeTravelSuite extends AnyFunSuite with LocalSparkSuite {

  override protected def configure(builder: SparkSession.Builder): SparkSession.Builder =
    builder.config("spark.sql.session.timeZone", "UTC")

  private val dbtable = SnowflakeTableName(Seq("MYDB", "db", "secret"))
  // Each part a Snowflake identifier: plain names in upper case, as Snowflake resolves them
  private val sql = "\"MYDB\".\"DB\".\"SECRET\""
  private val epochMillis = 1764547200000L

  private def read(options: (String, String)*): Spec = readSpec(Main, options.toMap)

  test("each time travel read option maps to its Snowflake AT clause") {
    val ts = "2025-12-01 00:00:00"
    val tsMicros = LocalDateTime.parse("2025-12-01T00:00:00")
      .atZone(ZoneId.of("UTC")).toInstant.toEpochMilli * 1000L
    val cases = Seq(
      ("snapshot-id" -> "123") -> "AT(VERSION => 123)",
      ("as-of-timestamp" -> epochMillis.toString) ->
        s"AT(TIMESTAMP => TO_TIMESTAMP_LTZ(${epochMillis * 1000L}, 6))",
      ("branch" -> "audit") -> "AT(BRANCH => 'audit')",
      ("tag" -> "v1") -> "AT(VERSION_REF => 'v1')",
      ("versionAsOf" -> "123") -> "AT(VERSION => 123)",
      ("versionAsOf" -> "audit") -> "AT(VERSION_REF => 'audit')",
      ("timestampAsOf" -> ts) -> s"AT(TIMESTAMP => TO_TIMESTAMP_LTZ($tsMicros, 6))")
    for ((option, clause) <- cases) {
      val params = read(option).readParameters(dbtable)
      assert(params == Map("query" -> s"SELECT * FROM $sql $clause"), option)
      assert(withoutReadOptions(Map(option, "other" -> "x")) == Map("other" -> "x"),
        s"$option must not reach JDBC")
    }
  }

  test("branch and tag names are quoted as Snowflake string literals") {
    assert(Branch("it's").query(dbtable) == s"SELECT * FROM $sql AT(BRANCH => 'it''s')")
    assert(Tag("it's").query(dbtable) == s"SELECT * FROM $sql AT(VERSION_REF => 'it''s')")
    // A backslash escapes in a Snowflake string literal, so it can't end the literal early
    assert(Tag("x\\").clause == "AT(VERSION_REF => 'x\\\\')")
    assert(Branch("x\\'); DROP TABLE t; --").clause ==
      "AT(BRANCH => 'x\\\\''); DROP TABLE t; --')")
  }

  test("table names are Snowflake identifiers, so a crafted name can't change the SQL") {
    // Plain names stay case-insensitive, as when Snowflake resolves them unquoted
    assert(SnowflakeTableName(Seq("my_db", "Schema", "t1")).sql == "\"MY_DB\".\"SCHEMA\".\"T1\"")
    // A part already quoted, e.g. a database configured as "My DB", is kept as is
    assert(SnowflakeTableName(Seq("\"My DB\"", "s", "t")).sql == "\"My DB\".\"S\".\"T\"")
    // Anything else is quoted exactly, with its double quotes doubled
    assert(SnowflakeTableName(Seq("my-schema", "my.table")).sql == "\"my-schema\".\"my.table\"")
    val crafted = SnowflakeTableName(Seq("db", "t\" AT(VERSION => 1); DROP TABLE x; --"))
    assert(crafted.sql == "\"DB\".\"t\"\" AT(VERSION => 1); DROP TABLE x; --\"")
    assert(Main.readParameters(crafted) == Map("dbtable" -> crafted.sql))
    // The database of the refs function is an identifier too, and the table name a literal
    val db = SnowflakeTableName(Seq("d\".x", "s", "t'"))
    assert(db.informationSchema == "\"d\"\".x\".INFORMATION_SCHEMA")
    assert(Refs.query(db).endsWith(
      "FROM TABLE(\"d\"\".x\".INFORMATION_SCHEMA.GET_TABLE_VERSION_REFS(" +
        "TABLE_NAME => '\"d\"\".x\".\"S\".\"t''\"'))"))
  }

  test("VERSION AS OF picks a snapshot ID only for numeric versions, else a branch or tag") {
    assert(fromVersion("42") == Snapshot(42L))
    assert(fromVersion("v42") == Named("v42"))
    assert(fromVersion("") == Named(""))
    assert(fromBranch("dev") == Branch("dev"))
  }

  test("VERSION AS OF '<name>' reads a branch with AT(BRANCH) and a tag with AT(VERSION_REF)") {
    // Snowflake refreshes a recorded branch of an unmanaged table only for AT(BRANCH)
    assert(resolve(Named("dev"), Map("dev" -> "BRANCH")) == Branch("dev"))
    assert(resolve(Named("v1"), Map("v1" -> "TAG")) == Tag("v1"))
    assert(resolve(Named("gone"), Map.empty) == Tag("gone"))
    assert(resolve(Snapshot(1L), sys.error("no lookup for a snapshot")) == Snapshot(1L))
  }

  test("main is read as the current table, without an AT clause") {
    assert(fromVersion("main") == Main)
    assert(fromTag("main") == Main)
    // Iceberg ref names are case-sensitive
    assert(fromTag("MAIN") == Tag("MAIN"))
    for (option <- Seq("branch" -> "main", "versionAsOf" -> "main")) {
      assert(read(option) == Main, option)
    }
    assert(Main.readParameters(dbtable) == Map("dbtable" -> sql))
  }

  test("a read without time travel options reads what the catalog loaded") {
    assert(read() == Main)
    assert(readSpec(Snapshot(1L), Map("other" -> "x")) == Snapshot(1L))
  }

  test("Spark's own AS OF options are ignored once the catalog applied them") {
    assert(readSpec(Snapshot(1L), Map("versionAsOf" -> "1")) == Snapshot(1L))
    assert(readSpec(TimestampMicros(5L), Map("timestampAsOf" -> "x")) == TimestampMicros(5L))
  }

  test("conflicting time travel requests fail") {
    intercept[IllegalArgumentException](read("branch" -> "x", "tag" -> "t"))
    intercept[IllegalArgumentException](readSpec(Snapshot(1L), Map("branch" -> "x")))
  }

  test("invalid option values fail") {
    intercept[NumberFormatException](read("snapshot-id" -> "abc"))
    intercept[IllegalArgumentException](read("timestampAsOf" -> "not a timestamp"))
    intercept[ArithmeticException](read("as-of-timestamp" -> Long.MaxValue.toString))
  }

  test("the refs metadata table is read from GET_TABLE_VERSION_REFS with Iceberg's columns") {
    assert(Refs.query(dbtable) ==
      "SELECT REFERENCE_NAME AS \"name\", REFERENCE_TYPE AS \"type\", VERSION_ID AS " +
        "\"snapshot_id\", MAX_REFERENCE_AGE_IN_MS AS \"max_reference_age_in_ms\", " +
        "MIN_SNAPSHOTS_TO_KEEP AS \"min_snapshots_to_keep\", MAX_SNAPSHOT_AGE_IN_MS AS " +
        "\"max_snapshot_age_in_ms\" FROM TABLE(\"MYDB\".INFORMATION_SCHEMA." +
        "GET_TABLE_VERSION_REFS(TABLE_NAME => '\"MYDB\".\"DB\".\"SECRET\"'))")
    // Without a database in the name, the function resolves in the session's database
    assert(Refs.query(SnowflakeTableName(Seq("db", "secret"))).endsWith(
      "FROM TABLE(INFORMATION_SCHEMA.GET_TABLE_VERSION_REFS(TABLE_NAME => '\"DB\".\"SECRET\"'))"))
    // Iceberg's lowercase column names, not "name"; other reads keep the connector's default
    assert(Refs.readParameters(dbtable).get("keep_column_case").contains("on"))
    assert(!Branch("x").readParameters(dbtable).contains("keep_column_case"))
    assert(refTypesQuery(dbtable) == s"""SELECT "name", "type" FROM (${Refs.query(dbtable)})""")
  }

  test("writes to a branch, tag or snapshot of a fallback table fail") {
    checkWrite(Main, None, None)
    for (spec <- Seq(Branch("x"), Tag("t"), Snapshot(1L), Named("n"), Refs)) {
      intercept[UnsupportedOperationException](checkWrite(spec, None, None))
    }
  }

  test("writes with spark.wap.branch or spark.wap.id set fail instead of writing main") {
    val branch = intercept[UnsupportedOperationException](checkWrite(Main, Some("audit"), None))
    assert(branch.getMessage.contains("spark.wap.branch=audit"))
    val id = intercept[UnsupportedOperationException](checkWrite(Main, None, Some("w1")))
    assert(id.getMessage.contains("spark.wap.id=w1"))
    // Also on a WAP branch read, the WAP message explains the failure
    val onBranch =
      intercept[UnsupportedOperationException](checkWrite(Branch("dev"), Some("dev"), None))
    assert(onBranch.getMessage.contains("spark.wap.branch=dev"))
  }

  test("spark.wap.branch and spark.wap.id count only when set, and main is no WAP branch") {
    for ((value, expected) <- Seq("" -> None, "main" -> None, "audit" -> Some("audit"))) {
      withConf("spark.wap.branch" -> value)(assert(wapBranch(spark.conf) == expected, value))
    }
    assert(wapBranch(spark.conf).isEmpty)
    withConf("spark.wap.id" -> "")(assert(wapId(spark.conf).isEmpty))
    withConf("spark.wap.id" -> "w1")(assert(wapId(spark.conf).contains("w1")))
  }

  private val creds = Map("sfurl" -> "acct.snowflakecomputing.com:443", "sfuser" -> "u",
    "sfpassword" -> "p")
  private val schema = StructType(Seq(StructField("id", IntegerType)))

  private def withConf[T](kv: (String, String))(f: => T): T = {
    spark.conf.set(kv._1, kv._2)
    try f finally spark.conf.unset(kv._1)
  }

  /**
   * A fallback table whose refs and TIMESTAMP AS OF snapshots come from `refs` and `asOf` instead
   * of Snowflake; counts the lookups of each.
   */
  private class StubbedTable(
      spec: Spec, refs: => Map[String, String], asOf: Long => Option[Long] = _ => None) {
    var lookups = 0
    var asOfLookups = Seq.empty[Long]
    val table = new SnowflakeFallbackTable("fb.db.secret", creds + ("sfdatabase" -> "MYDB"),
      dbtable, spec, (_, _) => { lookups += 1; refs },
      (_, _, micros) => { asOfLookups :+= micros; asOf(micros) })
  }

  private def stubbed(spec: Spec = Main, refs: Map[String, String] = Map.empty) =
    new StubbedTable(spec, refs)

  private def relation(params: Map[String, String]): SnowflakeFallbackTable.SnowflakeRelation =
    new DefaultSource().createRelation(spark.sqlContext, params, schema)
      .asInstanceOf[SnowflakeFallbackTable.SnowflakeRelation]

  // SnowflakeRelation is private to the connector package, so it is read by reflection
  private def paramsOf(rel: AnyRef): Parameters.MergedParameters =
    rel.getClass.getMethod("params").invoke(rel).asInstanceOf[Parameters.MergedParameters]

  private def selectSql(rel: AnyRef): String = {
    val method = rel.getClass.getDeclaredMethods.find(_.getName.endsWith("standardStatement")).get
    method.setAccessible(true)
    val statement = method.invoke(rel, Array("id"), Array.empty[Filter])
    statement.getClass.getMethod("statementString").invoke(statement).asInstanceOf[String]
  }

  test("a time travel read sends the AT clause in the SELECT") {
    val rel = relation(stubbed().table.scanParameters(Map("snapshot-id" -> "123")))
    assert(paramsOf(rel).table.isEmpty)
    assert(selectSql(rel) == s"""SELECT "ID" FROM (SELECT * FROM $sql AT(VERSION => 123))""")
    assert(!paramsOf(rel).sfExtraOptions.contains("snapshot-id"))
  }

  test("a read without options reads dbtable, and other read options reach the connector") {
    val params = stubbed().table.scanParameters(Map("partition_size_in_mb" -> "8"))
    assert(params.get("dbtable").contains(sql))
    assert(!params.contains("query"))
    assert(params.get("partition_size_in_mb").contains("8"))
    assert(params.get("sfdatabase").contains("MYDB"))
  }

  test("spark.wap.branch doesn't change a read, as for Iceberg without write.wap.enabled") {
    withConf("spark.wap.branch" -> "dev") {
      val t = stubbed(refs = Map("dev" -> "BRANCH"))
      assert(t.table.readSpec(Map.empty) == Main)
      assert(t.table.scanParameters(Map.empty).get("dbtable").contains(sql))
      assert(t.lookups == 0)
    }
  }

  test("an explicit version wins over spark.wap.branch, and no WAP branch needs no lookup") {
    withConf("spark.wap.branch" -> "dev") {
      val t = stubbed(refs = Map("dev" -> "BRANCH"))
      assert(t.table.readSpec(Map("snapshot-id" -> "123")) == Snapshot(123L))
      assert(t.lookups == 0)
    }
    withConf("spark.wap.branch" -> "main") {
      val t = stubbed()
      assert(t.table.readSpec(Map.empty) == Main)
      assert(t.lookups == 0)
    }
  }

  test("VERSION AS OF '<name>' on a fallback table reads a branch with AT(BRANCH)") {
    assert(stubbed(Named("dev"), Map("dev" -> "BRANCH")).table.readSpec(Map.empty) == Branch("dev"))
    assert(stubbed(Named("v1"), Map("v1" -> "TAG")).table.readSpec(Map.empty) == Tag("v1"))
  }

  test("a failed refs lookup reads VERSION AS OF '<name>' with VERSION_REF") {
    def failing(spec: Spec) = new StubbedTable(spec, throw new RuntimeException("no privilege"))
    assert(failing(Named("dev")).table.readSpec(Map.empty) == Tag("dev"))
    withConf("spark.wap.branch" -> "dev")(assert(failing(Main).table.readSpec(Map.empty) == Main))
  }

  test("TIMESTAMP AS OF reads the snapshot Iceberg picks from the history, by version") {
    val micros = epochMillis * 1000L + 999L
    val t = new StubbedTable(TimestampMicros(micros), Map.empty, _ => Some(42L))
    assert(t.table.readSpec(Map.empty) == Snapshot(42L))
    assert(t.asOfLookups == Seq(micros))
    assert(t.table.scanParameters(Map.empty).get("query")
      .contains(s"SELECT * FROM $sql AT(VERSION => 42)"))
    // The read options too, and no lookup for other reads
    val u = new StubbedTable(Main, Map.empty, _ => Some(7L))
    assert(u.table.readSpec(Map("as-of-timestamp" -> epochMillis.toString)) == Snapshot(7L))
    assert(u.table.readSpec(Map("snapshot-id" -> "1")) == Snapshot(1L))
    assert(u.table.readSpec(Map.empty) == Main)
    assert(u.asOfLookups == Seq(epochMillis * 1000L))
  }

  test("TIMESTAMP AS OF before the first snapshot fails, as in Iceberg") {
    val t = new StubbedTable(TimestampMicros(epochMillis * 1000L), Map.empty, _ => None)
    val e = intercept[IllegalArgumentException](t.table.readSpec(Map.empty))
    assert(e.getMessage == "Cannot find a snapshot older than 2025-12-01T00:00:00Z")
  }

  test("a failed history lookup reads TIMESTAMP AS OF with AT(TIMESTAMP)") {
    val micros = epochMillis * 1000L
    val t = new StubbedTable(TimestampMicros(micros), Map.empty,
      _ => throw new RuntimeException("Unknown function ICEBERG_TABLE_HISTORY"))
    assert(t.table.readSpec(Map.empty) == TimestampMicros(micros))
  }

  test("the TIMESTAMP AS OF lookup reads the snapshot log in milliseconds") {
    assert(snapshotAsOfQuery(dbtable, epochMillis * 1000L + 999L) ==
      "SELECT SNAPSHOT_ID FROM TABLE(\"MYDB\".INFORMATION_SCHEMA.ICEBERG_TABLE_HISTORY(" +
        "TABLE_NAME => '\"MYDB\".\"DB\".\"SECRET\"')) WHERE MADE_CURRENT_AT <= " +
        s"TO_TIMESTAMP_LTZ($epochMillis, 3) ORDER BY MADE_CURRENT_AT DESC LIMIT 1")
  }

  test("connection, table and unknown options of a read or write are ignored") {
    val t = stubbed().table
    val options = Map("sfURL" -> "evil.example.com:443", "sfRole" -> "ACCOUNTADMIN",
      "sfUser" -> "other", "sfAuthenticator" -> "externalbrowser", "proxy_host" -> "evil",
      "sfDatabase" -> "OTHER", "connection_id" -> "c1", "preactions" -> "USE ROLE ACCOUNTADMIN",
      "query" -> "SELECT * FROM OTHER.T", "dbtable" -> "OTHER.T",
      // Unknown to the connector, so it would reach JDBC as a connection property
      "role" -> "ACCOUNTADMIN",
      "Partition_Size_In_MB" -> "8")
    for (params <- Seq(t.scanParameters(options), t.writeParams(options))) {
      assert((params -- creds.keys - "sfdatabase" - "dbtable" - "usestagingtable" -
        "truncate_table") == Map("partition_size_in_mb" -> "8"))
      assert(creds.forall { case (k, v) => params.get(k).contains(v) })
      assert(params.get("sfdatabase").contains("MYDB"))
      assert(params.get("dbtable").contains(sql))
    }
    // None of them reaches JDBC as a connection property
    val jdbcProperties = paramsOf(relation(t.scanParameters(options))).sfExtraOptions.keySet
    assert(jdbcProperties.intersect(options.keySet.map(_.toLowerCase)).isEmpty)
  }

  test("a write overwrites in place, so the Iceberg table and its policies stay") {
    val params = stubbed().table.writeParams(Map("usestagingtable" -> "on",
      "TRUNCATE_TABLE" -> "off"))
    assert(params.get("usestagingtable").contains("off"))
    assert(params.get("truncate_table").contains("on"))
    val merged = Parameters.mergeParameters(params)
    assert(!merged.useStagingTable && merged.truncateTable)
  }

  test("the V1 scan pushes columns and the filters Snowflake handles, as the V1 relation") {
    val rel = relation(creds + ("dbtable" -> sql))
    val builder = new SnowflakeFallbackTable.SnowflakeScanBuilder(rel)
    val filters: Array[Filter] = Array(EqualTo("id", 1))
    assert(builder.pushFilters(filters).toSeq == rel.unhandledFilters(filters).toSeq)
    assert(builder.pushedFilters().toSeq == filters.toSeq)
    builder.pruneColumns(schema)
    assert(builder.build().readSchema() == schema)
  }

  test("a write checks spark.wap.branch and the loaded spec before writing") {
    def insert(t: SnowflakeFallbackTable): Unit = {
      val info = new LogicalWriteInfo {
        override def options(): CaseInsensitiveStringMap =
          new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
        override def queryId(): String = "q"
        override def schema(): StructType = IcebergTimeTravelSuite.this.schema
      }
      t.newWriteBuilder(info).build().asInstanceOf[V1Write].toInsertableRelation
        .insert(spark.emptyDataFrame, overwrite = false)
    }
    withConf("spark.wap.branch" -> "dev") {
      val e = intercept[UnsupportedOperationException](insert(stubbed().table))
      assert(e.getMessage.contains("spark.wap.branch=dev"))
    }
    intercept[UnsupportedOperationException](insert(stubbed(Branch("x")).table))
  }

  test("the Snowflake data source itself is unchanged") {
    val plain = new DefaultSource().createRelation(
      spark.sqlContext, Map("dbtable" -> "T", "branch" -> "x") ++ creds, schema)
    assert(plain.getClass.getSimpleName == "SnowflakeRelation")
  }
}
