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

import org.apache.iceberg.CatalogUtil
import org.apache.iceberg.exceptions.ForbiddenException
import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.spark.SparkCatalog
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.snowflake.catalog.LocalSparkSuite.fallbackTable
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite
import scala.collection.JavaConverters._

class SnowflakeFallbackCatalogTest extends AnyFunSuite {

  // The fallback reads ONLY the immutable SparkConf, so connector properties must be set when
  // the SparkContext is created. SparkCatalog.initialize also needs an active session.
  private def withSpark[T](conf: Map[String, String] = Map.empty)(f: SparkSession => T): T = {
    SparkSession.getDefaultSession.foreach(_.stop())
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    val spark = SparkSession.builder()
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .config(conf)
      .getOrCreate()
    try {
      SparkSession.setActiveSession(spark)
      f(spark)
    } finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  private def newCatalog(name: String, options: (String, String)*): ForbiddingFallbackCatalog = {
    val catalog = new ForbiddingFallbackCatalog()
    catalog.initialize(name, new CaseInsensitiveStringMap(options.toMap.asJava))
    catalog
  }

  private def name(dotted: String): SnowflakeTableName = SnowflakeTableName(dotted.split('.').toSeq)

  private def forbidden(ex: Throwable): Boolean = {
    val catalog = new SnowflakeFallbackCatalog()
    val method = catalog.getClass.getDeclaredMethod("isForbiddenException", classOf[Throwable])
    method.setAccessible(true)
    method.invoke(catalog, ex).asInstanceOf[Boolean]
  }

  test("isForbiddenException detects Iceberg's ForbiddenException, also when wrapped") {
    val forbiddenEx = new ForbiddenException("Forbidden: Access to table %s is denied", "db.t")
    assert(forbidden(forbiddenEx))
    assert(forbidden(new RuntimeException("wrapper", forbiddenEx)))
    assert(forbidden(new RuntimeException("a", new IllegalStateException("b", forbiddenEx))))
  }

  test("isForbiddenException ignores 403 or Forbidden in other errors' messages") {
    // Names and IDs in messages contain 403 (db.t.snapshot_id_5010424037564433155, sales_403)
    assert(!forbidden(new RuntimeException("Some other error")))
    assert(!forbidden(new RuntimeException("HTTP 403 Forbidden")))
    assert(!forbidden(new org.apache.spark.sql.catalyst.analysis.NoSuchTableException(
      Identifier.of(Array("db", "secret"), "snapshot_id_5010424037564433155"))))
    assert(!forbidden(new org.apache.iceberg.exceptions.NoSuchNamespaceException(
      "Namespace does not exist: sales_403")))
  }

  test("the catalog is an Iceberg SparkCatalog") {
    // Iceberg plans branch/tag DDL and procedures only for SparkCatalog instances
    assert(classOf[SparkCatalog].isAssignableFrom(classOf[SnowflakeFallbackCatalog]))
  }

  test("loadTable should return the Iceberg table when no 403 error occurs") {
    withSpark(Map(
      "spark.sql.catalog.fb" -> classOf[ForbiddingFallbackCatalog].getName,
      "spark.sql.catalog.fb.hadoop-warehouse" ->
        Files.createTempDirectory("fallback-ok").toUri.toString)) { spark =>
      // The Hadoop catalog creates the namespace with the table
      spark.sql("CREATE TABLE fb.schema.t (id INT) USING iceberg")
      val catalog = spark.sessionState.catalogManager.catalog("fb").asInstanceOf[TableCatalog]
      assert(catalog.loadTable(Identifier.of(Array("schema"), "t")).isInstanceOf[SparkTable])
    }
  }

  test("loadTable should fall back to a Snowflake table on 403") {
    withSpark(Map(
      "spark.snowflake.url" -> "test.snowflakecomputing.com",
      "spark.snowflake.user" -> "testuser",
      "spark.snowflake.password" -> "testpass",
      "spark.snowflake.database" -> "testdb",
      "spark.snowflake.schema" -> "testschema")) { _ =>
      val catalog = newCatalog("test_catalog", "forbidden-tables" -> "testschema.testtable")
      val result = catalog.loadTable(Identifier.of(Array("testschema"), "testtable"))

      val table = fallbackTable(result)
      assert(table.table == name("testschema.testtable"))
      assert(table.spec == IcebergTimeTravel.Main)
      assert(table.params.get("url").contains("test.snowflakecomputing.com"),
        "Should have url from SparkSession config")
      assert(table.params.get("user").contains("testuser"),
        "Should have user from SparkSession config")
    }
  }

  test("loadTable should propagate non-403 exceptions") {
    withSpark() { _ =>
      val catalog = newCatalog("test", "failing-tables" -> "schema.table")
      val thrown = intercept[RuntimeException] {
        catalog.loadTable(Identifier.of(Array("schema"), "table"))
      }
      assert(thrown.getMessage == "Some other error")
    }
  }

  test("initialize should set catalog name") {
    withSpark() { _ =>
      assert(newCatalog("test_catalog").name() === "test_catalog")
    }
  }

  test("catalog-impl from before the catalog extended SparkCatalog") {
    withSpark() { _ =>
      // Naming SparkCatalog (the old delegate) is redundant and keeps working
      val legacy = newCatalog("test", "catalog-impl" -> classOf[SparkCatalog].getName)
      assert(legacy.icebergOptions.get("catalog-impl") == null)

      // Any other Spark catalog can no longer be the delegate
      val thrown = intercept[IllegalArgumentException] {
        newCatalog("test", "catalog-impl" -> "org.apache.iceberg.spark.SparkSessionCatalog")
      }
      assert(thrown.getMessage.contains("catalog-impl") && thrown.getMessage.contains("type"))
    }
  }

  test("catalog-impl naming an Iceberg Catalog configures Iceberg's catalog") {
    withSpark() { _ =>
      for (impl <- Seq("org.apache.iceberg.rest.RESTCatalog", "com.example.NotOnClasspath")) {
        assert(newCatalog("test", "catalog-impl" -> impl).icebergOptions.get("catalog-impl") == impl)
      }
    }
  }

  test("type defaults to rest unless type or an Iceberg catalog-impl is set") {
    withSpark() { _ =>
      def catalogType(options: (String, String)*): String =
        newCatalog("test", options: _*).icebergOptions.get(CatalogUtil.ICEBERG_CATALOG_TYPE)
      val rest = CatalogUtil.ICEBERG_CATALOG_TYPE_REST
      assert(catalogType() == rest)
      // The old config, with only catalog-impl=SparkCatalog, gets Horizon's REST catalog too
      assert(catalogType("catalog-impl" -> classOf[SparkCatalog].getName) == rest)
      assert(catalogType("TYPE" -> "hadoop") == "hadoop")
      assert(catalogType("catalog-impl" -> classOf[RESTCatalog].getName) == null)
    }
  }

  test("User-Agent header should be passed to the Iceberg catalog") {
    withSpark() { _ =>
      val catalog = newCatalog("test_catalog",
        "catalog-impl" -> classOf[SparkCatalog].getName,
        "custom-option" -> "custom-value",
        "snowflake-database" -> "MY_DB_2")
      val options = catalog.icebergOptions

      val userAgent = options.get("header.User-Agent")
      assert(userAgent != null, "User-Agent header should be set")
      val expectedUserAgent =
        s"spark-snowflake/${net.snowflake.spark.snowflake.Utils.VERSION} spark/${SPARK_VERSION}"
      assert(userAgent == expectedUserAgent, s"Expected '$expectedUserAgent', got '$userAgent'")
      val pattern = """spark-snowflake/\d+\.\d+\.\d+ spark/\d+\.\d+\.\d+""".r
      assert(pattern.findFirstIn(userAgent).isDefined,
        s"User-Agent should match pattern spark-snowflake/X.Y.Z spark/X.Y.Z")

      assert(options.get("custom-option") == "custom-value")
      assert(options.get("catalog-impl") == null)
      assert(options.get("snowflake-database") == null)
    }
  }

  test("namespace and table listing come from the Iceberg catalog") {
    withSpark() { _ =>
      val catalog = newCatalog("test")
      catalog.createNamespace(Array("schema"), java.util.Collections.emptyMap())
      assert(catalog.listNamespaces().map(_.toSeq).toSeq == Seq(Seq("schema")))
      assert(catalog.listTables(Array("schema")).isEmpty)
    }
  }

  test("tableExists should return false on exception") {
    withSpark() { _ =>
      val catalog = newCatalog("test", "failing-tables" -> "schema.table")
      assert(!catalog.tableExists(Identifier.of(Array("schema"), "table")))
    }
  }

  test("tableExists should return true for a protected table") {
    withSpark() { _ =>
      val catalog = newCatalog("test", "forbidden-tables" -> "schema.table")
      assert(catalog.tableExists(Identifier.of(Array("schema"), "table")))
    }
  }

  test("dbtable property should be constructed from Identifier namespace and name") {
    // buildFullTableName is private[snowflake], so it is accessible to tests in this package
    val catalog = new SnowflakeFallbackCatalog()

    // Multi-part namespace (Snowflake: database.schema.table)
    assert(catalog.buildFullTableName(
      Identifier.of(Array("production_db", "analytics_schema"), "sales_table"), None) ===
      name("production_db.analytics_schema.sales_table"))
    // Single-part namespace (Snowflake: schema.table)
    assert(catalog.buildFullTableName(
      Identifier.of(Array("analytics_schema"), "sales_table"), None) ===
      name("analytics_schema.sales_table"))
    // No namespace (Snowflake: table only)
    assert(catalog.buildFullTableName(Identifier.of(Array(), "sales_table"), None) ===
      name("sales_table"))
    // Deep multi-part namespace (e.g., catalog.database.schema.table)
    assert(catalog.buildFullTableName(
      Identifier.of(Array("catalog", "production_db", "analytics_schema"), "sales_table"),
      None) === name("catalog.production_db.analytics_schema.sales_table"))
    // Special characters are kept, each part quoted as a Snowflake identifier
    val special = catalog.buildFullTableName(Identifier.of(Array("my_db", "my-schema"), "my.table"), None)
    assert(special === SnowflakeTableName(Seq("my_db", "my-schema", "my.table")))
    assert(special.sql === "\"MY_DB\".\"my-schema\".\"my.table\"")
  }

  // Hardening: a runtime SQLConf value (what a Spark-SQL `SET` writes) must NOT
  // leak into the privileged fallback connector options; only immutable SparkConf should flow.
  test("runtime SQLConf values must not leak into fallback table options (dbee43e2)") {
    withSpark(Map(
      "spark.snowflake.url" -> "test.snowflakecomputing.com",
      "spark.snowflake.user" -> "testuser")) { spark =>
      // Simulate a malicious `SET snowflake.preactions=...` (writes to runtime SQLConf).
      spark.conf.set("snowflake.preactions", "GRANT ROLE ACCOUNTADMIN TO USER attacker")
      val catalog = newCatalog("test_catalog", "forbidden-tables" -> "testschema.testtable")
      val props = fallbackTable(
        catalog.loadTable(Identifier.of(Array("testschema"), "testtable"))).params

      assert(props.get("preactions").isEmpty,
        s"runtime SET value leaked into fallback table options: ${props.get("preactions")}")
      // Legit admin SparkConf value must still flow through.
      assert(props.get("url").contains("test.snowflakecomputing.com"),
        "admin SparkConf 'url' should still be present in fallback table options")
    }
  }

  test("snowflake-database catalog option should qualify dbtable") {
    withSpark() { _ =>
      val catalog = newCatalog("alias", "snowflake-database" -> "MY_DB_2")
      assert(catalog.buildFullTableName(Identifier.of(Array("MY_SCHEMA"), "MY_TABLE"),
        catalog.resolveDatabase(None)) === name("MY_DB_2.MY_SCHEMA.MY_TABLE"))
      intercept[IllegalArgumentException] {
        catalog.buildFullTableName(Identifier.of(Array("A", "B"), "MY_TABLE"),
          catalog.resolveDatabase(None))
      }
      intercept[IllegalArgumentException] {
        catalog.buildFullTableName(Identifier.of(Array.empty[String], "MY_TABLE"),
          catalog.resolveDatabase(None))
      }
    }
  }

  test("blank snowflake-database should fall back to the warehouse") {
    withSpark() { _ =>
      val catalog = newCatalog("alias", "snowflake-database" -> "  ", "warehouse" -> "MY_DB_1")
      assert(catalog.buildFullTableName(Identifier.of(Array("MY_SCHEMA"), "MY_TABLE"),
        catalog.resolveDatabase(None)) === name("MY_DB_1.MY_SCHEMA.MY_TABLE"))
    }
  }

  test("warehouse should qualify dbtable when snowflake-database is unset") {
    withSpark() { _ =>
      val catalog = newCatalog("MY_DB_1", "warehouse" -> "MY_DB_2")
      assert(catalog.buildFullTableName(Identifier.of(Array("MY_SCHEMA"), "MY_TABLE"),
        catalog.resolveDatabase(None)) === name("MY_DB_2.MY_SCHEMA.MY_TABLE"))
      intercept[IllegalArgumentException] {
        catalog.buildFullTableName(Identifier.of(Array("A", "B"), "MY_TABLE"),
          catalog.resolveDatabase(None))
      }
      intercept[IllegalArgumentException] {
        catalog.buildFullTableName(Identifier.of(Array.empty[String], "MY_TABLE"),
          catalog.resolveDatabase(None))
      }
    }
  }

  test("session sfdatabase should replace the warehouse but not snowflake-database") {
    withSpark() { _ =>
      val byWarehouse = newCatalog("alias", "warehouse" -> "MY_DB_2")
      assert(byWarehouse.resolveDatabase(Some("MY_DB_1")).isEmpty)

      val explicit = newCatalog("alias",
        "snowflake-database" -> "MY_DB_3", "warehouse" -> "MY_DB_2")
      assert(explicit.resolveDatabase(Some("MY_DB_1")).contains("MY_DB_3"))
    }
  }

  test("each catalog registered in Spark should use its own warehouse as the database") {
    val impl = classOf[ForbiddingFallbackCatalog].getName
    withSpark(Map(
      "spark.sql.catalog.cat1" -> impl,
      "spark.sql.catalog.cat1.warehouse" -> "MY_DB_1",
      "spark.sql.catalog.cat2" -> impl,
      "spark.sql.catalog.cat2.warehouse" -> "MY_DB_2",
      "spark.sql.catalog.cat3" -> impl,
      "spark.sql.catalog.cat3.warehouse" -> "MY_DB_2",
      "spark.sql.catalog.cat3.snowflake-database" -> "MY_DB_3")) { spark =>
      def dbtable(name: String): SnowflakeTableName = {
        val catalog = spark.sessionState.catalogManager.catalog(name)
          .asInstanceOf[SnowflakeFallbackCatalog]
        catalog.buildFullTableName(Identifier.of(Array("MY_SCHEMA"), "MY_TABLE"),
          catalog.resolveDatabase(None))
      }

      assert(dbtable("cat1") === name("MY_DB_1.MY_SCHEMA.MY_TABLE"))
      assert(dbtable("cat2") === name("MY_DB_2.MY_SCHEMA.MY_TABLE"))
      assert(dbtable("cat3") === name("MY_DB_3.MY_SCHEMA.MY_TABLE"))
    }
  }

  test("resolved database should default sfdatabase only when session does not set it") {
    def fallbackProps(
        sessionConf: Map[String, String],
        catalogOptions: Map[String, String] = Map("snowflake-database" -> "MY_DB_2"))
        : Map[String, String] = withSpark(sessionConf) { _ =>
      val catalog = newCatalog("alias",
        (catalogOptions + ("forbidden-tables" -> "MY_SCHEMA.MY_TABLE")).toSeq: _*)
      val table = fallbackTable(catalog.loadTable(Identifier.of(Array("MY_SCHEMA"), "MY_TABLE")))
      table.params + ("table" -> table.table.parts.mkString("."))
    }

    val defaulted = fallbackProps(Map.empty)
    assert(defaulted.get("sfdatabase").contains("MY_DB_2"))
    assert(defaulted.get("table").contains("MY_DB_2.MY_SCHEMA.MY_TABLE"))
    val sessionSet = fallbackProps(Map("spark.snowflake.sfDatabase" -> "MY_DB_1"))
    assert(sessionSet.get("sfdatabase").contains("MY_DB_1"))
    assert(sessionSet.get("table").contains("MY_DB_2.MY_SCHEMA.MY_TABLE"))
    val fromWarehouse = fallbackProps(Map.empty, Map("warehouse" -> "MY_DB_3"))
    assert(fromWarehouse.get("sfdatabase").contains("MY_DB_3"))
    assert(fromWarehouse.get("table").contains("MY_DB_3.MY_SCHEMA.MY_TABLE"))
    val sessionOverWarehouse = fallbackProps(
      Map("spark.snowflake.sfDatabase" -> "MY_DB_1"), Map("warehouse" -> "MY_DB_3"))
    assert(sessionOverWarehouse.get("sfdatabase").contains("MY_DB_1"))
    assert(sessionOverWarehouse.get("table").contains("MY_SCHEMA.MY_TABLE"))
  }
}
