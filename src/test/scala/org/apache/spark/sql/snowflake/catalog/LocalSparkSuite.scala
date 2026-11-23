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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.Table
import org.scalatest.{BeforeAndAfterAll, Suite}

/** A local SparkSession for the whole suite, configured by `configure`. */
trait LocalSparkSuite extends BeforeAndAfterAll { self: Suite =>

  protected var spark: SparkSession = _

  protected def configure(builder: SparkSession.Builder): SparkSession.Builder = builder

  override def beforeAll(): Unit = {
    super.beforeAll()
    // spark.sql.extensions is static: stop any session leaked by an earlier suite in this JVM
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())
    spark = configure(SparkSession.builder()
      .master("local[1]")
      .config("spark.ui.enabled", "false")).getOrCreate()
  }

  override def afterAll(): Unit = try {
    if (spark != null) spark.stop()
  } finally {
    super.afterAll()
  }
}

object LocalSparkSuite {

  /** Iceberg's extensions only, as for Iceberg's own SparkCatalog. */
  def withIceberg(builder: SparkSession.Builder): SparkSession.Builder = builder
    // Iceberg probes optional classes on executors; don't route misses to the artifact server
    .config("spark.sql.artifact.isolation.enabled", "false")
    .config("spark.sql.extensions",
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

  /** The Snowflake table a protected table falls back to. */
  def fallbackTable(table: Table): SnowflakeFallbackTable = {
    assert(table.isInstanceOf[SnowflakeFallbackTable], s"expected a fallback table, got $table")
    table.asInstanceOf[SnowflakeFallbackTable]
  }
}
