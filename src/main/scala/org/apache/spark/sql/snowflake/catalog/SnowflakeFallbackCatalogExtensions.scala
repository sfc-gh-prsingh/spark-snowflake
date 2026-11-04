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

import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.plans.logical.{CreateOrReplaceBranch, CreateOrReplaceTag, DropBranch, DropTag, LogicalPlan}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.datasources.v2.{CreateOrReplaceBranchExec, CreateOrReplaceTagExec, DropBranchExec, DropTagExec}

import scala.collection.JavaConverters._

/**
 * Register after IcebergSparkSessionExtensions in spark.sql.extensions. Iceberg only plans
 * branch/tag DDL for its own catalog classes, so this plans it for SnowflakeFallbackCatalog
 * against the delegate catalog.
 */
class SnowflakeFallbackCatalogExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit =
    extensions.injectPlannerStrategy(spark => SnowflakeFallbackIcebergStrategy(spark))
}

case class SnowflakeFallbackIcebergStrategy(spark: SparkSession) extends SparkStrategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case CreateOrReplaceBranch(
        FallbackDelegate(catalog, ident), branch, branchOptions, create, replace, ifNotExists) =>
      CreateOrReplaceBranchExec(
        catalog, ident, branch, branchOptions, create, replace, ifNotExists) :: Nil

    case CreateOrReplaceTag(
        FallbackDelegate(catalog, ident), tag, tagOptions, create, replace, ifNotExists) =>
      CreateOrReplaceTagExec(catalog, ident, tag, tagOptions, create, replace, ifNotExists) :: Nil

    case DropBranch(FallbackDelegate(catalog, ident), branch, ifExists) =>
      DropBranchExec(catalog, ident, branch, ifExists) :: Nil

    case DropTag(FallbackDelegate(catalog, ident), tag, ifExists) =>
      DropTagExec(catalog, ident, tag, ifExists) :: Nil

    case _ => Nil
  }

  private object FallbackDelegate {
    def unapply(nameParts: Seq[String]): Option[(TableCatalog, Identifier)] = {
      val catalogAndIdentifier = Spark3Util.catalogAndIdentifier(spark, nameParts.asJava)
      catalogAndIdentifier.catalog match {
        case fallback: SnowflakeFallbackCatalog =>
          Some((fallback.delegateTableCatalog, catalogAndIdentifier.identifier))
        case _ => None
      }
    }
  }
}
