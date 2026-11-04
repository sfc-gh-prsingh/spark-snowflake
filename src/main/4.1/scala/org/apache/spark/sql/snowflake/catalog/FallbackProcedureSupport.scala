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

import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, ProcedureCatalog}
import org.apache.spark.sql.connector.catalog.procedures.UnboundProcedure

/** Exposes the delegate's procedures (e.g. Iceberg system.fast_forward) via Spark's CALL. */
private[catalog] trait FallbackProcedureSupport extends ProcedureCatalog {

  protected def procedureDelegate: CatalogPlugin

  override def loadProcedure(ident: Identifier): UnboundProcedure = procedureDelegate match {
    case pc: ProcedureCatalog => pc.loadProcedure(ident)
    case other =>
      throw new UnsupportedOperationException(
        s"Delegate catalog ${other.getClass.getName} does not support procedures")
  }

  override def listProcedures(namespace: Array[String]): Array[Identifier] =
    procedureDelegate match {
      case pc: ProcedureCatalog => pc.listProcedures(namespace)
      case _ => Array.empty
    }
}
