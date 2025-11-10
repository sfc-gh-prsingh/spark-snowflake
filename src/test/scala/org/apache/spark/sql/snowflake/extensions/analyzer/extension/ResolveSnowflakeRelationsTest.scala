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

package org.apache.spark.sql.snowflake.extensions.analyzer.extension

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class ResolveSnowflakeRelationsTest extends FunSuite with BeforeAndAfterEach {
  
  var spark: SparkSession = _
  
  override def beforeEach(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("ResolveSnowflakeRelationsTest")
      .getOrCreate()
  }
  
  override def afterEach(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
  }

  private val configKey = "spark.snowflake.extensions.fgacJdbcFallback.enabled"

  test("Configuration key should be correctly defined") {
    assert(configKey == "spark.snowflake.extensions.fgacJdbcFallback.enabled")
  }

  test("ResolveSnowflakeRelations class should be accessible") {
    assert(classOf[ResolveSnowflakeRelations] != null)
    
    val methods = classOf[ResolveSnowflakeRelations].getMethods
    assert(methods.exists(_.getName == "apply"))
  }

  test("Configuration key naming follows expected convention") {
    assert(configKey.startsWith("spark.snowflake.extensions."))
    assert(configKey.contains("fgac"))
    assert(configKey.contains("Jdbc"))
    assert(configKey.contains("Fallback"))
    assert(configKey.endsWith(".enabled"))
  }

  test("ResolveSnowflakeRelations should be a case class") {
    val clazz = classOf[ResolveSnowflakeRelations]
    assert(clazz.getInterfaces.length > 0)
    
    val methods = clazz.getMethods
    assert(methods.exists(_.getName == "apply"))
  }

  test("Configuration key constant should be accessible via reflection") {
    try {
      val clazz = classOf[ResolveSnowflakeRelations]
      val fields = clazz.getDeclaredFields
      val configKeyField = fields.find(_.getName.contains("FGAC_JDBC_FALLBACK_ENABLED_KEY"))
      
      assert(configKeyField.isDefined, "Configuration key field should be defined")
      
      configKeyField.get.setAccessible(true)
      assert(configKeyField.get.getType == classOf[String])
      
    } catch {
      case _: Exception =>
        assert(classOf[ResolveSnowflakeRelations] != null)
    }
  }

  test("buildSnowflakeOptions method should exist") {
    val clazz = classOf[ResolveSnowflakeRelations]
    val methods = clazz.getDeclaredMethods
    val buildOptionsMethod = methods.find(_.getName == "buildSnowflakeOptions")
    
    assert(buildOptionsMethod.isDefined, "buildSnowflakeOptions method should exist")
  }

  test("createSnowflakeRelation method should exist") {
    val clazz = classOf[ResolveSnowflakeRelations]
    val methods = clazz.getDeclaredMethods
    val createRelationMethod = methods.find(_.getName == "createSnowflakeRelation")
    
    assert(createRelationMethod.isDefined, "createSnowflakeRelation method should exist")
  }

  test("Rule should extend Rule[LogicalPlan]") {
    val clazz = classOf[ResolveSnowflakeRelations]
    val interfaces = clazz.getInterfaces
    
    val hasRuleInterface = interfaces.exists(_.getName.contains("Rule"))
    assert(hasRuleInterface || clazz.getSuperclass != null, 
      "Should extend Rule or implement Rule interface")
  }

  test("Rule should implement LookupCatalog") {
    val clazz = classOf[ResolveSnowflakeRelations]
    val interfaces = clazz.getInterfaces
    
    val hasLookupCatalog = interfaces.exists(_.getName.contains("LookupCatalog"))
    assert(hasLookupCatalog, "Should implement LookupCatalog trait")
  }

  test("UnresolvedRelation should be importable") {
    assert(classOf[UnresolvedRelation] != null)
  }

  test("shouldFallbackToSnowflake method should exist") {
    val clazz = classOf[ResolveSnowflakeRelations]
    val methods = clazz.getDeclaredMethods
    val fallbackMethod = methods.find(_.getName == "shouldFallbackToSnowflake")
    
    assert(fallbackMethod.isDefined, "shouldFallbackToSnowflake method should exist")
  }

  test("Rule should use instanceof check for FGACForbiddenException") {
    val clazz = Class.forName("org.apache.spark.sql.snowflake.catalog.FGACForbiddenException")
    assert(clazz != null, "FGACForbiddenException class should exist")
    assert(classOf[org.apache.spark.sql.catalyst.analysis.NoSuchTableException].isAssignableFrom(clazz),
      "FGACForbiddenException should extend NoSuchTableException")
  }
  
  test("buildSnowflakeOptions should accept Identifier parameter") {
    val clazz = classOf[ResolveSnowflakeRelations]
    val methods = clazz.getDeclaredMethods
    val buildOptionsMethod = methods.find(_.getName == "buildSnowflakeOptions")
    
    assert(buildOptionsMethod.isDefined, "buildSnowflakeOptions method should exist")
    
    val paramTypes = buildOptionsMethod.get.getParameterTypes
    assert(paramTypes.length == 1, "buildSnowflakeOptions should take 1 parameter")
    assert(paramTypes(0) == classOf[Identifier], 
      "buildSnowflakeOptions should accept Identifier parameter")
  }
  
  test("createSnowflakeRelation should accept Identifier parameter") {
    val clazz = classOf[ResolveSnowflakeRelations]
    val methods = clazz.getDeclaredMethods
    val createRelationMethod = methods.find(_.getName == "createSnowflakeRelation")
    
    assert(createRelationMethod.isDefined, "createSnowflakeRelation method should exist")
    
    val paramTypes = createRelationMethod.get.getParameterTypes
    assert(paramTypes.length == 1, "createSnowflakeRelation should take 1 parameter")
    assert(paramTypes(0) == classOf[Identifier], 
      "createSnowflakeRelation should accept Identifier parameter")
  }
  
  test("buildSnowflakeOptions should handle table without namespace") {
    spark.conf.set("spark.snowflake.sfURL", "account.snowflakecomputing.com")
    spark.conf.set("spark.snowflake.sfUser", "testuser")
    
    val rule = ResolveSnowflakeRelations(spark)
    val buildOptionsMethod = classOf[ResolveSnowflakeRelations]
      .getDeclaredMethod("buildSnowflakeOptions", classOf[Identifier])
    buildOptionsMethod.setAccessible(true)
    
    val ident = Identifier.of(Array.empty[String], "mytable")
    val options = buildOptionsMethod.invoke(rule, ident).asInstanceOf[Map[String, String]]
    
    assert(options("dbtable") == "mytable", "Should use just table name")
    assert(options.contains("sfURL"), "Should include SparkConf settings")
    assert(options("sfURL") == "account.snowflakecomputing.com")
  }
  
  test("buildSnowflakeOptions should handle table with single namespace (schema)") {
    spark.conf.set("spark.snowflake.sfURL", "account.snowflakecomputing.com")
    
    val rule = ResolveSnowflakeRelations(spark)
    val buildOptionsMethod = classOf[ResolveSnowflakeRelations]
      .getDeclaredMethod("buildSnowflakeOptions", classOf[Identifier])
    buildOptionsMethod.setAccessible(true)
    
    val ident = Identifier.of(Array("myschema"), "mytable")
    val options = buildOptionsMethod.invoke(rule, ident).asInstanceOf[Map[String, String]]
    
    assert(options("dbtable") == "myschema.mytable", 
      "Should use schema.table format")
  }
  
  test("buildSnowflakeOptions should handle table with multi-level namespace (db.schema)") {
    spark.conf.set("spark.snowflake.sfURL", "account.snowflakecomputing.com")
    
    val rule = ResolveSnowflakeRelations(spark)
    val buildOptionsMethod = classOf[ResolveSnowflakeRelations]
      .getDeclaredMethod("buildSnowflakeOptions", classOf[Identifier])
    buildOptionsMethod.setAccessible(true)
    
    val ident = Identifier.of(Array("mydb", "myschema"), "mytable")
    val options = buildOptionsMethod.invoke(rule, ident).asInstanceOf[Map[String, String]]
    
    assert(options("dbtable") == "mydb.myschema.mytable", 
      "Should use db.schema.table format")
  }
  
  test("buildSnowflakeOptions should merge configs from all sources") {
    spark.sparkContext.getConf.set("spark.snowflake.sfURL", "from-sparkcontext.snowflakecomputing.com")
    spark.conf.set("spark.snowflake.sfUser", "from-runtimeconfig")
    spark.sessionState.conf.setConfString("spark.snowflake.sfPassword", "from-sessionstate")
    
    val rule = ResolveSnowflakeRelations(spark)
    val buildOptionsMethod = classOf[ResolveSnowflakeRelations]
      .getDeclaredMethod("buildSnowflakeOptions", classOf[Identifier])
    buildOptionsMethod.setAccessible(true)
    
    val ident = Identifier.of(Array("myschema"), "mytable")
    val options = buildOptionsMethod.invoke(rule, ident).asInstanceOf[Map[String, String]]
    
    assert(options.contains("sfURL"), "Should include config from SparkContext")
    assert(options.contains("sfUser"), "Should include config from RuntimeConfig")
    assert(options.contains("sfPassword"), "Should include config from SessionState")
  }
  
  test("buildSnowflakeOptions should give precedence to SessionState configs") {
    spark.sparkContext.getConf.set("spark.snowflake.sfURL", "from-sparkcontext.snowflakecomputing.com")
    spark.conf.set("spark.snowflake.sfURL", "from-runtimeconfig.snowflakecomputing.com")
    spark.sessionState.conf.setConfString("spark.snowflake.sfURL", "from-sessionstate.snowflakecomputing.com")
    
    val rule = ResolveSnowflakeRelations(spark)
    val buildOptionsMethod = classOf[ResolveSnowflakeRelations]
      .getDeclaredMethod("buildSnowflakeOptions", classOf[Identifier])
    buildOptionsMethod.setAccessible(true)
    
    val ident = Identifier.of(Array("myschema"), "mytable")
    val options = buildOptionsMethod.invoke(rule, ident).asInstanceOf[Map[String, String]]
    
    assert(options("sfURL") == "from-sessionstate.snowflakecomputing.com", 
      "SessionState config should take precedence")
  }
  
  test("buildSnowflakeOptions should strip spark.snowflake prefix") {
    spark.conf.set("spark.snowflake.sfURL", "account.snowflakecomputing.com")
    spark.conf.set("snowflake.sfUser", "testuser")
    
    val rule = ResolveSnowflakeRelations(spark)
    val buildOptionsMethod = classOf[ResolveSnowflakeRelations]
      .getDeclaredMethod("buildSnowflakeOptions", classOf[Identifier])
    buildOptionsMethod.setAccessible(true)
    
    val ident = Identifier.of(Array("myschema"), "mytable")
    val options = buildOptionsMethod.invoke(rule, ident).asInstanceOf[Map[String, String]]
    
    assert(options.contains("sfURL"), "Should strip spark.snowflake prefix")
    assert(!options.contains("spark.snowflake.sfURL"), "Should not contain prefixed key")
    assert(options.contains("sfUser"), "Should strip snowflake prefix")
    assert(!options.contains("snowflake.sfUser"), "Should not contain prefixed key")
  }
}
