/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.columnar.InMemoryRelation
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

/** Holds a cached logical plan and its data */
private[sql] case class CachedData(plan: LogicalPlan, cachedRepresentation: InMemoryRelation)

/**
 * Provides support in a SQLContext for caching query results and automatically using these cached
 * results when subsequent queries are executed.  Data is cached using byte buffers stored in an
 * InMemoryRelation.  This relation is automatically substituted query plans that return the
 * `sameResult` as the originally cached query.
 *
 * Internal to Spark SQL.
 */
private[sql] class CacheManager(sqlContext: SQLContext) extends Logging {

  @transient
  private val cachedData = new scala.collection.mutable.ArrayBuffer[CachedData]

  @transient
  private val cacheLock = new ReentrantReadWriteLock

  /** Returns true if the table is currently cached in-memory. */
  def isCached(tableName: String): Boolean = lookupCachedData(sqlContext.table(tableName)).nonEmpty

  /** Caches the specified table in-memory. */
  def cacheTable(tableName: String): Unit = cacheQuery(sqlContext.table(tableName), Some(tableName))

  /** Removes the specified table from the in-memory cache. */
  def uncacheTable(tableName: String): Unit = uncacheQuery(sqlContext.table(tableName))

  /** Acquires a read lock on the cache for the duration of `f`. */
  private def readLock[A](f: => A): A = {
    val lock = cacheLock.readLock()
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  /** Acquires a write lock on the cache for the duration of `f`. */
  private def writeLock[A](f: => A): A = {
    val lock = cacheLock.writeLock()
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  /** Clears all cached tables. */
  private[sql] def clearCache(): Unit = writeLock {
    cachedData.foreach(_.cachedRepresentation.cachedColumnBuffers.unpersist())
    cachedData.clear()
  }

  /** Checks if the cache is empty. */
  private[sql] def isEmpty: Boolean = readLock {
    cachedData.isEmpty
  }

  /**
   * Caches the data produced by the logical representation of the given [[DataFrame]]. Unlike
   * `RDD.cache()`, the default storage level is set to be `MEMORY_AND_DISK` because recomputing
   * the in-memory columnar representation of the underlying table is expensive.
   */
  private[sql] def autoCachePlan( planToCache: LogicalPlan,
                                  executedPlan: SparkPlan,
                                  storageLevel: StorageLevel = MEMORY_AND_DISK): Unit = writeLock {
    logDebug(s"autoCachePlan: \nplan to cache:\n[${planToCache.toString}]")
    logDebug(s"executed plan:\n[${executedPlan.toString}]")
    if (lookupCachedData(planToCache).nonEmpty) {
      logWarning("Asked to cache already cached data.")
    } else {
      cachedData +=
        CachedData(
          planToCache,
          InMemoryRelation(
            sqlContext.conf.useCompression,
            sqlContext.conf.columnBatchSize,
            storageLevel,
            executedPlan,
            None))
    }
  }

  /**
   * Caches the data produced by the logical representation of the given [[DataFrame]]. Unlike
   * `RDD.cache()`, the default storage level is set to be `MEMORY_AND_DISK` because recomputing
   * the in-memory columnar representation of the underlying table is expensive.
   */
  private[sql] def cacheQuery(
      query: DataFrame,
      tableName: Option[String] = None,
      storageLevel: StorageLevel = MEMORY_AND_DISK): Unit = writeLock {
    val planToCache = query.queryExecution.analyzed
    if (lookupCachedData(planToCache).nonEmpty) {
      logWarning("Asked to cache already cached data.")
    } else {
      cachedData +=
        CachedData(
          planToCache,
          InMemoryRelation(
            sqlContext.conf.useCompression,
            sqlContext.conf.columnBatchSize,
            storageLevel,
            sqlContext.executePlan(query.logicalPlan).executedPlan,
            tableName))
    }
  }

  /** Removes the data for the given [[DataFrame]] from the cache */
  private[sql] def uncacheQuery(query: DataFrame, blocking: Boolean = true): Unit = writeLock {
    val planToCache = query.queryExecution.analyzed
    val dataIndex = cachedData.indexWhere(cd => planToCache.sameResult(cd.plan))
    require(dataIndex >= 0, s"Table $query is not cached.")
    cachedData(dataIndex).cachedRepresentation.uncache(blocking)
    cachedData.remove(dataIndex)
  }

  /** Tries to remove the data for the given [[DataFrame]] from the cache if it's cached */
  private[sql] def tryUncacheQuery(
      query: DataFrame,
      blocking: Boolean = true): Boolean = writeLock {
    val planToCache = query.queryExecution.analyzed
    val dataIndex = cachedData.indexWhere(cd => planToCache.sameResult(cd.plan))
    val found = dataIndex >= 0
    if (found) {
      cachedData(dataIndex).cachedRepresentation.cachedColumnBuffers.unpersist(blocking)
      cachedData.remove(dataIndex)
    }
    found
  }

  /** Optionally returns cached data for the given [[DataFrame]] */
  private[sql] def lookupCachedData(query: DataFrame): Option[CachedData] = readLock {
    lookupCachedData(query.queryExecution.analyzed)
  }

  /** Optionally returns cached data for the given LogicalPlan. */
  private[sql] def lookupCachedData(plan: LogicalPlan): Option[CachedData] = readLock {
    cachedData.find(cd => plan.sameResult(cd.plan))
  }

  /** Optionally returns a cached table that is similar to plan. The structure of logicalPlans
    * are the same, but we allow the cached table to have different predicates than plan).
    * Returns a similar cached table and a predicate. By applying the predicate on cached table,
    * we get logically the same result as plan. */
  private[sql] def lookupSimilarCachedData(plan: LogicalPlan): (Option[CachedData],
    Option[org.apache.spark.sql.catalyst.plans.logical.Filter]) = readLock {
    logDebug(s"LookupSimilar")
    cachedData.find(cd => plan.sameResultIfApplyingFilter(cd.plan)) match {
      case Some(cd) => (Some(cd), plan.findFilterToMakeSameResult(cd.plan))
      case None => {
        logDebug(s"not matching any cache")
        (None, None)
      }
    }
  }

  /** Replaces segments of the given logical plan with cached versions where possible. */
  private[sql] def useCachedData(plan: LogicalPlan): LogicalPlan = {
    plan transformDown {
      case currentFragment => getCachedData(currentFragment)
    }
  }


  private[sql] def getCachedData(subPlan: LogicalPlan): LogicalPlan = {
    logDebug(s"getCachedData")
    lookupCachedData(subPlan) match {
      case Some(cd) => cd.cachedRepresentation.withOutput(subPlan.output)
      case None => {
        lookupSimilarCachedData(subPlan) match {
          case (Some(cd), Some(f)) => applyFilterOnCachedData(subPlan, cd, f)
          case _ => subPlan
        }
      }
    }
  }

  //cd.map(_.cachedRepresentation.withOutput(subPlan.output))
  //.getOrElse(subPlan)        //  find filter
  //  add filter
  // transform
  private[sql] def applyFilterOnCachedData(subPlan: LogicalPlan, cd: CachedData,
                                           f: org.apache.spark.sql.catalyst.plans.logical.Filter): LogicalPlan = {
    logDebug(s"applyFilterOnCachedData: [${subPlan.argString}}] with filter [${f.condition.toString}]")
    val newFilter = org.apache.spark.sql.catalyst.plans.logical.Filter(f.condition,
      cd.cachedRepresentation.withOutput(subPlan.output))
    newFilter
  }

  /**
   * Invalidates the cache of any data that contains `plan`. Note that it is possible that this
   * function will over invalidate.
   */
  private[sql] def invalidateCache(plan: LogicalPlan): Unit = writeLock {
    cachedData.foreach {
      case data if data.plan.collect { case p if p.sameResult(plan) => p }.nonEmpty =>
        data.cachedRepresentation.recache()
      case _ =>
    }
  }
}
