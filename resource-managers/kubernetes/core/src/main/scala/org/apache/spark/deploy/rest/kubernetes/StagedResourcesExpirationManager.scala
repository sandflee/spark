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
package org.apache.spark.deploy.rest.kubernetes

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.util.{Clock, Utils}

private[spark] trait StagedResourcesExpirationManager {

  def startMonitoringForExpiredResources(): Unit

  def monitorResourceForExpiration(
      resourceId: String,
      podNamespace: String,
      podLabels: Map[String, String],
      kubernetesCredentials: PodMonitoringCredentials): Unit
}

private class StagedResourcesExpirationManagerImpl(
      stagedResourcesStore: StagedResourcesStore,
      kubernetesClientProvider: ResourceStagingServiceKubernetesClientProvider,
      expirationExecutorService: ScheduledExecutorService,
      clock: Clock,
      expiredResourceTtlMs: Long,
      resourceCleanupIntervalMs: Long)
    extends StagedResourcesExpirationManager {

  private val activeResources = TrieMap.empty[String, MonitoredResource]

  override def startMonitoringForExpiredResources(): Unit = {
    expirationExecutorService.scheduleAtFixedRate(
        new CleanupRunnable(),
        resourceCleanupIntervalMs,
        resourceCleanupIntervalMs,
        TimeUnit.MILLISECONDS)
  }

  override def monitorResourceForExpiration(
      resourceId: String,
      podNamespace: String,
      podLabels: Map[String, String],
      kubernetesCredentials: PodMonitoringCredentials): Unit = {
    activeResources(resourceId) = MonitoredResource(
        resourceId, podNamespace, podLabels, kubernetesCredentials)
  }

  private class CleanupRunnable extends Runnable with Logging {

    private val expiringResources = mutable.Map.empty[String, ExpiringResource]

    override def run(): Unit = {
      // Make a copy so we can iterate through this while modifying activeResources
      val activeResourcesCopy = Map.apply(activeResources.toSeq: _*)
      for ((resourceId, resource) <- activeResourcesCopy) {
        Utils.tryWithResource(
            kubernetesClientProvider.getKubernetesClient(
                resource.podNamespace, resource.kubernetesCredentials)) { kubernetesClient =>
          val podsWithLabels = kubernetesClient
            .pods()
            .withLabels(resource.podLabels.asJava)
            .list()
          if (podsWithLabels.getItems.isEmpty) {
            activeResources.remove(resourceId)
            val expiresAt = clock.getTimeMillis() + expiredResourceTtlMs
            expiringResources(resourceId) = ExpiringResource(expiresAt, resource)
          }
        }
      }

      val expiringResourcesCopy = Map.apply(expiringResources.toSeq: _*)
      for ((resourceId, expiringResource) <- expiringResourcesCopy) {
        Utils.tryWithResource(
            kubernetesClientProvider.getKubernetesClient(
                expiringResource.resource.podNamespace,
                expiringResource.resource.kubernetesCredentials)) { kubernetesClient =>
          val podsWithLabels = kubernetesClient
            .pods()
            .withLabels(expiringResource.resource.podLabels.asJava)
            .list()
          if (!podsWithLabels.getItems.isEmpty) {
            activeResources(resourceId) = expiringResource.resource
            expiringResources.remove(resourceId)
          } else if (clock.getTimeMillis() > expiringResource.expiresAt) {
            stagedResourcesStore.removeResources(resourceId)
            expiringResources.remove(resourceId)
          }
        }
      }
    }
  }

  private case class MonitoredResource(
      resourceId: String,
      podNamespace: String,
      podLabels: Map[String, String],
      kubernetesCredentials: PodMonitoringCredentials)

  private case class ExpiringResource(expiresAt: Long, resource: MonitoredResource)
}


