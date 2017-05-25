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

import java.util.{Timer, TimerTask}

import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientException, Watch, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import org.apache.commons.io.IOUtils
import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap

import org.apache.spark.internal.Logging

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
      expirationTimer: Timer,
      expiredResourceTtlMs: Long,
      resourceCleanupIntervalMs: Long)
    extends StagedResourcesExpirationManager {

  private val activeResources = TrieMap.empty[String, MonitoredResource]

  override def startMonitoringForExpiredResources(): Unit = {
    expirationTimer.scheduleAtFixedRate(
        new CleanupTask(), resourceCleanupIntervalMs, resourceCleanupIntervalMs)
  }

  override def monitorResourceForExpiration(
      resourceId: String,
      podNamespace: String,
      podLabels: Map[String, String],
      kubernetesCredentials: PodMonitoringCredentials): Unit = {
    activeResources(resourceId) = MonitoredResource(
        resourceId, podNamespace, podLabels, kubernetesCredentials)
  }

  private class CleanupTask extends TimerTask with Logging {
    override def run(): Unit = {
      // Make a copy so we can iterate through this while modifying activeResources
      val activeResourcesCopy = Map.apply(activeResources.toSeq: _*)
      for ((resourceId, resource) <- activeResourcesCopy) {
        val kubernetesClient = kubernetesClientProvider.getKubernetesClient(
            resource.podNamespace, resource.kubernetesCredentials)
        var shouldCloseKubernetesClient = true
        try {
          val podsWithLabels = kubernetesClient
              .pods()
              .withLabels(resource.podLabels.asJava)
              .list()
          if (podsWithLabels.getItems.isEmpty) {
            shouldCloseKubernetesClient = false
            activeResources.remove(resourceId)
            try {
              val expiringResource = new ExpiringResource(kubernetesClient, resource)
              expiringResource.countDownExpiration()
            } catch {
              case e: Throwable =>
                // Add back the resource if we failed to start the expiration process
                activeResources(resourceId) = resource
                throw e
            }
          }
        } catch {
          case e: Throwable =>
            shouldCloseKubernetesClient = true
            logWarning(s"Encountered an error while examining resource with" +
              s" id $resourceId for cleanup.", e)
        } finally {
          if (shouldCloseKubernetesClient) {
            IOUtils.closeQuietly(kubernetesClient)
          }
        }
      }
    }
  }

  private[spark] class ExpiringResource(
      kubernetesClient: KubernetesClient, monitoredResource: MonitoredResource) extends Logging {

    private var cancelExpirationWatch: Option[Watch] = _
    private val expirationTask = new ExpirationTask()
    private val cancelExpirationWatcher = new CancelExpirationWatcher()

    def countDownExpiration(): Unit = {
      try {
        cancelExpirationWatch = Some(kubernetesClient
          .pods()
          .withLabels(monitoredResource.podLabels.asJava)
          .watch(cancelExpirationWatcher))
        expirationTimer.schedule(expirationTask, expiredResourceTtlMs)
      } catch {
        case e: Throwable =>
          closeKubernetesConnections()
          throw e
      }
    }

    private def closeKubernetesConnections(): Unit = {
      cancelExpirationWatch.foreach(IOUtils.closeQuietly)
      IOUtils.closeQuietly(kubernetesClient)
    }

    private class ExpirationTask extends TimerTask {
      override def run(): Unit = {
        try {
          if (kubernetesClient
            .pods()
            .withLabels(monitoredResource.podLabels.asJava)
            .list()
            .getItems
            .isEmpty) {
            stagedResourcesStore.removeResources(monitoredResource.resourceId)
          }
        } finally {
          closeKubernetesConnections()
        }
      }

      override def cancel(): Boolean = {
        try {
          val successfulCancellation = super.cancel()
          if (successfulCancellation) {
            monitorResourceForExpiration(
              monitoredResource.resourceId,
              monitoredResource.podNamespace,
              monitoredResource.podLabels,
              monitoredResource.kubernetesCredentials)
          } else {
            logWarning(s"Failed to cancel expiration task for resource with" +
              s" id: ${monitoredResource.resourceId}.")
          }
          successfulCancellation
        } finally {
          closeKubernetesConnections()
        }
      }
    }

    private class CancelExpirationWatcher extends Watcher[Pod] {
      override def eventReceived(action: Action, resource: Pod): Unit = {
        action match {
          case Action.ADDED | Action.MODIFIED =>
            expirationTask.cancel()
          case other =>
            logDebug(s"Ignoring action on pod with type $other")
        }
      }

      override def onClose(cause: KubernetesClientException): Unit = {
        logDebug("Resource expiration watcher closed.", cause)
      }
    }
  }
}


