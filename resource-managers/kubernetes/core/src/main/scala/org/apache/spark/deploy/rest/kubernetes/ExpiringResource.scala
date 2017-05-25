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
import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging

private[spark] class ExpiringResource(
    kubernetesClient: KubernetesClient,
    monitoredResource: MonitoredResource,
    stagedResourcesStore: StagedResourcesStore,
    resourceTtlMs: Long,
    expirationTimer: Timer,
    resourcesExpirationManager: StagedResourcesExpirationManager) extends Logging {

  private var cancelExpirationWatch: Option[Watch] = _

  private val cancelExpirationWatcher = new Watcher[Pod] {
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

  private val expirationTask = new TimerTask {
    override def run() = {
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
        closeAllResources()
      }
    }

    override def cancel(): Boolean = {
      try {
        val successfulCancellation = super.cancel()
        if (successfulCancellation) {
          resourcesExpirationManager.monitorResourceForExpiration(
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
        closeAllResources()
      }
    }
  }

  def countDownExpiration(): Unit = {
    try {
      cancelExpirationWatch = Some(kubernetesClient
        .pods()
        .withLabels(monitoredResource.podLabels.asJava)
        .watch(cancelExpirationWatcher))
      expirationTimer.schedule(expirationTask, resourceTtlMs)
    } catch {
      case e: Throwable =>
        closeAllResources()
        throw e
    }
  }

  private def closeAllResources(): Unit = {
    try {
      cancelExpirationWatch.foreach(_.close())
    } catch {
      case e: Throwable =>
        logWarning(s"Failed to close the expiration watch for" +
          s" resource id: ${monitoredResource.resourceId}.", e)
    }
    try {
      kubernetesClient.close()
    } catch {
      case e: Throwable =>
        logWarning("Failed to close the Kubernetes client monitoring resource with id: " +
            monitoredResource.resourceId, e)
    }
  }
}
