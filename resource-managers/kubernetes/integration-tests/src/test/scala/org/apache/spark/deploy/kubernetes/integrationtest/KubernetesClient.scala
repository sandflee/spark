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

package org.apache.spark.deploy.kubernetes.integrationtest

import io.fabric8.kubernetes.client.{ConfigBuilder, DefaultKubernetesClient}

import org.apache.spark.deploy.kubernetes.config.resolveK8sMaster
import org.apache.spark.deploy.kubernetes.integrationtest.docker.SparkDockerImageBuilder
import org.apache.spark.deploy.kubernetes.integrationtest.minikube.Minikube

object TestBackend extends Enumeration {
  val SingleNode, MultiNode = Value
}

object KubernetesClient {
  var defaultClient: DefaultKubernetesClient = _
  var testBackend: TestBackend.Value = _

  def getClient(): DefaultKubernetesClient = {
    if (defaultClient == null) {
      createDefaultClient
    }
    defaultClient
  }

  def cleanUp(): Unit = {
    if (testBackend == TestBackend.SingleNode
      && !System.getProperty("spark.docker.test.persistMinikube", "false").toBoolean) {
      Minikube.deleteMinikube()
    }
  }

  private def createDefaultClient(): Unit = {
    System.getProperty("spark.docker.test.master") match {
      case null =>
        Minikube.startMinikube()
        new SparkDockerImageBuilder(Minikube.getDockerEnv).buildSparkDockerImages()
        defaultClient = Minikube.getKubernetesClient
        testBackend = TestBackend.SingleNode

      case _ =>
        val master = System.getProperty("spark.docker.test.master")
        var k8ConfBuilder = new ConfigBuilder()
          .withApiVersion("v1")
          .withMasterUrl(resolveK8sMaster(master))
        val k8ClientConfig = k8ConfBuilder.build
        defaultClient = new DefaultKubernetesClient(k8ClientConfig)
        testBackend = TestBackend.MultiNode
    }
  }
}