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

import java.io.File

import com.google.common.base.Charsets
import com.google.common.io.BaseEncoding
import io.fabric8.kubernetes.client.{Config, ConfigBuilder, DefaultKubernetesClient, KubernetesClient}
import io.fabric8.kubernetes.client.utils.HttpClientUtils
import okhttp3.{ConnectionPool, Dispatcher}

import org.apache.spark.util.ThreadUtils

private[spark] trait ResourceStagingServiceKubernetesClientProvider {
  def getKubernetesClient(
      namespace: String,
      credentials: StagedResourcesOwnerMonitoringCredentials): KubernetesClient
}

private class OptionConfigurableConfigBuilder(configBuilder: ConfigBuilder) {
  def withOption[T]
      (option: Option[T])
      (configurator: ((T, ConfigBuilder) => ConfigBuilder)): OptionConfigurableConfigBuilder = {
    new OptionConfigurableConfigBuilder(option.map { opt =>
      configurator(opt, configBuilder)
    }.getOrElse(configBuilder))
  }

  def build(): Config = configBuilder.build()
}

private[spark] class ResourceStagingServiceKubernetesClientProviderImpl(
      apiServerUrl: String,
      caCertFile: Option[File])
    extends ResourceStagingServiceKubernetesClientProvider {

  private val connectionPool = new ConnectionPool()
  private val dispatcher = new Dispatcher(
      ThreadUtils.newDaemonCachedThreadPool("kubernetes-dispatcher"))

  override def getKubernetesClient(
      namespace: String, credentials: StagedResourcesOwnerMonitoringCredentials)
      : KubernetesClient = {
    val clientConfig = new OptionConfigurableConfigBuilder(new ConfigBuilder()
      .withMasterUrl(apiServerUrl)
      .withNamespace(namespace))
      .withOption(caCertFile) {
        (caCertFile, configBuilder) => configBuilder.withCaCertFile(caCertFile.getAbsolutePath)
      }.withOption(credentials.oauthTokenBase64) {
        (oauthTokenBase64, configBuilder) =>
            configBuilder.withOauthToken(
                new String(BaseEncoding.base64().decode(oauthTokenBase64), Charsets.UTF_8))
      }.withOption(credentials.clientKeyDataBase64) {
        (clientKeyDataBase64, configBuilder) =>
            configBuilder.withClientKeyData(
              new String(BaseEncoding.base64().decode(clientKeyDataBase64), Charsets.UTF_8))
      }.withOption(credentials.clientCertDataBase64) {
        (clientCertDataBase64, configBuilder) =>
            configBuilder.withClientCertData(
              new String(BaseEncoding.base64().decode(clientCertDataBase64), Charsets.UTF_8))
      }.build()
    val baseHttpClient = HttpClientUtils.createHttpClient(clientConfig)
    val httpClientWithReusedComponents = baseHttpClient.newBuilder()
      .connectionPool(connectionPool)
      .dispatcher(dispatcher)
      .build()
    new DefaultKubernetesClient(httpClientWithReusedComponents, clientConfig)
  }

  private implicit def withOption[T]
      (option: Option[T])
      (configurator: ((T, ConfigBuilder) => ConfigBuilder))
      (configBuilder: ConfigBuilder): ConfigBuilder = {
    option.map { opt =>
      configurator(opt, configBuilder)
    }.getOrElse(configBuilder)
  }
}
