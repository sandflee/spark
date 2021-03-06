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
package org.apache.spark.deploy.kubernetes.submit

import io.fabric8.kubernetes.api.model.ConfigMap

import org.apache.spark.{SparkConf, SSLOptions}
import org.apache.spark.deploy.kubernetes.{InitContainerResourceStagingServerSecretPluginImpl, OptionRequirements, SparkPodInitContainerBootstrap, SparkPodInitContainerBootstrapImpl}
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.deploy.rest.kubernetes.RetrofitClientFactoryImpl
import org.apache.spark.util.Utils

/**
 * Interface that wraps the provision of everything the submission client needs to set up the
 * driver's init-container. This is all wrapped in the same place to ensure that related
 * components are being constructed with consistent configurations with respect to one another.
 */
private[spark] trait DriverInitContainerComponentsProvider {

  def provideContainerLocalizedFilesResolver(
      mainAppResource: String): ContainerLocalizedFilesResolver
  def provideInitContainerSubmittedDependencyUploader(
      driverPodLabels: Map[String, String]): Option[SubmittedDependencyUploader]
  def provideSubmittedDependenciesSecretBuilder(
      maybeSubmittedResourceSecrets: Option[SubmittedResourceSecrets])
      : Option[SubmittedDependencySecretBuilder]
  def provideInitContainerBootstrap(): SparkPodInitContainerBootstrap
  def provideDriverPodFileMounter(): DriverPodKubernetesFileMounter
  def provideInitContainerBundle(maybeSubmittedResourceIds: Option[SubmittedResourceIds],
      uris: Iterable[String]): Option[InitContainerBundle]
}

private[spark] class DriverInitContainerComponentsProviderImpl(
      sparkConf: SparkConf,
      kubernetesResourceNamePrefix: String,
      namespace: String,
      sparkJars: Seq[String],
      sparkFiles: Seq[String],
      pySparkFiles: Seq[String],
      resourceStagingServerExternalSslOptions: SSLOptions)
    extends DriverInitContainerComponentsProvider {

  private val maybeResourceStagingServerUri = sparkConf.get(RESOURCE_STAGING_SERVER_URI)
  private val maybeResourceStagingServerInternalUri =
      sparkConf.get(RESOURCE_STAGING_SERVER_INTERNAL_URI)
  private val maybeResourceStagingServerInternalTrustStore =
      sparkConf.get(RESOURCE_STAGING_SERVER_INTERNAL_TRUSTSTORE_FILE)
          .orElse(sparkConf.get(RESOURCE_STAGING_SERVER_TRUSTSTORE_FILE))
  private val maybeResourceStagingServerInternalTrustStorePassword =
      sparkConf.get(RESOURCE_STAGING_SERVER_INTERNAL_TRUSTSTORE_PASSWORD)
          .orElse(sparkConf.get(RESOURCE_STAGING_SERVER_TRUSTSTORE_PASSWORD))
  private val maybeResourceStagingServerInternalTrustStoreType =
      sparkConf.get(RESOURCE_STAGING_SERVER_INTERNAL_TRUSTSTORE_TYPE)
          .orElse(sparkConf.get(RESOURCE_STAGING_SERVER_TRUSTSTORE_TYPE))
  private val maybeResourceStagingServerInternalClientCert =
      sparkConf.get(RESOURCE_STAGING_SERVER_INTERNAL_CLIENT_CERT_PEM)
          .orElse(sparkConf.get(RESOURCE_STAGING_SERVER_CLIENT_CERT_PEM))
  private val resourceStagingServerInternalSslEnabled =
      sparkConf.get(RESOURCE_STAGING_SERVER_INTERNAL_SSL_ENABLED)
          .orElse(sparkConf.get(RESOURCE_STAGING_SERVER_SSL_ENABLED))
          .getOrElse(false)

  OptionRequirements.requireNandDefined(
      maybeResourceStagingServerInternalClientCert,
      maybeResourceStagingServerInternalTrustStore,
      "Cannot provide both a certificate file and a trustStore file for init-containers to" +
        " use for contacting the resource staging server over TLS.")

  require(maybeResourceStagingServerInternalTrustStore.forall { trustStore =>
    Option(Utils.resolveURI(trustStore).getScheme).getOrElse("file") match {
      case "file" | "local" => true
      case _ => false
    }
  }, "TrustStore URI used for contacting the resource staging server from init containers must" +
    " have no scheme, or scheme file://, or scheme local://.")

  require(maybeResourceStagingServerInternalClientCert.forall { trustStore =>
    Option(Utils.resolveURI(trustStore).getScheme).getOrElse("file") match {
      case "file" | "local" => true
      case _ => false
    }
  }, "Client cert file URI used for contacting the resource staging server from init containers" +
    " must have no scheme, or scheme file://, or scheme local://.")

  private val jarsDownloadPath = sparkConf.get(INIT_CONTAINER_JARS_DOWNLOAD_LOCATION)
  private val filesDownloadPath = sparkConf.get(INIT_CONTAINER_FILES_DOWNLOAD_LOCATION)
  private val maybeSecretName = maybeResourceStagingServerUri.map { _ =>
    s"$kubernetesResourceNamePrefix-init-secret"
  }
  private val configMapName = s"$kubernetesResourceNamePrefix-init-config"
  private val configMapKey = s"$kubernetesResourceNamePrefix-init-config-key"
  private val initContainerImage = sparkConf.get(INIT_CONTAINER_DOCKER_IMAGE)
  private val dockerImagePullPolicy = sparkConf.get(DOCKER_IMAGE_PULL_POLICY)
  private val downloadTimeoutMinutes = sparkConf.get(INIT_CONTAINER_MOUNT_TIMEOUT)
  private val pySparkSubmitted = KubernetesFileUtils.getOnlySubmitterLocalFiles(pySparkFiles)

  private def provideInitContainerConfigMap(
      maybeSubmittedResourceIds: Option[SubmittedResourceIds]): ConfigMap = {
    val submittedDependencyConfigPlugin = for {
      stagingServerUri <- maybeResourceStagingServerUri
      jarsResourceId <- maybeSubmittedResourceIds.map(_.jarsResourceId)
      filesResourceId <- maybeSubmittedResourceIds.map(_.filesResourceId)
    } yield {
      new SubmittedDependencyInitContainerConfigPluginImpl(
          // Configure the init-container with the internal URI over the external URI.
          maybeResourceStagingServerInternalUri.getOrElse(stagingServerUri),
          jarsResourceId,
          filesResourceId,
          INIT_CONTAINER_SUBMITTED_JARS_SECRET_KEY,
          INIT_CONTAINER_SUBMITTED_FILES_SECRET_KEY,
          INIT_CONTAINER_STAGING_SERVER_TRUSTSTORE_SECRET_KEY,
          INIT_CONTAINER_STAGING_SERVER_CLIENT_CERT_SECRET_KEY,
          resourceStagingServerInternalSslEnabled,
          maybeResourceStagingServerInternalTrustStore,
          maybeResourceStagingServerInternalClientCert,
          maybeResourceStagingServerInternalTrustStorePassword,
          maybeResourceStagingServerInternalTrustStoreType,
          INIT_CONTAINER_SECRET_VOLUME_MOUNT_PATH)
    }
    new SparkInitContainerConfigMapBuilderImpl(
        sparkJars,
        sparkFiles ++ pySparkSubmitted,
        jarsDownloadPath,
        filesDownloadPath,
        configMapName,
        configMapKey,
        submittedDependencyConfigPlugin).build()
  }

  override def provideContainerLocalizedFilesResolver(mainAppResource: String)
    : ContainerLocalizedFilesResolver = {
    new ContainerLocalizedFilesResolverImpl(
        sparkJars, sparkFiles, pySparkFiles, mainAppResource, jarsDownloadPath, filesDownloadPath)
  }

  private def provideExecutorInitContainerConfiguration(): ExecutorInitContainerConfiguration = {
    new ExecutorInitContainerConfigurationImpl(
        maybeSecretName,
        INIT_CONTAINER_SECRET_VOLUME_MOUNT_PATH,
        configMapName,
        configMapKey)
  }

  override def provideInitContainerSubmittedDependencyUploader(
      driverPodLabels: Map[String, String]): Option[SubmittedDependencyUploader] = {
    maybeResourceStagingServerUri.map { stagingServerUri =>
      new SubmittedDependencyUploaderImpl(
          driverPodLabels,
          namespace,
          stagingServerUri,
          sparkJars,
          sparkFiles ++ pySparkSubmitted,
          resourceStagingServerExternalSslOptions,
          RetrofitClientFactoryImpl)
    }
  }

  override def provideSubmittedDependenciesSecretBuilder(
      maybeSubmittedResourceSecrets: Option[SubmittedResourceSecrets])
      : Option[SubmittedDependencySecretBuilder] = {
    for {
      secretName <- maybeSecretName
      jarsResourceSecret <- maybeSubmittedResourceSecrets.map(_.jarsResourceSecret)
      filesResourceSecret <- maybeSubmittedResourceSecrets.map(_.filesResourceSecret)
    } yield {
      new SubmittedDependencySecretBuilderImpl(
          secretName,
          jarsResourceSecret,
          filesResourceSecret,
          INIT_CONTAINER_SUBMITTED_JARS_SECRET_KEY,
          INIT_CONTAINER_SUBMITTED_FILES_SECRET_KEY,
          INIT_CONTAINER_STAGING_SERVER_TRUSTSTORE_SECRET_KEY,
          INIT_CONTAINER_STAGING_SERVER_CLIENT_CERT_SECRET_KEY,
          maybeResourceStagingServerInternalTrustStore,
          maybeResourceStagingServerInternalClientCert)
    }
  }

  override def provideInitContainerBootstrap(): SparkPodInitContainerBootstrap = {
    val resourceStagingServerSecretPlugin = maybeSecretName.map { secret =>
      new InitContainerResourceStagingServerSecretPluginImpl(
          secret, INIT_CONTAINER_SECRET_VOLUME_MOUNT_PATH)
    }
    new SparkPodInitContainerBootstrapImpl(
        initContainerImage,
        dockerImagePullPolicy,
        jarsDownloadPath,
        filesDownloadPath,
        downloadTimeoutMinutes,
        configMapName,
        configMapKey,
        resourceStagingServerSecretPlugin)
  }
  override def provideDriverPodFileMounter(): DriverPodKubernetesFileMounter = {
    new DriverPodKubernetesFileMounterImpl()
  }
  override def provideInitContainerBundle(
      maybeSubmittedResourceIds: Option[SubmittedResourceIds],
      uris: Iterable[String]): Option[InitContainerBundle] = {
    // Bypass init-containers if `spark.jars` and `spark.files` and '--py-rilfes'
    // is empty or only has `local://` URIs
    if ((KubernetesFileUtils.getNonContainerLocalFiles(uris) ++ pySparkSubmitted).nonEmpty) {
      Some(InitContainerBundle(provideInitContainerConfigMap(maybeSubmittedResourceIds),
        provideInitContainerBootstrap(),
        provideExecutorInitContainerConfiguration()))
    } else None
  }
}
