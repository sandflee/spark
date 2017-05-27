---
layout: global
title: Running Spark on Kubernetes
---

Support for running on [Kubernetes](https://kubernetes.io/docs/whatisk8s/) is available in experimental status. The
feature set is currently limited and not well-tested. This should not be used in production environments.

## Prerequisites

* You must have a running Kubernetes cluster with access configured to it
using [kubectl](https://kubernetes.io/docs/user-guide/prereqs/). If you do not already have a working Kubernetes
cluster, you may setup a test cluster on your local machine using
[minikube](https://kubernetes.io/docs/getting-started-guides/minikube/).
  * We recommend that minikube be updated to the most recent version (0.19.0 at the time of this documentation), as some
  earlier versions may not start up the kubernetes cluster with all the necessary components.
* You must have appropriate permissions to create and list [pods](https://kubernetes.io/docs/user-guide/pods/),
[ConfigMaps](https://kubernetes.io/docs/tasks/configure-pod-container/configmap/) and
[secrets](https://kubernetes.io/docs/concepts/configuration/secret/) in your cluster. You can verify that
you can list these resources by running `kubectl get pods` `kubectl get configmap`, and `kubectl get secrets` which
should give you a list of pods and configmaps (if any) respectively.
* You must have a spark distribution with Kubernetes support. This may be obtained from the
[release tarball](https://github.com/apache-spark-on-k8s/spark/releases) or by
[building Spark with Kubernetes support](../resource-managers/kubernetes/README.md#building-spark-with-kubernetes-support).

## Driver & Executor Images

Kubernetes requires users to supply images that can be deployed into containers within pods. The images are built to
be run in a container runtime environment that Kubernetes supports. Docker is a container runtime environment that is
frequently used with Kubernetes, so Spark provides some support for working with Docker to get started quickly.

If you wish to use pre-built docker images, you may use the images published in
[kubespark](https://hub.docker.com/u/kubespark/). The images are as follows:

<table class="table">
<tr><th>Component</th><th>Image</th></tr>
<tr>
  <td>Spark Driver Image</td>
  <td><code>kubespark/spark-driver:v2.1.0-kubernetes-0.1.0-alpha.2</code></td>
</tr>
<tr>
  <td>Spark Executor Image</td>
  <td><code>kubespark/spark-executor:v2.1.0-kubernetes-0.1.0-alpha.2</code></td>
</tr>
<tr>
  <td>Spark Initialization Image</td>
  <td><code>kubespark/spark-init:v2.1.0-kubernetes-0.1.0-alpha.2</code></td>
</tr>
</table>

You may also build these docker images from sources, or customize them as required. Spark distributions include the
Docker files for the driver, executor, and init-container at `dockerfiles/driver/Dockerfile`,
`dockerfiles/executor/Dockerfile`, and `dockerfiles/init-container/Dockerfile` respectively. Use these Docker files to
build the Docker images, and then tag them with the registry that the images should be sent to. Finally, push the images
to the registry.

For example, if the registry host is `registry-host` and the registry is listening on port 5000:

    cd $SPARK_HOME
    docker build -t registry-host:5000/spark-driver:latest -f dockerfiles/driver/Dockerfile .
    docker build -t registry-host:5000/spark-executor:latest -f dockerfiles/executor/Dockerfile .
    docker build -t registry-host:5000/spark-init:latest -f dockerfiles/init-container/Dockerfile .
    docker push registry-host:5000/spark-driver:latest
    docker push registry-host:5000/spark-executor:latest
    docker push registry-host:5000/spark-init:latest

## Submitting Applications to Kubernetes

Kubernetes applications can be executed via `spark-submit`. For example, to compute the value of pi, assuming the images
are set up as described above:

    bin/spark-submit \
      --deploy-mode cluster \
      --class org.apache.spark.examples.SparkPi \
      --master k8s://https://<k8s-apiserver-host>:<k8s-apiserver-port> \
      --kubernetes-namespace default \
      --conf spark.executor.instances=5 \
      --conf spark.app.name=spark-pi \
      --conf spark.kubernetes.driver.docker.image=kubespark/spark-driver:v2.1.0-kubernetes-0.1.0-alpha.2 \
      --conf spark.kubernetes.executor.docker.image=kubespark/spark-executor:v2.1.0-kubernetes-0.1.0-alpha.2 \
      --conf spark.kubernetes.initcontainer.docker.image=kubespark/spark-init:v2.1.0-kubernetes-0.1.0-alpha.2 \
      local:///opt/spark/examples/jars/spark_examples_2.11-2.2.0.jar

The Spark master, specified either via passing the `--master` command line argument to `spark-submit` or by setting
`spark.master` in the application's configuration, must be a URL with the format `k8s://<api_server_url>`. Prefixing the
master string with `k8s://` will cause the Spark application to launch on the Kubernetes cluster, with the API server
being contacted at `api_server_url`. If no HTTP protocol is specified in the URL, it defaults to `https`. For example,
setting the master to `k8s://example.com:443` is equivalent to setting it to `k8s://https://example.com:443`, but to
connect without TLS on a different port, the master would be set to `k8s://http://example.com:8443`.

If you have a Kubernetes cluster setup, one way to discover the apiserver URL is by executing `kubectl cluster-info`.

    > kubectl cluster-info
    Kubernetes master is running at http://127.0.0.1:8080

In the above example, the specific Kubernetes cluster can be used with spark submit by specifying
`--master k8s://http://127.0.0.1:8080` as an argument to spark-submit.

Note that applications can currently only be executed in cluster mode, where the driver and its executors are running on
the cluster.

Finally, notice that in the above example we specify a jar with a specific URI with a scheme of `local://`. This URI is
the location of the example jar that is already in the Docker image. Using dependencies that are on your machine's local
disk is discussed below.

## Dependency Management

Application dependencies that are being submitted from your machine need to be sent to a **resource staging server**
that the driver and executor can then communicate with to retrieve those dependencies. A YAML file denoting a minimal
set of Kubernetes resources that runs this service is located in the file `conf/kubernetes-resource-staging-server.yaml`.
This YAML file configures a Deployment with one pod running the resource staging server configured with a ConfigMap,
and exposes the server through a Service with a fixed NodePort. Deploying a resource staging server with the included
YAML file requires you to have permissions to create Deployments, Services, and ConfigMaps.

To run the resource staging server with default configurations, the Kubernetes resources can be created:

    kubectl create -f conf/kubernetes-resource-staging-server.yaml

and then you can compute the value of Pi as follows:

    bin/spark-submit \
      --deploy-mode cluster \
      --class org.apache.spark.examples.SparkPi \
      --master k8s://<k8s-apiserver-host>:<k8s-apiserver-port> \
      --kubernetes-namespace default \
      --conf spark.executor.instances=5 \
      --conf spark.app.name=spark-pi \
      --conf spark.kubernetes.driver.docker.image=kubespark/spark-driver:v2.1.0-kubernetes-0.1.0-alpha.2 \
      --conf spark.kubernetes.executor.docker.image=kubespark/spark-executor:v2.1.0-kubernetes-0.1.0-alpha.2 \
      --conf spark.kubernetes.initcontainer.docker.image=kubespark/spark-init:v2.1.0-kubernetes-0.1.0-alpha.2 \
      --conf spark.kubernetes.resourceStagingServer.uri=http://<address-of-any-cluster-node>:31000 \
      examples/jars/spark_examples_2.11-2.2.0.jar

The Docker image for the resource staging server may also be built from source, in a similar manner to the driver
and executor images. The Dockerfile is provided in `dockerfiles/resource-staging-server/Dockerfile`.

The provided YAML file specifically sets the NodePort to 31000 on the service's specification. If port 31000 is not
available on any of the nodes of your cluster, you should remove the NodePort field from the service's specification
and allow the Kubernetes cluster to determine the NodePort itself. Be sure to provide the correct port in the resource
staging server URI when submitting your application, in accordance to the NodePort chosen by the Kubernetes cluster.

### Dependency Management Without The Resource Staging Server

Note that this resource staging server is only required for submitting local dependencies. If your application's
dependencies are all hosted in remote locations like HDFS or http servers, they may be referred to by their appropriate
remote URIs. Also, application dependencies can be pre-mounted into custom-built Docker images. Those dependencies
can be added to the classpath by referencing them with `local://` URIs and/or setting the `SPARK_EXTRA_CLASSPATH`
environment variable in your Dockerfiles.

### Accessing Kubernetes Clusters

For details about running on public cloud environments, such as Google Container Engine (GKE), refer to [running Spark in the cloud with Kubernetes](running-on-kubernetes-cloud.md).

Spark-submit also supports submission through the
[local kubectl proxy](https://kubernetes.io/docs/user-guide/accessing-the-cluster/#using-kubectl-proxy). One can use the
authenticating proxy to communicate with the api server directly without passing credentials to spark-submit.

The local proxy can be started by running:

    kubectl proxy

If our local proxy were listening on port 8001, we would have our submission looking like the following:

    bin/spark-submit \
      --deploy-mode cluster \
      --class org.apache.spark.examples.SparkPi \
      --master k8s://http://127.0.0.1:8001 \
      --kubernetes-namespace default \
      --conf spark.executor.instances=5 \
      --conf spark.app.name=spark-pi \
      --conf spark.kubernetes.driver.docker.image=kubespark/spark-driver:v2.1.0-kubernetes-0.1.0-alpha.2 \
      --conf spark.kubernetes.executor.docker.image=kubespark/spark-executor:v2.1.0-kubernetes-0.1.0-alpha.2 \
      --conf spark.kubernetes.initcontainer.docker.image=kubespark/spark-init:v2.1.0-kubernetes-0.1.0-alpha.2 \
      local:///opt/spark/examples/jars/spark_examples_2.11-2.2.0.jar

Communication between Spark and Kubernetes clusters is performed using the fabric8 kubernetes-client library.
The above mechanism using `kubectl proxy` can be used when we have authentication providers that the fabric8
kubernetes-client library does not support. Authentication using X509 Client Certs and OAuth tokens
is currently supported.

## Dynamic Executor Scaling

Spark on Kubernetes supports Dynamic Allocation with cluster mode. This mode requires running
an external shuffle service. This is typically a [daemonset](https://kubernetes.io/docs/concepts/workloads/controllers/daemonset/)
with a provisioned [hostpath](https://kubernetes.io/docs/concepts/storage/volumes/#hostpath) volume.
This shuffle service may be shared by executors belonging to different SparkJobs. Using Spark with dynamic allocation
on Kubernetes assumes that a cluster administrator has set up one or more shuffle-service daemonsets in the cluster.

A sample configuration file is provided in `conf/kubernetes-shuffle-service.yaml` which can be customized as needed
for a particular cluster. It is important to note that `spec.template.metadata.labels` are setup appropriately for the shuffle
service because there may be multiple shuffle service instances running in a cluster. The labels give Spark applications
a way to target a particular shuffle service.

For example, if the shuffle service we want to use is in the default namespace, and
has pods with labels `app=spark-shuffle-service` and `spark-version=2.1.0`, we can
use those tags to target that particular shuffle service at job launch time. In order to run a job with dynamic allocation enabled,
the command may then look like the following:

    bin/spark-submit \
      --deploy-mode cluster \
      --class org.apache.spark.examples.GroupByTest \
      --master k8s://<k8s-master>:<port> \
      --kubernetes-namespace default \
      --conf spark.app.name=group-by-test \
      --conf spark.kubernetes.driver.docker.image=kubespark/spark-driver:latest \
      --conf spark.kubernetes.executor.docker.image=kubespark/spark-executor:latest \
      --conf spark.dynamicAllocation.enabled=true \
      --conf spark.shuffle.service.enabled=true \
      --conf spark.kubernetes.shuffle.namespace=default \
      --conf spark.kubernetes.shuffle.labels="app=spark-shuffle-service,spark-version=2.1.0" \
      local:///opt/spark/examples/jars/spark_examples_2.11-2.2.0.jar 10 400000 2

## ThirdPartyResources for visibility into state of deployed Spark job

In order to expose the state of a deployed spark job to a kubernetes administrator or user, via the kubectl or the 
kubernetes dashboard, we have added a kubernetes Resource (of kind: SparkJob) storing pertinent information 
related to a specific spark job. 

Using this, we can view current and all past (if not already cleaned up) deployed spark apps within the 
current namespace using `kubectl` like so:
    
    kubectl get sparkjobs

Or via the kubernetes dashboard using the link as provided by:

    kubectl cluster-info


### Pre-requisite

Note that this resource is dependent on extending the kubernetes API using a 
[ThirdPartyResource (TPR)](https://kubernetes.io/docs/tasks/access-kubernetes-api/extend-api-third-party-resource/). 

See conf/kubernetes-custom-resource.yaml for the recommended yaml file. From the spark base directory, 
we can create the recommended TPR like so:

    kubectl create -f conf/kubernetes-custom-resource.yaml

### Important Things to note

TPRs are an alpha feature that might not be available in every cluster.
TPRs need to be manually cleaned up because garbage collection support does not exist for them yet.

### Future work

Kube administrators or users would be able to stop a spark app running in their cluster by simply 
deleting the attached resource. 


## Advanced

### Securing the Resource Staging Server with TLS

The default configuration of the resource staging server is not secured with TLS. It is highly recommended to configure
this to protect the secrets and jars/files being submitted through the staging server.

The YAML file in `conf/kubernetes-resource-staging-server.yaml` includes a ConfigMap resource that holds the resource
staging server's configuration. The properties can be adjusted here to make the resource staging server listen over TLS.
Refer to the [security](security.html) page for the available settings related to TLS. The namespace for the
resource staging server is `kubernetes.resourceStagingServer`, so for example the path to the server's keyStore would
be set by `spark.ssl.kubernetes.resourceStagingServer.keyStore`.

In addition to the settings specified by the previously linked security page, the resource staging server supports the
following additional configurations:

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.ssl.kubernetes.resourceStagingServer.keyPem</code></td>
  <td>(none)</td>
  <td>
    Private key file encoded in PEM format that the resource staging server uses to secure connections over TLS. If this
    is specified, the associated public key file must be specified in
    <code>spark.ssl.kubernetes.resourceStagingServer.serverCertPem</code>. PEM files and a keyStore file (set by
    <code>spark.ssl.kubernetes.resourceStagingServer.keyStore</code>) cannot both be specified at the same time.
  </td>
</tr>
<tr>
  <td><code>spark.ssl.kubernetes.resourceStagingServer.serverCertPem</code></td>
  <td>(none)</td>
  <td>
    Certificate file encoded in PEM format that the resource staging server uses to secure connections over TLS. If this
    is specified, the associated private key file must be specified in
    <code>spark.ssl.kubernetes.resourceStagingServer.keyPem</code>. PEM files and a keyStore file (set by
    <code>spark.ssl.kubernetes.resourceStagingServer.keyStore</code>) cannot both be specified at the same time.
  </td>
</tr>
<tr>
  <td><code>spark.ssl.kubernetes.resourceStagingServer.keyStorePasswordFile</code></td>
  <td>(none)</td>
  <td>
    Provides the KeyStore password through a file in the container instead of a static value. This is useful if the
    keyStore's password is to be mounted into the container with a secret.
  </td>
</tr>
<tr>
  <td><code>spark.ssl.kubernetes.resourceStagingServer.keyPasswordFile</code></td>
  <td>(none)</td>
  <td>
    Provides the keyStore's key password using a file in the container instead of a static value. This is useful if the
    keyStore's key password is to be mounted into the container with a secret.
  </td>
</tr>
</table>

Note that while the properties can be set in the ConfigMap, you will still need to consider the means of mounting the
appropriate secret files into the resource staging server's container. A common mechanism that is used for this is
to use [Kubernetes secrets](https://kubernetes.io/docs/concepts/configuration/secret/) that are mounted as secret
volumes. Refer to the appropriate Kubernetes documentation for guidance and adjust the resource staging server's
specification in the provided YAML file accordingly.

Finally, when you submit your application, you must specify either a trustStore or a PEM-encoded certificate file to
communicate with the resource staging server over TLS. The trustStore can be set with
`spark.ssl.kubernetes.resourceStagingServer.trustStore`, or a certificate file can be set with
`spark.ssl.kubernetes.resourceStagingServer.clientCertPem`. For example, our SparkPi example now looks like this:

    bin/spark-submit \
      --deploy-mode cluster \
      --class org.apache.spark.examples.SparkPi \
      --master k8s://https://<k8s-apiserver-host>:<k8s-apiserver-port> \
      --kubernetes-namespace default \
      --conf spark.executor.instances=5 \
      --conf spark.app.name=spark-pi \
      --conf spark.kubernetes.driver.docker.image=kubespark/spark-driver:v2.1.0-kubernetes-0.1.0-alpha.2 \
      --conf spark.kubernetes.executor.docker.image=kubespark/spark-executor:v2.1.0-kubernetes-0.1.0-alpha.2 \
      --conf spark.kubernetes.initcontainer.docker.image=kubespark/spark-init:v2.1.0-kubernetes-0.1.0-alpha.2 \
      --conf spark.kubernetes.resourceStagingServer.uri=https://<address-of-any-cluster-node>:31000 \
      --conf spark.ssl.kubernetes.resourceStagingServer.enabled=true \
      --conf spark.ssl.kubernetes.resourceStagingServer.clientCertPem=/home/myuser/cert.pem \
      examples/jars/spark_examples_2.11-2.2.0.jar

### Spark Properties

Below are some other common properties that are specific to Kubernetes. Most of the other configurations are the same
from the other deployment modes. See the [configuration page](configuration.html) for more information on those.

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.kubernetes.namespace</code></td>
  <td><code>default</code></td>
  <td>
    The namespace that will be used for running the driver and executor pods. When using
    <code>spark-submit</code> in cluster mode, this can also be passed to <code>spark-submit</code> via the
    <code>--kubernetes-namespace</code> command line argument.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.driver.docker.image</code></td>
  <td><code>spark-driver:2.2.0</code></td>
  <td>
    Docker image to use for the driver. Specify this using the standard
    <a href="https://docs.docker.com/engine/reference/commandline/tag/">Docker tag</a> format.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.executor.docker.image</code></td>
  <td><code>spark-executor:2.2.0</code></td>
  <td>
    Docker image to use for the executors. Specify this using the standard
    <a href="https://docs.docker.com/engine/reference/commandline/tag/">Docker tag</a> format.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.initcontainer.docker.image</code></td>
  <td><code>spark-init:2.2.0</code></td>
  <td>
    Docker image to use for the init-container that is run before the driver and executor containers. Specify this using
    the standard <a href="https://docs.docker.com/engine/reference/commandline/tag/">Docker tag</a> format. The
    init-container is responsible for fetching application dependencies from both remote locations like HDFS or S3,
    and from the resource staging server, if applicable.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.shuffle.namespace</code></td>
  <td><code>default</code></td>
  <td>
    Namespace in which the shuffle service pods are present. The shuffle service must be
    created in the cluster prior to attempts to use it.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.shuffle.labels</code></td>
  <td>(none)</td>
  <td>
    Labels that will be used to look up shuffle service pods. This should be a comma-separated list of label key-value pairs,
    where each label is in the format <code>key=value</code>. The labels chosen must be such that
    they match exactly one shuffle service pod on each node that executors are launched.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.allocation.batch.size</code></td>
  <td><code>5</code></td>
  <td>
    Number of pods to launch at once in each round of executor pod allocation.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.allocation.batch.delay</code></td>
  <td><code>1</code></td>
  <td>
    Number of seconds to wait between each round of executor pod allocation.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.submission.caCertFile</code></td>
  <td>(none)</td>
  <td>
    Path to the CA cert file for connecting to the Kubernetes API server over TLS when starting the driver. This file
    must be located on the submitting machine's disk. Specify this as a path as opposed to a URI (i.e. do not provide
    a scheme).
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.submission.clientKeyFile</code></td>
  <td>(none)</td>
  <td>
    Path to the client key file for authenticating against the Kubernetes API server when starting the driver. This file
    must be located on the submitting machine's disk. Specify this as a path as opposed to a URI (i.e. do not provide
    a scheme).
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.submission.clientCertFile</code></td>
  <td>(none)</td>
  <td>
    Path to the client cert file for authenticating against the Kubernetes API server when starting the driver. This
    file must be located on the submitting machine's disk. Specify this as a path as opposed to a URI (i.e. do not
    provide a scheme).
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.submission.oauthToken</code></td>
  <td>(none)</td>
  <td>
    OAuth token to use when authenticating against the Kubernetes API server when starting the driver. Note
    that unlike the other authentication options, this is expected to be the exact string value of the token to use for
    the authentication.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.driver.caCertFile</code></td>
  <td>(none)</td>
  <td>
    Path to the CA cert file for connecting to the Kubernetes API server over TLS from the driver pod when requesting
    executors. This file must be located on the submitting machine's disk, and will be uploaded to the driver pod.
    Specify this as a path as opposed to a URI (i.e. do not provide a scheme).
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.driver.clientKeyFile</code></td>
  <td>(none)</td>
  <td>
    Path to the client key file for authenticating against the Kubernetes API server from the driver pod when requesting
    executors. This file must be located on the submitting machine's disk, and will be uploaded to the driver pod.
    Specify this as a path as opposed to a URI (i.e. do not provide a scheme). If this is specified, it is highly
    recommended to set up TLS for the driver submission server, as this value is sensitive information that would be
    passed to the driver pod in plaintext otherwise.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.driver.clientCertFile</code></td>
  <td>(none)</td>
  <td>
    Path to the client cert file for authenticating against the Kubernetes API server from the driver pod when
    requesting executors. This file must be located on the submitting machine's disk, and will be uploaded to the
    driver pod. Specify this as a path as opposed to a URI (i.e. do not provide a scheme).
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.driver.oauthToken</code></td>
  <td>(none)</td>
  <td>
    OAuth token to use when authenticating against the against the Kubernetes API server from the driver pod when
    requesting executors. Note that unlike the other authentication options, this must be the exact string value of
    the token to use for the authentication. This token value is uploaded to the driver pod. If this is specified, it is
    highly recommended to set up TLS for the driver submission server, as this value is sensitive information that would
    be passed to the driver pod in plaintext otherwise.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.driver.serviceAccountName</code></td>
  <td><code>default</code></td>
  <td>
    Service account that is used when running the driver pod. The driver pod uses this service account when requesting
    executor pods from the API server. Note that this cannot be specified alongside a CA cert file, client key file,
    client cert file, and/or OAuth token.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.executor.memoryOverhead</code></td>
  <td>executorMemory * 0.10, with minimum of 384</td>
  <td>
    The amount of off-heap memory (in megabytes) to be allocated per executor. This is memory that accounts for things
    like VM overheads, interned strings, other native overheads, etc. This tends to grow with the executor size
    (typically 6-10%).
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.driver.labels</code></td>
  <td>(none)</td>
  <td>
    Custom labels that will be added to the driver pod. This should be a comma-separated list of label key-value pairs,
    where each label is in the format <code>key=value</code>. Note that Spark also adds its own labels to the driver pod
    for bookkeeping purposes.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.driver.annotations</code></td>
  <td>(none)</td>
  <td>
    Custom annotations that will be added to the driver pod. This should be a comma-separated list of label key-value
    pairs, where each annotation is in the format <code>key=value</code>.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.driver.pod.name</code></td>
  <td>(none)</td>
  <td>
    Name of the driver pod. If not set, the driver pod name is set to "spark.app.name" suffixed by the current timestamp
    to avoid name conflicts.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.submission.waitAppCompletion</code></td>
  <td><code>true</code></td>
  <td>
    In cluster mode, whether to wait for the application to finish before exiting the launcher process.  When changed to
    false, the launcher has a "fire-and-forget" behavior when launching the Spark job.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.resourceStagingServer.port</code></td>
  <td><code>10000</code></td>
  <td>
    Port for the resource staging server to listen on when it is deployed.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.resourceStagingServer.uri</code></td>
  <td>(none)</td>
  <td>
    URI of the resource staging server that Spark should use to distribute the application's local dependencies. Note
    that by default, this URI must be reachable by both the submitting machine and the pods running in the cluster. If
    one URI is not simultaneously reachable both by the submitter and the driver/executor pods, configure the pods to
    access the staging server at a different URI by setting
    <code>spark.kubernetes.resourceStagingServer.internal.uri</code> as discussed below.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.resourceStagingServer.internal.uri</code></td>
  <td>Value of <code>spark.kubernetes.resourceStagingServer.uri</code></td>
  <td>
    URI of the resource staging server to communicate with when init-containers bootstrap the driver and executor pods
    with submitted local dependencies. Note that this URI must by the pods running in the cluster. This is useful to
    set if the resource staging server has a separate "internal" URI that must be accessed by components running in the
    cluster.
  </td>
</tr>
<tr>
  <td><code>spark.ssl.kubernetes.resourceStagingServer.internal.trustStore</code></td>
  <td>Value of <code>spark.ssl.kubernetes.resourceStagingServer.trustStore</code></td>
  <td>
    Location of the trustStore file to use when communicating with the resource staging server over TLS, as
    init-containers bootstrap the driver and executor pods with submitted local dependencies. This can be a URI with a
    scheme of <code>local://</code>, which denotes that the file is pre-mounted on the pod's disk. A uri without a
    scheme or a scheme of <code>file://</code> will result in this file being mounted from the submitting machine's
    disk as a secret into the init-containers.
  </td>
</tr>
<tr>
  <td><code>spark.ssl.kubernetes.resourceStagingServer.internal.trustStorePassword</code></td>
  <td>Value of <code><code>spark.ssl.kubernetes.resourceStagingServer.trustStorePassword</code></td>
  <td>
    Password of the trustStore file that is used when communicating with the resource staging server over TLS, as
    init-containers bootstrap the driver and executor pods with submitted local dependencies.
  </td>
</tr>
<tr>
  <td><code>spark.ssl.kubernetes.resourceStagingServer.internal.trustStoreType</code></td>
  <td>Value of <code><code>spark.ssl.kubernetes.resourceStagingServer.trustStoreType</code></td>
  <td>
    Type of the trustStore file that is used when communicating with the resource staging server over TLS, when
    init-containers bootstrap the driver and executor pods with submitted local dependencies.
  </td>
</tr>
<tr>
  <td><code>spark.ssl.kubernetes.resourceStagingServer.internal.clientCertPem</code></td>
  <td>Value of <code>spark.ssl.kubernetes.resourceStagingServer.clientCertPem</code></td>
  <td>
    Location of the certificate file to use when communicating with the resource staging server over TLS, as
    init-containers bootstrap the driver and executor pods with submitted local dependencies. This can be a URI with a
    scheme of <code>local://</code>, which denotes that the file is pre-mounted on the pod's disk. A uri without a
    scheme or a scheme of <code>file://</code> will result in this file being mounted from the submitting machine's
    disk as a secret into the init-containers.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.mountdependencies.jarsDownloadDir</code></td>
  <td><code>/var/spark-data/spark-jars</code></td>
  <td>
    Location to download jars to in the driver and executors. This will be mounted as an empty directory volume
    into the driver and executor containers.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.mountdependencies.filesDownloadDir</code></td>
  <td><code>/var/spark-data/spark-files</code></td>
  <td>
    Location to download files to in the driver and executors. This will be mounted as an empty directory volume
    into the driver and executor containers.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.report.interval</code></td>
  <td><code>1s</code></td>
  <td>
    Interval between reports of the current Spark job status in cluster mode.
  </td>
</tr>
</table>


## Current Limitations

Running Spark on Kubernetes is currently an experimental feature. Some restrictions on the current implementation that
should be lifted in the future include:
* Applications can only run in cluster mode.
* Only Scala and Java applications can be run.
