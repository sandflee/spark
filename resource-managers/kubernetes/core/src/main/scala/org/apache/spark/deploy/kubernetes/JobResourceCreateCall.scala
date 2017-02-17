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
package org.apache.spark.deploy.kubernetes

import org.apache.spark.deploy.kubernetes.SparkJobResource.{SparkJobState, WatchObject}

import scala.concurrent.Future

/**
 * Isolated this since the method is called on the client machine
 */
trait JobResourceCreateCall {
  def createJobObject(name: String, keyValuePairs: Map[String, Any]): Unit
}

/**
 * RUD and W - Read, Update, Delete & Watch
 */
trait JobResourceRUDCalls {
  def deleteJobObject(name: String): Unit

  def getJobObject(name: String): SparkJobState

  def updateJobObject(name: String, value: String, fieldPath: String): Unit

  def watchJobObject(): Future[WatchObject]
}
