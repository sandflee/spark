#!/bin/bash

kubectl create -f conf/kubernetes-resource-staging-server.yaml 
kubectl create -f conf/kubernetes-shuffle-service.yaml
