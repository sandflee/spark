#!/bin/sh


cd dist
#docker build -t docker.oa.com:8080/moonfang/spark-driver:latest -f ./resource-managers/kubernetes/docker-minimal-bundle/src/main/docker/driver/Dockerfile .
docker build -t docker.oa.com:8080/moonfang/spark-driver:v2 -f ./dockerfiles/driver/Dockerfile .
docker push docker.oa.com:8080/moonfang/spark-driver:v2

docker build -t docker.oa.com:8080/moonfang/spark-executor:v2 -f ./dockerfiles/executor/Dockerfile .
docker push docker.oa.com:8080/moonfang/spark-executor:v2
