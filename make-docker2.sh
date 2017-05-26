#!/bin/sh


version=2
path="/home/moonfang/opensource/spark-on-k8s/spark/resource-managers/kubernetes/docker-minimal-bundle/src/main/docker"
cd dist
#docker build -t docker.oa.com:8080/moonfang/spark-driver:latest -f ./resource-managers/kubernetes/docker-minimal-bundle/src/main/docker/driver/Dockerfile .
rm -rf moon-docker
cp -r $path moon-docker

docker build -t docker.oa.com:8080/moonfang/spark-init:v2 -f moon-docker/init-container/Dockerfile .
docker push docker.oa.com:8080/moonfang/spark-init:v2


#docker build -t docker.oa.com:8080/moonfang/resource-staging-server:v2 -f moon-docker/resource-staging-server/Dockerfile .
#docker push docker.oa.com:8080/moonfang/resource-staging-server:v2
#
#docker build -t docker.oa.com:8080/moonfang/shuffle-service:v2 -f moon-docker/shuffle-service/Dockerfile .
#docker push docker.oa.com:8080/moonfang/shuffle-service:v2
