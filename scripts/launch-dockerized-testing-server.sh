#!/bin/bash

script_path=$(realpath $0)
script_dir=$(dirname $script_path)

cd $script_dir/..

docker run \
  --name mrtparse-serve-test \
  --env-file ./.env \
  --add-host "host.docker.internal:host-gateway" \
  -v go-dev-vol-mrtparse-serve:/go \
  -v ./:/root/projects/mrtparse-stream \
  -v ./.env:/root/projects/mrtparse-stream/.env \
  -w /root/projects/mrtparse-stream \
  -p 8190:8190 \
  --rm \
  -it \
  docker.io/library/golang:latest \
    go run ./cmd/serve \
      --listen-address=":8190" \
      --jwt-auth-secret-from-env="JWT_SECRET" \
      --authentication="jwt"
