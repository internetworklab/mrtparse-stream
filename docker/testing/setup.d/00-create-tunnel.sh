#!/bin/bash

# Run this only at first time when the cloudflared tunnel object hasn't been created yet.
# It generates cfd_tunnel.json as the response content,
# cfd_credentials.json as credential file for your cloudflared client to authenticate itself to cloudflare endpoint,
# then it prints tunnel UUID to stdout.

set -e

script_path=$(realpath "$0")
script_dir=$(dirname "$script_path")
cd "${script_dir}/.."


source .env

if [ -z "${CF_ACCOUNT_ID}" ]; then
    echo "No CF_ACCOUNT_ID provided"
    exit 1
fi

if [ -z "${CF_API_TOKEN}" ]; then
    echo "No CF_API_TOKEN provided"
    exit 1
fi

if [ -z "${TUNNEL_NAME}" ]; then
    echo "No TUNNEL_NAME provided"
    exit 1
fi

# see https://developers.cloudflare.com/api/resources/zero_trust/subresources/tunnels/

curl -o ./cfd_tunnel.json "https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/cfd_tunnel" \
  --request POST \
  --header "Authorization: Bearer ${CF_API_TOKEN}" \
  --json "{
    \"name\": \"${TUNNEL_NAME}\",
    \"config_src\": \"local\"
  }"

jq '.result.credentials_file' cfd_tunnel.json > ./cfd_credentials.json
jq --raw-output '.result.id' cfd_tunnel.json
