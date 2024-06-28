#!/bin/sh

if [ -z "${!LOCALSTACK_RESOLVE_IP_ADDRESS}" ]; then
    echo "Resolving *.localhost.localstack.cloud to ${LOCALSTACK_RESOLVE_IP_ADDRESS}"
    echo "${LOCALSTACK_RESOLVE_IP_ADDRESS} snowflake.localhost.localstack.cloud" >> /etc/hosts
    echo "${LOCALSTACK_RESOLVE_IP_ADDRESS} localhost.localstack.cloud" >> /etc/hosts
else
    echo "Skipping adding domain resolver to /etc/hosts"
fi
