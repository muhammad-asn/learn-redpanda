#!/bin/bash
helm repo add redpanda https://charts.redpanda.com
helm repo add jetstack https://charts.jetstack.io
helm repo update
helm install cert-manager jetstack/cert-manager \
--set crds.enabled=true --namespace cert-manager \
--create-namespace
helm repo add redpanda https://charts.redpanda.com/
helm repo update
helm upgrade --install redpanda redpanda/redpanda \
  --version 5.10.2 \
  --namespace redpanda \
  --create-namespace \
  -f values.yaml
