#!/usr/bin/env bash
# Resolve a Kafka cluster in Kubernetes into the environment `livetest` wants.
#
# Prints `export` lines on stdout, so it is used as:
#
#     eval "$(.claude/skills/live-cluster/resolve-target.sh strimzi)"
#
# Nothing here is baked into the Rust tool on purpose: the cluster's shape lives
# in the skill, so pointing the same binary at a port-forward, a laptop broker
# or another Kubernetes cluster needs no rebuild.
set -euo pipefail

TARGET="${1:-strimzi}"
LISTENER="${2:-plain}"

fail() { echo "resolve-target: $*" >&2; exit 1; }

command -v kubectl >/dev/null 2>&1 || fail "kubectl is not on PATH"

case "$TARGET" in
  strimzi)
    NS=strimzi
    # Strimzi publishes the answer in the Kafka CR's status, which is more
    # reliable than guessing the service name: it already accounts for the
    # cluster name, the listener name and the port.
    KAFKA=$(kubectl -n "$NS" get kafka -o jsonpath='{.items[0].metadata.name}' 2>/dev/null) \
      || fail "no Kafka resource in namespace $NS"
    [ -n "$KAFKA" ] || fail "no Kafka resource in namespace $NS"

    BOOTSTRAP=$(kubectl -n "$NS" get kafka "$KAFKA" \
      -o jsonpath="{.status.listeners[?(@.name=='$LISTENER')].bootstrapServers}")
    [ -n "$BOOTSTRAP" ] || fail "listener '$LISTENER' has no bootstrapServers on $KAFKA"

    # `.svc` is not a resolvable suffix from every pod; `.svc.cluster.local` is.
    BOOTSTRAP=${BOOTSTRAP/.svc:/.svc.cluster.local:}

    echo "export KAAS_TEST_BOOTSTRAP='$BOOTSTRAP'"
    echo "export KAAS_TEST_LABEL='strimzi'"

    if [ "$LISTENER" = "tls" ]; then
      # The cluster CA, which the broker certificates chain to. Written to a
      # file rather than inlined so the PEM's newlines survive.
      CA_FILE="${TMPDIR:-/tmp}/kaas-live-strimzi-ca.pem"
      kubectl -n "$NS" get secret "${KAFKA}-cluster-ca-cert" \
        -o jsonpath='{.data.ca\.crt}' | base64 -d > "$CA_FILE"
      echo "export KAAS_TEST_CA_FILE='$CA_FILE'"
      # Broker certificates carry the service name, and we connect by it, so no
      # override is needed — stated explicitly so a future change is deliberate.
      echo "unset KAAS_TEST_TLS_SERVER_NAME"
    else
      echo "unset KAAS_TEST_CA_FILE"
      echo "unset KAAS_TEST_TLS_SERVER_NAME"
    fi
    ;;

  kaas)
    NS=kaas
    SVC=kaas
    case "$LISTENER" in
      plain)  PORT_NAME=kafka-plain ;;
      authed) PORT_NAME=kafka-authed ;;
      tls)    PORT_NAME=kafka-tls ;;
      *)      fail "unknown kaas listener '$LISTENER' (plain|authed|tls)" ;;
    esac
    PORT=$(kubectl -n "$NS" get svc "$SVC" \
      -o jsonpath="{.spec.ports[?(@.name=='$PORT_NAME')].port}")
    [ -n "$PORT" ] || fail "service $SVC has no port named $PORT_NAME"

    echo "export KAAS_TEST_BOOTSTRAP='${SVC}.${NS}.svc.cluster.local:${PORT}'"
    echo "export KAAS_TEST_LABEL='kaas'"
    echo "unset KAAS_TEST_CA_FILE"
    echo "unset KAAS_TEST_TLS_SERVER_NAME"
    ;;

  *)
    fail "unknown target '$TARGET' (strimzi|kaas)"
    ;;
esac

# Credentials are never resolved automatically. A run that needs SASL sets
# these itself, so an unauthenticated run cannot silently pick up somebody's
# credentials from a secret it happened to be able to read.
echo "unset KAAS_TEST_SASL_MECHANISM KAAS_TEST_SASL_USERNAME KAAS_TEST_SASL_PASSWORD"
