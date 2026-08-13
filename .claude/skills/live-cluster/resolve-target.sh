#!/usr/bin/env bash
# Resolve a Kafka cluster in Kubernetes into the environment `livetest` wants.
#
# Prints `export` lines on stdout, so it is used as:
#
#     eval "$(.claude/skills/live-cluster/resolve-target.sh strimzi)"
#     eval "$(.claude/skills/live-cluster/resolve-target.sh strimzi mtls bob-mtls)"
#
# Nothing here is baked into the Rust tool on purpose: the cluster's shape lives
# in the skill, so pointing the same binary at a port-forward, a laptop broker
# or another Kubernetes cluster needs no rebuild.
set -euo pipefail

TARGET="${1:-strimzi}"
LISTENER="${2:-plain}"
# A KafkaUser whose client certificate to present, for a mutual-TLS listener.
#
# Deliberately a third argument rather than something inferred from the
# listener: this script resolves *endpoints*, and a private key is not an
# endpoint. Picking one up automatically would mean a run silently
# authenticating as whatever identity the pod happened to be able to read,
# which is the one thing an endpoint resolver must not do. Naming the user is
# the opt-in.
CLIENT_USER="${3:-}"

fail() { echo "resolve-target: $*" >&2; exit 1; }

command -v kubectl >/dev/null 2>&1 || fail "kubectl is not on PATH"

# Checked before a single `export` line is printed: this script's output is
# `eval`-ed, so failing halfway through leaves a shell holding an endpoint and
# no credential, which then fails again further away from the cause.
if [ -n "$CLIENT_USER" ]; then
  [ "$TARGET" = "strimzi" ] \
    || fail "a client certificate can only be resolved from a Strimzi KafkaUser"
  kubectl -n strimzi get kafkauser "$CLIENT_USER" >/dev/null 2>&1 \
    || fail "no KafkaUser '$CLIENT_USER' in namespace strimzi"
  USER_AUTH=$(kubectl -n strimzi get kafkauser "$CLIENT_USER" \
    -o jsonpath='{.spec.authentication.type}')
  [ "$USER_AUTH" = "tls" ] \
    || fail "KafkaUser '$CLIENT_USER' authenticates with '${USER_AUTH:-nothing}', not a certificate"
fi

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

    # Whether *this* listener is encrypted, from the CR rather than from its
    # name. `tls` is not the only encrypted listener any more — the `internal`
    # OAUTHBEARER one is SASL_SSL — and keying the CA off the name silently
    # produced an unencrypted-looking run that then failed in the handshake.
    LISTENER_TLS=$(kubectl -n "$NS" get kafka "$KAFKA" \
      -o jsonpath="{.spec.kafka.listeners[?(@.name=='$LISTENER')].tls}")

    if [ "$LISTENER_TLS" = "true" ]; then
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

    # Say so when the listener authenticates, because the next thing to happen
    # otherwise is a handshake failure that looks like a broken listener.
    LISTENER_AUTH=$(kubectl -n "$NS" get kafka "$KAFKA" \
      -o jsonpath="{.spec.kafka.listeners[?(@.name=='$LISTENER')].authentication.type}")
    case "$LISTENER_AUTH" in
      "") ;;
      tls)
        # Mutual TLS is not SASL, and saying "set KAAS_TEST_SASL_MECHANISM"
        # here would send someone to configure the wrong half entirely.
        [ -n "$CLIENT_USER" ] || echo "resolve-target: listener '$LISTENER'" \
          "authenticates clients by certificate; pass a KafkaUser name as the" \
          "third argument to present one" >&2
        ;;
      *)
        echo "resolve-target: listener '$LISTENER' authenticates ($LISTENER_AUTH);" \
             "set KAAS_TEST_SASL_MECHANISM and its credentials yourself" >&2
        ;;
    esac
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
# credentials from a secret it happened to be able to read. The client
# certificate is in the same list for the same reason, and is re-exported below
# only when a KafkaUser was named on the command line.
echo "unset KAAS_TEST_SASL_MECHANISM KAAS_TEST_SASL_USERNAME KAAS_TEST_SASL_PASSWORD"
echo "unset KAAS_TEST_OAUTH_TOKEN KAAS_TEST_OAUTH_TOKEN_ENDPOINT KAAS_TEST_OAUTH_CLIENT_ID"
echo "unset KAAS_TEST_OAUTH_CLIENT_SECRET KAAS_TEST_OAUTH_SCOPE KAAS_TEST_OAUTH_AUDIENCE"
echo "unset KAAS_TEST_CLIENT_CERT_PEM KAAS_TEST_CLIENT_CERT_FILE"
echo "unset KAAS_TEST_CLIENT_KEY_PEM KAAS_TEST_CLIENT_KEY_FILE"

# The one credential this script will resolve, and only when asked for by name.
if [ -n "$CLIENT_USER" ]; then
  # The User Operator writes the certificate into a Secret named after the
  # user. `user.p12` is in there too and is not needed: rustls-pemfile reads
  # the PEMs directly, so there is no keytool step.
  CERT_FILE="${TMPDIR:-/tmp}/kaas-live-${CLIENT_USER}.crt"
  KEY_FILE="${TMPDIR:-/tmp}/kaas-live-${CLIENT_USER}.key"
  kubectl -n "$NS" get secret "$CLIENT_USER" -o jsonpath='{.data.user\.crt}' \
    | base64 -d > "$CERT_FILE"
  # The key is a credential: readable by its owner and nobody else, and created
  # that way rather than chmod-ed after the fact.
  (umask 077 && kubectl -n "$NS" get secret "$CLIENT_USER" -o jsonpath='{.data.user\.key}' \
    | base64 -d > "$KEY_FILE")
  [ -s "$CERT_FILE" ] || fail "Secret '$CLIENT_USER' has no user.crt"
  [ -s "$KEY_FILE" ] || fail "Secret '$CLIENT_USER' has no user.key"

  echo "export KAAS_TEST_CLIENT_CERT_FILE='$CERT_FILE'"
  echo "export KAAS_TEST_CLIENT_KEY_FILE='$KEY_FILE'"

  # The principal Kafka will authorize is a distinguished name, not a
  # username — `CN=bob-mtls`, which is what ACLs have to be written against.
  PRINCIPAL=$(kubectl -n "$NS" get kafkauser "$CLIENT_USER" -o jsonpath='{.status.username}')
  echo "resolve-target: presenting '$CLIENT_USER'; the broker will authorize it as" \
       "User:${PRINCIPAL:-<not reported yet>}" >&2
fi
