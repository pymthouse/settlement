#!/bin/sh
set -eu

# Railway volume mounts are root-owned; redpanda (uid 101) cannot create files
# under the mount without fixing ownership first. Drop to the redpanda user
# before starting the broker so the long-running process never runs as root.
REDPANDA_UID=101
REDPANDA_GID=101

if [ "$(id -u)" = "0" ]; then
	if [ -d /var/lib/redpanda/data ]; then
		chown -R "${REDPANDA_UID}:${REDPANDA_GID}" /var/lib/redpanda/data
	fi
	exec setpriv --reuid="${REDPANDA_UID}" --regid="${REDPANDA_GID}" --clear-groups -- "$@"
fi

exec "$@"
