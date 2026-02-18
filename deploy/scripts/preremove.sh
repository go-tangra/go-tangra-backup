#!/bin/sh
set -e

if systemctl is-active --quiet tangra-backup 2>/dev/null; then
    systemctl stop tangra-backup
fi

if systemctl is-enabled --quiet tangra-backup 2>/dev/null; then
    systemctl disable tangra-backup
fi
