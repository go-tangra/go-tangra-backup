#!/bin/sh
set -e

systemctl daemon-reload

echo ""
echo "tangra-backup installed successfully!"
echo ""
echo "Next steps:"
echo "  1. Edit /etc/tangra-backup/server.yaml"
echo "  2. Place mTLS certs in /etc/tangra-backup/certs/"
echo "  3. systemctl enable --now tangra-backup"
echo ""
