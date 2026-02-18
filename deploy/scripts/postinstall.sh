#!/bin/sh
set -e

systemctl daemon-reload

echo ""
echo "tangra-backup installed successfully!"
echo ""
echo "Next steps:"
echo "  1. Edit /etc/tangra-backup/server.yaml"
echo "  2. Set environment variables in /etc/tangra-backup/env:"
echo "     ADMIN_GRPC_ENDPOINT=admin-service:7787"
echo "     MODULE_REGISTRATION_SECRET=your-secret"
echo "     GRPC_ADVERTISE_ADDR=this-host:10100"
echo "  3. Place mTLS certs in /etc/tangra-backup/certs/"
echo "  4. systemctl enable --now tangra-backup"
echo ""
