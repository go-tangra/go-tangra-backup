package service

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/tx7do/kratos-bootstrap/bootstrap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	grpcMD "google.golang.org/grpc/metadata"

	backupV1 "github.com/go-tangra/go-tangra-backup/gen/go/backup/service/v1"
	"github.com/go-tangra/go-tangra-common/grpcx"
)

// ExportResult holds the result of a dynamic ExportBackup call.
type ExportResult struct {
	Data         []byte
	Module       string
	Version      string
	TenantID     uint32
	EntityCounts map[string]int64
}

// ModuleClient connects to any module's BackupService dynamically using raw
// gRPC invocation. It does not import any module-specific proto code.
type ModuleClient struct {
	log *log.Helper
}

// NewModuleClient creates a new dynamic module client.
func NewModuleClient(ctx *bootstrap.Context) *ModuleClient {
	return &ModuleClient{
		log: ctx.NewLoggerHelper("backup/module-client"),
	}
}

// ExportBackup calls the target module's BackupService.ExportBackup via dynamic gRPC invocation.
func (c *ModuleClient) ExportBackup(ctx context.Context, target *backupV1.ModuleTarget, tenantID *uint32) (*ExportResult, error) {
	conn, cleanup, err := c.dialModule(target.GrpcEndpoint)
	if err != nil {
		return nil, fmt.Errorf("dial %s at %s: %w", target.ModuleId, target.GrpcEndpoint, err)
	}
	defer cleanup()

	// Construct method path dynamically: /{moduleId}.service.v1.BackupService/ExportBackup
	method := fmt.Sprintf("/%s.service.v1.BackupService/ExportBackup", target.ModuleId)

	req := &backupV1.ModuleExportRequest{TenantId: tenantID}
	resp := &backupV1.ModuleExportResponse{}

	// Forward auth metadata with a per-call timeout
	outCtx := forwardMetadata(ctx)
	callCtx, cancel := context.WithTimeout(outCtx, 30*time.Second)
	defer cancel()

	c.log.Infof("Calling %s on %s", method, target.GrpcEndpoint)
	if err := conn.Invoke(callCtx, method, req, resp); err != nil {
		return nil, fmt.Errorf("invoke ExportBackup on %s: %w", target.ModuleId, err)
	}

	return &ExportResult{
		Data:         resp.Data,
		Module:       resp.Module,
		Version:      resp.Version,
		TenantID:     resp.TenantId,
		EntityCounts: resp.EntityCounts,
	}, nil
}

// ImportBackup calls the target module's BackupService.ImportBackup via dynamic gRPC invocation.
func (c *ModuleClient) ImportBackup(ctx context.Context, target *backupV1.ModuleTarget, data []byte, mode backupV1.RestoreMode) (*backupV1.ModuleImportResponse, error) {
	conn, cleanup, err := c.dialModule(target.GrpcEndpoint)
	if err != nil {
		return nil, fmt.Errorf("dial %s at %s: %w", target.ModuleId, target.GrpcEndpoint, err)
	}
	defer cleanup()

	method := fmt.Sprintf("/%s.service.v1.BackupService/ImportBackup", target.ModuleId)

	req := &backupV1.ModuleImportRequest{
		Data: data,
		Mode: mode,
	}
	resp := &backupV1.ModuleImportResponse{}

	outCtx := forwardMetadata(ctx)
	callCtx, cancel := context.WithTimeout(outCtx, 60*time.Second)
	defer cancel()

	c.log.Infof("Calling %s on %s", method, target.GrpcEndpoint)
	if err := conn.Invoke(callCtx, method, req, resp); err != nil {
		return nil, fmt.Errorf("invoke ImportBackup on %s: %w", target.ModuleId, err)
	}

	return resp, nil
}

// dialModule establishes a gRPC connection to a module endpoint.
func (c *ModuleClient) dialModule(endpoint string) (*grpc.ClientConn, func(), error) {
	c.log.Infof("dialModule: raw endpoint=%q", endpoint)

	// grpc.NewClient requires a URI scheme; passthrough lets the OS handle DNS
	if !strings.Contains(endpoint, "://") {
		endpoint = "passthrough:///" + endpoint
	}
	c.log.Infof("dialModule: resolved target=%q", endpoint)

	var dialOpt grpc.DialOption
	creds, err := loadClientTLSCredentials(c.log)
	if err != nil {
		c.log.Warnf("dialModule: TLS credentials failed, using insecure: %v", err)
		dialOpt = grpc.WithTransportCredentials(insecure.NewCredentials())
	} else {
		c.log.Infof("dialModule: using mTLS client credentials")
		dialOpt = grpc.WithTransportCredentials(creds)
	}

	connectParams := grpc.ConnectParams{
		Backoff: backoff.Config{
			BaseDelay:  500 * time.Millisecond,
			Multiplier: 1.5,
			Jitter:     0.2,
			MaxDelay:   5 * time.Second,
		},
		MinConnectTimeout: 5 * time.Second,
	}

	keepaliveParams := keepalive.ClientParameters{
		Time:                5 * time.Minute,
		Timeout:             20 * time.Second,
		PermitWithoutStream: false,
	}

	conn, err := grpc.NewClient(
		endpoint,
		dialOpt,
		grpc.WithConnectParams(connectParams),
		grpc.WithKeepaliveParams(keepaliveParams),
	)
	if err != nil {
		return nil, func() {}, fmt.Errorf("connect to %s: %w", endpoint, err)
	}

	cleanup := func() {
		if err := conn.Close(); err != nil {
			c.log.Warnf("Failed to close connection to %s: %v", endpoint, err)
		}
	}

	return conn, cleanup, nil
}

// forwardMetadata builds outgoing gRPC metadata by forwarding relevant headers
// from the incoming context so the target module sees the caller's auth context.
func forwardMetadata(ctx context.Context) context.Context {
	outMD := grpcMD.New(map[string]string{
		"x-md-global-tenant-id": fmt.Sprintf("%d", grpcx.GetTenantIDFromContext(ctx)),
	})

	if inMD, ok := grpcMD.FromIncomingContext(ctx); ok {
		for _, key := range []string{"x-md-global-user-id", "x-md-global-username", "x-md-global-roles"} {
			if vals := inMD.Get(key); len(vals) > 0 {
				outMD.Set(key, vals[0])
			}
		}
	}

	return grpcMD.NewOutgoingContext(ctx, outMD)
}

// loadClientTLSCredentials loads mTLS client credentials for calling modules.
func loadClientTLSCredentials(l *log.Helper) (credentials.TransportCredentials, error) {
	caCertPath := os.Getenv("BACKUP_CA_CERT_PATH")
	if caCertPath == "" {
		caCertPath = "/app/certs/ca/ca.crt"
	}
	clientCertPath := os.Getenv("BACKUP_CLIENT_CERT_PATH")
	if clientCertPath == "" {
		clientCertPath = "/app/certs/client/client.crt"
	}
	clientKeyPath := os.Getenv("BACKUP_CLIENT_KEY_PATH")
	if clientKeyPath == "" {
		clientKeyPath = "/app/certs/client/client.key"
	}

	caCert, err := os.ReadFile(caCertPath)
	if err != nil {
		return nil, fmt.Errorf("read CA cert from %s: %w", caCertPath, err)
	}
	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("parse CA certificate")
	}

	clientCert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
	if err != nil {
		return nil, fmt.Errorf("load client cert/key: %w", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      caCertPool,
		MinVersion:   tls.VersionTLS12,
	}

	return credentials.NewTLS(tlsConfig), nil
}
