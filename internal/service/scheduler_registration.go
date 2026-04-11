package service

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"path/filepath"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	commonV1 "github.com/go-tangra/go-tangra-common/gen/go/common/service/v1"
)

// RegisterTasksWithScheduler registers this module's task types with the scheduler.
// Runs in a background goroutine with retries since the scheduler may start after this module.
func RegisterTasksWithScheduler(logger log.Logger) {
	l := log.NewHelper(log.With(logger, "module", "scheduler-registration/backup-service"))

	schedulerEndpoint := os.Getenv("SCHEDULER_GRPC_ENDPOINT")
	if schedulerEndpoint == "" {
		l.Info("SCHEDULER_GRPC_ENDPOINT not set, skipping task type registration")
		return
	}

	go func() {
		// Wait for scheduler to be ready
		time.Sleep(10 * time.Second)

		for attempt := 0; attempt < 30; attempt++ {
			if err := doRegister(schedulerEndpoint, l); err != nil {
				l.Warnf("Task type registration attempt %d failed: %v", attempt+1, err)
				time.Sleep(10 * time.Second)
				continue
			}
			return
		}
		l.Error("Failed to register task types with scheduler after 30 attempts")
	}()
}

func doRegister(endpoint string, l *log.Helper) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Try mTLS first, fall back to insecure
	transportCreds := loadSchedulerTLS(l)
	conn, err := grpc.NewClient(endpoint, transportCreds)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := commonV1.NewTaskTypeRegistrationServiceClient(conn)

	resp, err := client.RegisterTaskTypes(ctx, &commonV1.RegisterTaskTypesRequest{
		ModuleId: "backup",
		TaskTypes: []*commonV1.TaskTypeDescriptor{
			{
				TaskType:        "backup:cleanup-old",
				DisplayName:     "Cleanup Old Backups",
				Description:     "Delete backups older than the configured retention period",
				PayloadSchema:   `{"type":"object","properties":{"maxAgeDays":{"type":"integer","default":30},"moduleId":{"type":"string"},"dryRun":{"type":"boolean","default":false}}}`,
				DefaultCron:     "0 3 * * *",
				DefaultMaxRetry: 2,
			},
			{
				TaskType:        "backup:validate-all",
				DisplayName:     "Validate All Backups",
				Description:     "Check all stored backups for consistency and report status",
				DefaultCron:     "0 4 * * 0",
				DefaultMaxRetry: 1,
			},
			{
				TaskType:        "backup:full-platform",
				DisplayName:     "Full Platform Backup",
				Description:     "Create a full backup of all platform modules (all services with BackupService)",
				PayloadSchema:   `{"type":"object","properties":{"modules":{"type":"array","items":{"type":"string"},"description":"List of module_id:grpc_endpoint pairs. Empty = all defaults."},"password":{"type":"string","description":"Optional encryption password"}}}`,
				DefaultCron:     "0 2 * * *",
				DefaultMaxRetry: 1,
			},
		},
	})
	if err != nil {
		return err
	}

	l.Infof("Registered %d task types with scheduler: %s", resp.GetRegisteredCount(), resp.GetMessage())
	return nil
}

// loadSchedulerTLS attempts to load mTLS credentials for connecting to the scheduler.
// Falls back to insecure if certs are not available.
func loadSchedulerTLS(l *log.Helper) grpc.DialOption {
	certsDir := os.Getenv("CERTS_DIR")
	if certsDir == "" {
		certsDir = "/app/certs"
	}

	caCertPath := filepath.Join(certsDir, "ca", "ca.crt")
	caCert, err := os.ReadFile(caCertPath)
	if err != nil {
		l.Info("No CA cert found, using insecure credentials for scheduler")
		return grpc.WithTransportCredentials(insecure.NewCredentials())
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		l.Warn("Failed to parse CA cert, using insecure credentials for scheduler")
		return grpc.WithTransportCredentials(insecure.NewCredentials())
	}

	// Look for a client cert (backup/backup.crt)
	clientCertPath := filepath.Join(certsDir, "backup", "backup.crt")
	clientKeyPath := filepath.Join(certsDir, "backup", "backup.key")

	clientCert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
	if err != nil {
		l.Info("No client cert found, using insecure credentials for scheduler")
		return grpc.WithTransportCredentials(insecure.NewCredentials())
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      caCertPool,
		ServerName:   "scheduler-service",
		MinVersion:   tls.VersionTLS12,
	}

	l.Info("Using mTLS credentials for scheduler connection")
	return grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
}
