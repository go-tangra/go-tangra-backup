package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/tx7do/kratos-bootstrap/bootstrap"

	commonV1 "github.com/go-tangra/go-tangra-common/gen/go/common/service/v1"
	backupV1 "github.com/go-tangra/go-tangra-backup/gen/go/backup/service/v1"
)

// TaskExecutor implements the TaskExecutorService gRPC interface.
// The scheduler calls ExecuteTask when a backup-related task fires.
type TaskExecutor struct {
	commonV1.UnimplementedTaskExecutorServiceServer

	log           *log.Helper
	orchestrator  *OrchestratorService
	backupStorage *BackupStorage
}

func NewTaskExecutor(
	ctx *bootstrap.Context,
	orchestrator *OrchestratorService,
	storage *BackupStorage,
) *TaskExecutor {
	return &TaskExecutor{
		log:           ctx.NewLoggerHelper("task-executor/backup-service"),
		orchestrator:  orchestrator,
		backupStorage: storage,
	}
}

func (e *TaskExecutor) ExecuteTask(
	ctx context.Context,
	req *commonV1.ExecuteTaskRequest,
) (*commonV1.ExecuteTaskResponse, error) {
	e.log.Infof("Executing task %s (execution=%s, attempt=%d/%d, tenant=%d)",
		req.GetTaskType(), req.GetExecutionId(), req.GetAttempt(), req.GetMaxAttempts(), req.GetTenantId())

	switch req.GetTaskType() {
	case "backup:full-platform":
		return e.handleFullPlatformBackup(ctx, req)
	case "backup:cleanup-old":
		return e.handleCleanupOld(ctx, req)
	case "backup:validate-all":
		return e.handleValidateAll(ctx, req)
	default:
		return &commonV1.ExecuteTaskResponse{
			Success:          false,
			PermanentFailure: true,
			Message:          fmt.Sprintf("unknown task type: %s", req.GetTaskType()),
		}, nil
	}
}

// FullBackupConfig is the payload for backup:full-platform tasks.
type FullBackupConfig struct {
	// Modules to back up. Each entry: "module_id:grpc_endpoint" (e.g., "ipam:ipam-service:9400").
	// If empty, backs up all modules from the default list.
	Modules  []string `json:"modules,omitempty"`
	Password string   `json:"password,omitempty"`
}

// defaultModuleTargets returns the default list of modules to back up.
func defaultModuleTargets() []*backupV1.ModuleTarget {
	defaults := []struct {
		id       string
		endpoint string
	}{
		{"ipam", "ipam-service:9400"},
		{"warden", "warden-service:9300"},
		{"deployer", "deployer-service:9200"},
		{"paperless", "paperless-service:9500"},
		{"sharing", "sharing-service:9600"},
		{"executor", "executor-service:9800"},
		{"asset", "asset-service:9900"},
		{"hr", "hr-service:10200"},
		{"notification", "notification-service:10300"},
		{"signing", "signing-service:10400"},
		{"scheduler", "scheduler-svc:10500"},
	}
	targets := make([]*backupV1.ModuleTarget, 0, len(defaults))
	for _, d := range defaults {
		targets = append(targets, &backupV1.ModuleTarget{
			ModuleId:     d.id,
			GrpcEndpoint: d.endpoint,
		})
	}
	return targets
}

func (e *TaskExecutor) handleFullPlatformBackup(
	ctx context.Context,
	req *commonV1.ExecuteTaskRequest,
) (*commonV1.ExecuteTaskResponse, error) {
	cfg := FullBackupConfig{}
	if len(req.GetPayload()) > 0 {
		if err := json.Unmarshal(req.GetPayload(), &cfg); err != nil {
			return &commonV1.ExecuteTaskResponse{
				Success:          false,
				PermanentFailure: true,
				Message:          fmt.Sprintf("invalid payload: %v", err),
			}, nil
		}
	}

	// Build module targets from config or defaults
	var targets []*backupV1.ModuleTarget
	if len(cfg.Modules) > 0 {
		for _, m := range cfg.Modules {
			parts := strings.SplitN(m, ":", 2)
			if len(parts) != 2 {
				return &commonV1.ExecuteTaskResponse{
					Success:          false,
					PermanentFailure: true,
					Message:          fmt.Sprintf("invalid module format %q, expected 'module_id:grpc_endpoint'", m),
				}, nil
			}
			targets = append(targets, &backupV1.ModuleTarget{
				ModuleId:     parts[0],
				GrpcEndpoint: parts[1],
			})
		}
	} else {
		targets = defaultModuleTargets()
	}

	e.log.Infof("Starting full platform backup for %d modules", len(targets))

	resp, err := e.orchestrator.CreateFullBackup(ctx, &backupV1.CreateFullBackupRequest{
		Targets:  targets,
		Password: cfg.Password,
	})
	if err != nil {
		return &commonV1.ExecuteTaskResponse{
			Success: false,
			Message: fmt.Sprintf("full backup failed: %v", err),
		}, nil
	}

	moduleCount := 0
	if resp.GetBackup() != nil {
		moduleCount = len(resp.GetBackup().GetModuleBackups())
	}

	return &commonV1.ExecuteTaskResponse{
		Success: true,
		Message: fmt.Sprintf("Full platform backup completed: %d modules backed up, ID=%s",
			moduleCount, resp.GetBackup().GetId()),
	}, nil
}

// CleanupConfig is the payload for backup:cleanup-old tasks.
type CleanupConfig struct {
	MaxAgeDays int    `json:"maxAgeDays"`
	ModuleID   string `json:"moduleId,omitempty"` // empty = all modules
	DryRun     bool   `json:"dryRun"`
}

func (e *TaskExecutor) handleCleanupOld(
	ctx context.Context,
	req *commonV1.ExecuteTaskRequest,
) (*commonV1.ExecuteTaskResponse, error) {
	cfg := CleanupConfig{MaxAgeDays: 30}
	if len(req.GetPayload()) > 0 {
		if err := json.Unmarshal(req.GetPayload(), &cfg); err != nil {
			return &commonV1.ExecuteTaskResponse{
				Success:          false,
				PermanentFailure: true,
				Message:          fmt.Sprintf("invalid payload: %v", err),
			}, nil
		}
	}

	if cfg.MaxAgeDays <= 0 {
		cfg.MaxAgeDays = 30
	}

	cutoff := time.Now().AddDate(0, 0, -cfg.MaxAgeDays)
	e.log.Infof("Cleaning up backups older than %d days (cutoff=%s, module=%s, dryRun=%v)",
		cfg.MaxAgeDays, cutoff.Format(time.RFC3339), cfg.ModuleID, cfg.DryRun)

	backups, err := e.backupStorage.ListModuleBackups(cfg.ModuleID, tenantPtr(req.GetTenantId()))
	if err != nil {
		return &commonV1.ExecuteTaskResponse{
			Success: false,
			Message: fmt.Sprintf("failed to list backups: %v", err),
		}, nil
	}

	deleted := 0
	for _, b := range backups {
		if b.GetCreatedAt() != nil && b.GetCreatedAt().AsTime().Before(cutoff) {
			if cfg.DryRun {
				e.log.Infof("[dry-run] Would delete backup %s (created %s)", b.GetId(), b.GetCreatedAt().AsTime())
				deleted++
				continue
			}
			if err := e.backupStorage.DeleteModuleBackup(b.GetId()); err != nil {
				e.log.Warnf("Failed to delete backup %s: %v", b.GetId(), err)
			} else {
				e.log.Infof("Deleted old backup %s (created %s)", b.GetId(), b.GetCreatedAt().AsTime())
				deleted++
			}
		}
	}

	msg := fmt.Sprintf("Cleaned up %d backups older than %d days", deleted, cfg.MaxAgeDays)
	if cfg.DryRun {
		msg = fmt.Sprintf("[DRY RUN] Would clean up %d backups older than %d days", deleted, cfg.MaxAgeDays)
	}

	return &commonV1.ExecuteTaskResponse{
		Success: true,
		Message: msg,
	}, nil
}

func (e *TaskExecutor) handleValidateAll(
	ctx context.Context,
	req *commonV1.ExecuteTaskRequest,
) (*commonV1.ExecuteTaskResponse, error) {
	backups, err := e.backupStorage.ListModuleBackups("", tenantPtr(req.GetTenantId()))
	if err != nil {
		return &commonV1.ExecuteTaskResponse{
			Success: false,
			Message: fmt.Sprintf("failed to list backups: %v", err),
		}, nil
	}

	valid, invalid := 0, 0
	for _, b := range backups {
		if b.GetStatus() == "completed" {
			valid++
		} else {
			invalid++
			e.log.Warnf("Backup %s has status %s", b.GetId(), b.GetStatus())
		}
	}

	return &commonV1.ExecuteTaskResponse{
		Success: true,
		Message: fmt.Sprintf("Validated %d backups: %d valid, %d invalid", valid+invalid, valid, invalid),
	}, nil
}

func tenantPtr(id uint32) *uint32 {
	if id == 0 {
		return nil
	}
	return &id
}
