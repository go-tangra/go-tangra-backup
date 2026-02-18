package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/uuid"
	"github.com/tx7do/kratos-bootstrap/bootstrap"
	"google.golang.org/protobuf/types/known/timestamppb"

	backupV1 "github.com/go-tangra/go-tangra-backup/gen/go/backup/service/v1"
)

// OrchestratorService implements the BackupOrchestratorService gRPC interface.
type OrchestratorService struct {
	backupV1.UnimplementedBackupOrchestratorServiceServer

	log          *log.Helper
	moduleClient *ModuleClient
	storage      *BackupStorage
}

// NewOrchestratorService creates a new orchestrator service.
func NewOrchestratorService(
	ctx *bootstrap.Context,
	moduleClient *ModuleClient,
	storage *BackupStorage,
) *OrchestratorService {
	return &OrchestratorService{
		log:          ctx.NewLoggerHelper("backup/orchestrator"),
		moduleClient: moduleClient,
		storage:      storage,
	}
}

// --- Single Module Operations ---

func (s *OrchestratorService) CreateModuleBackup(ctx context.Context, req *backupV1.CreateModuleBackupRequest) (*backupV1.CreateModuleBackupResponse, error) {
	if req.Target == nil {
		return nil, fmt.Errorf("target is required")
	}

	username := getUsernameFromContext(ctx)
	now := time.Now()

	s.log.Infof("Creating backup for module %s at %s", req.Target.ModuleId, req.Target.GrpcEndpoint)

	result, err := s.moduleClient.ExportBackup(ctx, req.Target, req.TenantId, req.IncludeSecrets)
	if err != nil {
		// Save a failed backup record
		backupID := uuid.New().String()
		info := &backupV1.BackupInfo{
			Id:          backupID,
			ModuleId:    req.Target.ModuleId,
			Description: req.Description,
			TenantId:    tenantIDValue(req.TenantId),
			FullBackup:  req.TenantId != nil && *req.TenantId == 0,
			Status:      "failed",
			CreatedAt:   timestamppb.New(now),
			CreatedBy:   username,
			Warnings:    []string{err.Error()},
		}
		return &backupV1.CreateModuleBackupResponse{Backup: info}, nil
	}

	backupID := uuid.New().String()
	info := &backupV1.BackupInfo{
		Id:           backupID,
		ModuleId:     req.Target.ModuleId,
		Description:  req.Description,
		TenantId:     result.TenantID,
		FullBackup:   req.TenantId != nil && *req.TenantId == 0,
		Status:       "completed",
		SizeBytes:    int64(len(result.Data)),
		EntityCounts: result.EntityCounts,
		CreatedAt:    timestamppb.New(now),
		CreatedBy:    username,
		Version:      result.Version,
	}

	if err := s.storage.SaveModuleBackup(info, result.Data, req.Password); err != nil {
		return nil, fmt.Errorf("save backup: %w", err)
	}

	s.log.Infof("Module backup completed: id=%s module=%s size=%d", backupID, req.Target.ModuleId, len(result.Data))
	return &backupV1.CreateModuleBackupResponse{Backup: info}, nil
}

func (s *OrchestratorService) RestoreModuleBackup(ctx context.Context, req *backupV1.RestoreModuleBackupRequest) (*backupV1.RestoreModuleBackupResponse, error) {
	if req.Target == nil {
		return nil, fmt.Errorf("target is required")
	}

	s.log.Infof("Restoring backup %s to module %s at %s", req.BackupId, req.Target.ModuleId, req.Target.GrpcEndpoint)

	data, err := s.storage.LoadModuleBackupData(req.BackupId, req.Password)
	if err != nil {
		return nil, fmt.Errorf("load backup data: %w", err)
	}

	resp, err := s.moduleClient.ImportBackup(ctx, req.Target, data, req.Mode)
	if err != nil {
		return nil, fmt.Errorf("import backup to %s: %w", req.Target.ModuleId, err)
	}

	results := make([]*backupV1.EntityImportResult, len(resp.Results))
	for i, r := range resp.Results {
		results[i] = &backupV1.EntityImportResult{
			EntityType: r.EntityType,
			Total:      r.Total,
			Created:    r.Created,
			Updated:    r.Updated,
			Skipped:    r.Skipped,
			Failed:     r.Failed,
		}
	}

	s.log.Infof("Module restore completed: backup=%s module=%s", req.BackupId, req.Target.ModuleId)
	return &backupV1.RestoreModuleBackupResponse{
		Success:  resp.Success,
		Results:  results,
		Warnings: resp.Warnings,
	}, nil
}

func (s *OrchestratorService) ListBackups(ctx context.Context, req *backupV1.ListBackupsRequest) (*backupV1.ListBackupsResponse, error) {
	backups, err := s.storage.ListModuleBackups(req.ModuleId, req.TenantId)
	if err != nil {
		return nil, fmt.Errorf("list backups: %w", err)
	}

	// Pagination
	total := int32(len(backups))
	page, pageSize := normalizePagination(req.Page, req.PageSize)
	start := (page - 1) * pageSize
	if start >= total {
		return &backupV1.ListBackupsResponse{Total: total}, nil
	}
	end := start + pageSize
	if end > total {
		end = total
	}

	return &backupV1.ListBackupsResponse{
		Backups: backups[start:end],
		Total:   total,
	}, nil
}

func (s *OrchestratorService) GetBackup(ctx context.Context, req *backupV1.GetBackupRequest) (*backupV1.GetBackupResponse, error) {
	info, err := s.storage.GetModuleBackup(req.Id)
	if err != nil {
		return nil, fmt.Errorf("get backup: %w", err)
	}
	return &backupV1.GetBackupResponse{Backup: info}, nil
}

func (s *OrchestratorService) DeleteBackup(ctx context.Context, req *backupV1.DeleteBackupRequest) (*backupV1.DeleteBackupResponse, error) {
	if err := s.storage.DeleteModuleBackup(req.Id); err != nil {
		return nil, fmt.Errorf("delete backup: %w", err)
	}
	s.log.Infof("Deleted module backup: %s", req.Id)
	return &backupV1.DeleteBackupResponse{Success: true}, nil
}

func (s *OrchestratorService) DownloadBackup(ctx context.Context, req *backupV1.DownloadBackupRequest) (*backupV1.DownloadBackupResponse, error) {
	info, err := s.storage.GetModuleBackup(req.Id)
	if err != nil {
		return nil, fmt.Errorf("get backup metadata: %w", err)
	}

	if info.Encrypted && req.Password == "" {
		return nil, fmt.Errorf("backup is encrypted: password required")
	}

	data, err := s.storage.LoadModuleBackupData(req.Id, req.Password)
	if err != nil {
		return nil, fmt.Errorf("load backup data: %w", err)
	}

	filename := fmt.Sprintf("%s-%s-%s.json", info.ModuleId, info.Id[:8], info.CreatedAt.AsTime().Format("20060102"))
	return &backupV1.DownloadBackupResponse{
		Data:     data,
		Filename: filename,
	}, nil
}

// --- Full Platform Operations ---

func (s *OrchestratorService) CreateFullBackup(ctx context.Context, req *backupV1.CreateFullBackupRequest) (*backupV1.CreateFullBackupResponse, error) {
	if len(req.Targets) == 0 {
		return nil, fmt.Errorf("at least one target is required")
	}

	username := getUsernameFromContext(ctx)
	now := time.Now()
	backupID := uuid.New().String()

	s.log.Infof("Creating full backup %s for %d modules", backupID, len(req.Targets))

	type moduleResult struct {
		target *backupV1.ModuleTarget
		result *ExportResult
		err    error
	}

	results := make([]moduleResult, len(req.Targets))
	var wg sync.WaitGroup

	for i, target := range req.Targets {
		wg.Add(1)
		go func(idx int, t *backupV1.ModuleTarget) {
			defer wg.Done()
			result, err := s.moduleClient.ExportBackup(ctx, t, req.TenantId, req.IncludeSecrets)
			results[idx] = moduleResult{target: t, result: result, err: err}
		}(i, target)
	}
	wg.Wait()

	var moduleBackups []*backupV1.BackupInfo
	moduleData := make(map[string][]byte)
	var totalSize int64
	var errors []string

	for _, mr := range results {
		if mr.err != nil {
			s.log.Warnf("ExportBackup failed for %s: %v", mr.target.ModuleId, mr.err)
			errors = append(errors, fmt.Sprintf("%s: %v", mr.target.ModuleId, mr.err))
			moduleBackups = append(moduleBackups, &backupV1.BackupInfo{
				ModuleId: mr.target.ModuleId,
				Status:   "failed",
				Warnings: []string{mr.err.Error()},
			})
			continue
		}

		moduleBackups = append(moduleBackups, &backupV1.BackupInfo{
			ModuleId:     mr.target.ModuleId,
			TenantId:     mr.result.TenantID,
			FullBackup:   req.TenantId != nil && *req.TenantId == 0,
			Status:       "completed",
			SizeBytes:    int64(len(mr.result.Data)),
			EntityCounts: mr.result.EntityCounts,
			Version:      mr.result.Version,
		})

		moduleData[mr.target.ModuleId] = mr.result.Data
		totalSize += int64(len(mr.result.Data))
	}

	status := "completed"
	if len(errors) > 0 && len(errors) == len(req.Targets) {
		status = "failed"
	} else if len(errors) > 0 {
		status = "partial"
	}

	info := &backupV1.FullBackupInfo{
		Id:             backupID,
		Description:    req.Description,
		TenantId:       tenantIDValue(req.TenantId),
		FullBackup:     req.TenantId != nil && *req.TenantId == 0,
		Status:         status,
		TotalSizeBytes: totalSize,
		ModuleBackups:  moduleBackups,
		CreatedAt:      timestamppb.New(now),
		CreatedBy:      username,
		Errors:         errors,
	}

	if err := s.storage.SaveFullBackup(info, moduleData, req.Password); err != nil {
		return nil, fmt.Errorf("save full backup: %w", err)
	}

	s.log.Infof("Full backup completed: id=%s modules=%d status=%s", backupID, len(req.Targets), status)
	return &backupV1.CreateFullBackupResponse{Backup: info}, nil
}

func (s *OrchestratorService) RestoreFullBackup(ctx context.Context, req *backupV1.RestoreFullBackupRequest) (*backupV1.RestoreFullBackupResponse, error) {
	if len(req.Targets) == 0 {
		return nil, fmt.Errorf("at least one target is required")
	}

	info, err := s.storage.GetFullBackup(req.BackupId)
	if err != nil {
		return nil, fmt.Errorf("get full backup: %w", err)
	}

	s.log.Infof("Restoring full backup %s to %d modules", req.BackupId, len(req.Targets))

	// Build a map of module_id -> target for quick lookup
	targetMap := make(map[string]*backupV1.ModuleTarget, len(req.Targets))
	for _, t := range req.Targets {
		targetMap[t.ModuleId] = t
	}

	var moduleResults []*backupV1.ModuleRestoreResult
	allSuccess := true

	for _, mb := range info.ModuleBackups {
		if mb.Status != "completed" {
			continue
		}

		target, ok := targetMap[mb.ModuleId]
		if !ok {
			moduleResults = append(moduleResults, &backupV1.ModuleRestoreResult{
				ModuleId: mb.ModuleId,
				Success:  false,
				Error:    "no target endpoint provided for this module",
			})
			allSuccess = false
			continue
		}

		data, err := s.storage.LoadFullBackupModuleData(req.BackupId, mb.ModuleId, req.Password)
		if err != nil {
			moduleResults = append(moduleResults, &backupV1.ModuleRestoreResult{
				ModuleId: mb.ModuleId,
				Success:  false,
				Error:    fmt.Sprintf("load data: %v", err),
			})
			allSuccess = false
			continue
		}

		resp, err := s.moduleClient.ImportBackup(ctx, target, data, req.Mode)
		if err != nil {
			moduleResults = append(moduleResults, &backupV1.ModuleRestoreResult{
				ModuleId: mb.ModuleId,
				Success:  false,
				Error:    err.Error(),
			})
			allSuccess = false
			continue
		}

		results := make([]*backupV1.EntityImportResult, len(resp.Results))
		for i, r := range resp.Results {
			results[i] = &backupV1.EntityImportResult{
				EntityType: r.EntityType,
				Total:      r.Total,
				Created:    r.Created,
				Updated:    r.Updated,
				Skipped:    r.Skipped,
				Failed:     r.Failed,
			}
		}

		moduleResults = append(moduleResults, &backupV1.ModuleRestoreResult{
			ModuleId: mb.ModuleId,
			Success:  resp.Success,
			Results:  results,
			Warnings: resp.Warnings,
		})
	}

	s.log.Infof("Full restore completed: backup=%s success=%v", req.BackupId, allSuccess)
	return &backupV1.RestoreFullBackupResponse{
		Success:       allSuccess,
		ModuleResults: moduleResults,
	}, nil
}

func (s *OrchestratorService) ListFullBackups(ctx context.Context, req *backupV1.ListFullBackupsRequest) (*backupV1.ListFullBackupsResponse, error) {
	backups, err := s.storage.ListFullBackups(req.TenantId)
	if err != nil {
		return nil, fmt.Errorf("list full backups: %w", err)
	}

	total := int32(len(backups))
	page, pageSize := normalizePagination(req.Page, req.PageSize)
	start := (page - 1) * pageSize
	if start >= total {
		return &backupV1.ListFullBackupsResponse{Total: total}, nil
	}
	end := start + pageSize
	if end > total {
		end = total
	}

	return &backupV1.ListFullBackupsResponse{
		Backups: backups[start:end],
		Total:   total,
	}, nil
}

func (s *OrchestratorService) GetFullBackup(ctx context.Context, req *backupV1.GetFullBackupRequest) (*backupV1.GetFullBackupResponse, error) {
	info, err := s.storage.GetFullBackup(req.Id)
	if err != nil {
		return nil, fmt.Errorf("get full backup: %w", err)
	}
	return &backupV1.GetFullBackupResponse{Backup: info}, nil
}

func (s *OrchestratorService) DeleteFullBackup(ctx context.Context, req *backupV1.DeleteFullBackupRequest) (*backupV1.DeleteFullBackupResponse, error) {
	if err := s.storage.DeleteFullBackup(req.Id); err != nil {
		return nil, fmt.Errorf("delete full backup: %w", err)
	}
	s.log.Infof("Deleted full backup: %s", req.Id)
	return &backupV1.DeleteFullBackupResponse{Success: true}, nil
}

// --- Helpers ---

func tenantIDValue(tid *uint32) uint32 {
	if tid != nil {
		return *tid
	}
	return 0
}

func normalizePagination(page, pageSize int32) (int32, int32) {
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 20
	}
	if pageSize > 100 {
		pageSize = 100
	}
	return page, pageSize
}
