package service

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/tx7do/kratos-bootstrap/bootstrap"
	"google.golang.org/protobuf/encoding/protojson"

	backupV1 "github.com/go-tangra/go-tangra-backup/gen/go/backup/service/v1"
)

// BackupStorage manages backup metadata and data on the filesystem.
// No database — all state is stored as files.
type BackupStorage struct {
	basePath string
	log      *log.Helper
	mu       sync.RWMutex
}

// NewBackupStorage creates a new filesystem-backed backup storage.
func NewBackupStorage(ctx *bootstrap.Context) *BackupStorage {
	basePath := os.Getenv("BACKUP_STORAGE_PATH")
	if basePath == "" {
		basePath = "/data/backups"
	}

	l := ctx.NewLoggerHelper("backup/storage")

	// Ensure base directories exist
	for _, sub := range []string{"modules", "full"} {
		dir := filepath.Join(basePath, sub)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			l.Warnf("Failed to create storage directory %s: %v", dir, err)
		}
	}

	l.Infof("BackupStorage initialized at %s", basePath)
	return &BackupStorage{basePath: basePath, log: l}
}

// --- Module Backups ---

func (s *BackupStorage) moduleDir(backupID string) string {
	return filepath.Join(s.basePath, "modules", backupID)
}

// SaveModuleBackup persists backup metadata and gzipped data to disk.
func (s *BackupStorage) SaveModuleBackup(info *backupV1.BackupInfo, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	dir := s.moduleDir(info.Id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create backup dir: %w", err)
	}

	// Write metadata (use protojson for correct timestamp/zero-value handling)
	marshaler := protojson.MarshalOptions{Indent: "  ", EmitUnpopulated: true}
	metaBytes, err := marshaler.Marshal(info)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "metadata.json"), metaBytes, 0o644); err != nil {
		return fmt.Errorf("write metadata: %w", err)
	}

	// Write gzipped data
	compressed, err := gzipCompress(data)
	if err != nil {
		return fmt.Errorf("compress data: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "data.json.gz"), compressed, 0o644); err != nil {
		return fmt.Errorf("write data: %w", err)
	}

	s.log.Infof("Saved module backup %s (%d bytes compressed)", info.Id, len(compressed))
	return nil
}

// LoadModuleBackupData reads and decompresses the backup payload.
func (s *BackupStorage) LoadModuleBackupData(backupID string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	compressed, err := os.ReadFile(filepath.Join(s.moduleDir(backupID), "data.json.gz"))
	if err != nil {
		return nil, fmt.Errorf("read backup data: %w", err)
	}
	return gzipDecompress(compressed)
}

// GetModuleBackup reads backup metadata from disk.
func (s *BackupStorage) GetModuleBackup(backupID string) (*backupV1.BackupInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.readModuleMetadata(backupID)
}

func (s *BackupStorage) readModuleMetadata(backupID string) (*backupV1.BackupInfo, error) {
	metaPath := filepath.Join(s.moduleDir(backupID), "metadata.json")
	metaBytes, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, fmt.Errorf("read metadata: %w", err)
	}

	var info backupV1.BackupInfo
	if err := protojson.Unmarshal(metaBytes, &info); err != nil {
		return nil, fmt.Errorf("unmarshal metadata: %w", err)
	}
	return &info, nil
}

// ListModuleBackups returns all module backups, optionally filtered by module and tenant.
func (s *BackupStorage) ListModuleBackups(moduleID string, tenantID *uint32) ([]*backupV1.BackupInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	modulesDir := filepath.Join(s.basePath, "modules")
	entries, err := os.ReadDir(modulesDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read modules dir: %w", err)
	}

	var backups []*backupV1.BackupInfo
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		info, err := s.readModuleMetadata(entry.Name())
		if err != nil {
			s.log.Warnf("Skip backup %s: %v", entry.Name(), err)
			continue
		}
		if moduleID != "" && info.ModuleId != moduleID {
			continue
		}
		if tenantID != nil && info.TenantId != *tenantID {
			continue
		}
		backups = append(backups, info)
	}

	// Sort by creation time descending
	sort.Slice(backups, func(i, j int) bool {
		ti := backups[i].CreatedAt.AsTime()
		tj := backups[j].CreatedAt.AsTime()
		return ti.After(tj)
	})

	return backups, nil
}

// DeleteModuleBackup removes a backup directory.
func (s *BackupStorage) DeleteModuleBackup(backupID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	dir := s.moduleDir(backupID)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return fmt.Errorf("backup not found: %s", backupID)
	}
	return os.RemoveAll(dir)
}

// --- Full Backups ---

func (s *BackupStorage) fullDir(backupID string) string {
	return filepath.Join(s.basePath, "full", backupID)
}

// SaveFullBackup persists a full platform backup manifest and per-module data.
func (s *BackupStorage) SaveFullBackup(info *backupV1.FullBackupInfo, moduleData map[string][]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	dir := s.fullDir(info.Id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create full backup dir: %w", err)
	}

	// Write manifest (use protojson for correct timestamp/zero-value handling)
	marshaler := protojson.MarshalOptions{Indent: "  ", EmitUnpopulated: true}
	metaBytes, err := marshaler.Marshal(info)
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "metadata.json"), metaBytes, 0o644); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	// Write per-module data
	for moduleID, data := range moduleData {
		compressed, err := gzipCompress(data)
		if err != nil {
			return fmt.Errorf("compress %s data: %w", moduleID, err)
		}
		filename := fmt.Sprintf("%s.json.gz", moduleID)
		if err := os.WriteFile(filepath.Join(dir, filename), compressed, 0o644); err != nil {
			return fmt.Errorf("write %s data: %w", moduleID, err)
		}
	}

	s.log.Infof("Saved full backup %s with %d modules", info.Id, len(moduleData))
	return nil
}

// LoadFullBackupModuleData reads and decompresses a single module's data from a full backup.
func (s *BackupStorage) LoadFullBackupModuleData(backupID, moduleID string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	filename := filepath.Join(s.fullDir(backupID), fmt.Sprintf("%s.json.gz", moduleID))
	compressed, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("read module data %s: %w", moduleID, err)
	}
	return gzipDecompress(compressed)
}

// GetFullBackup reads full backup metadata from disk.
func (s *BackupStorage) GetFullBackup(backupID string) (*backupV1.FullBackupInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.readFullMetadata(backupID)
}

func (s *BackupStorage) readFullMetadata(backupID string) (*backupV1.FullBackupInfo, error) {
	metaPath := filepath.Join(s.fullDir(backupID), "metadata.json")
	metaBytes, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}

	var info backupV1.FullBackupInfo
	if err := protojson.Unmarshal(metaBytes, &info); err != nil {
		return nil, fmt.Errorf("unmarshal manifest: %w", err)
	}
	return &info, nil
}

// ListFullBackups returns all full backups, optionally filtered by tenant.
func (s *BackupStorage) ListFullBackups(tenantID *uint32) ([]*backupV1.FullBackupInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	fullDir := filepath.Join(s.basePath, "full")
	entries, err := os.ReadDir(fullDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read full dir: %w", err)
	}

	var backups []*backupV1.FullBackupInfo
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		info, err := s.readFullMetadata(entry.Name())
		if err != nil {
			s.log.Warnf("Skip full backup %s: %v", entry.Name(), err)
			continue
		}
		if tenantID != nil && info.TenantId != *tenantID {
			continue
		}
		backups = append(backups, info)
	}

	// Sort by creation time descending
	sort.Slice(backups, func(i, j int) bool {
		ti := backups[i].CreatedAt.AsTime()
		tj := backups[j].CreatedAt.AsTime()
		return ti.After(tj)
	})

	return backups, nil
}

// DeleteFullBackup removes a full backup directory.
func (s *BackupStorage) DeleteFullBackup(backupID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	dir := s.fullDir(backupID)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return fmt.Errorf("full backup not found: %s", backupID)
	}
	return os.RemoveAll(dir)
}

// --- Compression helpers ---

func gzipCompress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func gzipDecompress(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}
