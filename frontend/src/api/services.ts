/**
 * Backup Orchestrator Module Service Functions
 *
 * Typed service methods for the Backup API using dynamic module routing.
 * Base URL: /admin/v1/modules/backup/v1
 */

import { backupApi, type RequestOptions } from './client';

// ==================== Module type (from admin shell) ====================

export interface Module {
  moduleId?: string;
  moduleName?: string;
  grpcEndpoint?: string;
  version?: string;
  status?: string;
}

// ==================== Entity Types ====================

export interface ModuleTarget {
  moduleId: string;
  grpcEndpoint: string;
}

export interface BackupInfo {
  id: string;
  moduleId: string;
  description: string;
  tenantId: number;
  fullBackup: boolean;
  status: string;
  sizeBytes: string | number;
  entityCounts: Record<string, string | number>;
  createdAt: string;
  createdBy: string;
  version: string;
  warnings: string[];
  encrypted: boolean;
}

export interface FullBackupInfo {
  id: string;
  description: string;
  tenantId: number;
  fullBackup: boolean;
  status: string;
  totalSizeBytes: string | number;
  moduleBackups: BackupInfo[];
  createdAt: string;
  createdBy: string;
  errors: string[];
  encrypted: boolean;
}

export interface EntityImportResult {
  entityType: string;
  total: number;
  created: number;
  updated: number;
  skipped: number;
  failed: number;
}

export interface ModuleRestoreResult {
  moduleId: string;
  success: boolean;
  results: EntityImportResult[];
  warnings: string[];
  error: string;
}

// ==================== Request/Response Types ====================

export interface CreateModuleBackupRequest {
  target: ModuleTarget;
  tenantId?: number;
  description: string;
  includeSecrets?: boolean;
  password?: string;
}

export interface CreateModuleBackupResponse {
  backup: BackupInfo;
}

export interface RestoreModuleBackupRequest {
  target: ModuleTarget;
  mode: 'RESTORE_MODE_OVERWRITE' | 'RESTORE_MODE_SKIP';
  password?: string;
}

export interface RestoreModuleBackupResponse {
  success: boolean;
  results: EntityImportResult[];
  warnings: string[];
}

export interface ListBackupsResponse {
  backups: BackupInfo[];
  total: number;
}

export interface GetBackupResponse {
  backup: BackupInfo;
}

export interface DownloadBackupRequest {
  password?: string;
}

export interface DownloadBackupResponse {
  data: string;
  filename: string;
}

export interface CreateFullBackupRequest {
  targets: ModuleTarget[];
  tenantId?: number;
  description: string;
  includeSecrets?: boolean;
  password?: string;
}

export interface CreateFullBackupResponse {
  backup: FullBackupInfo;
}

export interface RestoreFullBackupRequest {
  targets: ModuleTarget[];
  mode: 'RESTORE_MODE_OVERWRITE' | 'RESTORE_MODE_SKIP';
  password?: string;
}

export interface RestoreFullBackupResponse {
  success: boolean;
  moduleResults: ModuleRestoreResult[];
}

export interface ListFullBackupsResponse {
  backups: FullBackupInfo[];
  total: number;
}

export interface GetFullBackupResponse {
  backup: FullBackupInfo;
}

export interface DownloadFullBackupRequest {
  password?: string;
}

export interface DownloadFullBackupResponse {
  data: string;
  filename: string;
}

// ==================== Helper ====================

function buildQuery(params: Record<string, unknown>): string {
  const searchParams = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== '') {
      searchParams.append(key, String(value));
    }
  }
  const query = searchParams.toString();
  return query ? `?${query}` : '';
}

// ==================== Module Backup Service ====================

export const ModuleBackupService = {
  create: (data: CreateModuleBackupRequest, options?: RequestOptions) =>
    backupApi.post<CreateModuleBackupResponse>('/backups/modules', data, options),

  restore: (
    backupId: string,
    data: RestoreModuleBackupRequest,
    options?: RequestOptions,
  ) =>
    backupApi.post<RestoreModuleBackupResponse>(
      `/backups/${backupId}/restore`,
      data,
      options,
    ),

  list: (
    params?: {
      module_id?: string;
      tenant_id?: number;
      page?: number;
      page_size?: number;
    },
    options?: RequestOptions,
  ) => {
    const qs = buildQuery({
      module_id: params?.module_id,
      tenant_id: params?.tenant_id,
      page: params?.page,
      page_size: params?.page_size,
    });
    return backupApi.get<ListBackupsResponse>(`/backups${qs}`, options);
  },

  get: (id: string, options?: RequestOptions) =>
    backupApi.get<GetBackupResponse>(`/backups/${id}`, options),

  delete: (id: string, options?: RequestOptions) =>
    backupApi.delete<void>(`/backups/${id}`, options),

  download: (id: string, data?: DownloadBackupRequest, options?: RequestOptions) =>
    backupApi.post<DownloadBackupResponse>(`/backups/${id}/download`, data ?? {}, options),
};

// ==================== Full Backup Service ====================

export const FullBackupService = {
  create: (data: CreateFullBackupRequest, options?: RequestOptions) =>
    backupApi.post<CreateFullBackupResponse>('/backups/full', data, options),

  restore: (
    backupId: string,
    data: RestoreFullBackupRequest,
    options?: RequestOptions,
  ) =>
    backupApi.post<RestoreFullBackupResponse>(
      `/backups/full/${backupId}/restore`,
      data,
      options,
    ),

  list: (
    params?: {
      tenant_id?: number;
      page?: number;
      page_size?: number;
    },
    options?: RequestOptions,
  ) => {
    const qs = buildQuery({
      tenant_id: params?.tenant_id,
      page: params?.page,
      page_size: params?.page_size,
    });
    return backupApi.get<ListFullBackupsResponse>(`/backups/full${qs}`, options);
  },

  get: (id: string, options?: RequestOptions) =>
    backupApi.get<GetFullBackupResponse>(`/backups/full/${id}`, options),

  download: (id: string, data?: DownloadFullBackupRequest, options?: RequestOptions) =>
    backupApi.post<DownloadFullBackupResponse>(`/backups/full/${id}/download`, data ?? {}, options),

  delete: (id: string, options?: RequestOptions) =>
    backupApi.delete<void>(`/backups/full/${id}`, options),
};
