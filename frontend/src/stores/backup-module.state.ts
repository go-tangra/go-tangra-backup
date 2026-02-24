import { defineStore } from 'pinia';

import {
  ModuleBackupService,
  type BackupInfo,
  type CreateModuleBackupRequest,
  type CreateModuleBackupResponse,
  type DownloadBackupResponse,
  type GetBackupResponse,
  type ListBackupsResponse,
  type RestoreModuleBackupRequest,
  type RestoreModuleBackupResponse,
} from '../api/services';

export const useBackupModuleStore = defineStore('backup-module', () => {
  async function listBackups(
    paging?: { page?: number; pageSize?: number },
    filters?: { module_id?: string } | null,
  ): Promise<ListBackupsResponse> {
    return await ModuleBackupService.list({
      page: paging?.page,
      page_size: paging?.pageSize,
      module_id: filters?.module_id,
    });
  }

  async function getBackup(id: string): Promise<BackupInfo> {
    const resp: GetBackupResponse = await ModuleBackupService.get(id);
    return resp.backup;
  }

  async function createBackup(
    data: CreateModuleBackupRequest,
  ): Promise<CreateModuleBackupResponse> {
    return await ModuleBackupService.create(data);
  }

  async function restoreBackup(
    backupId: string,
    data: RestoreModuleBackupRequest,
  ): Promise<RestoreModuleBackupResponse> {
    return await ModuleBackupService.restore(backupId, data);
  }

  async function deleteBackup(id: string): Promise<void> {
    return await ModuleBackupService.delete(id);
  }

  async function downloadBackup(id: string, password?: string): Promise<void> {
    const resp: DownloadBackupResponse = await ModuleBackupService.download(
      id,
      password ? { password } : {},
    );

    // Decode base64 data and trigger browser download
    const byteCharacters = atob(resp.data);
    const byteNumbers = new Array(byteCharacters.length);
    for (let i = 0; i < byteCharacters.length; i++) {
      byteNumbers[i] = byteCharacters.charCodeAt(i);
    }
    const byteArray = new Uint8Array(byteNumbers);
    const blob = new Blob([byteArray], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = resp.filename || 'backup.json';
    document.body.append(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  }

  function $reset() {}

  return {
    $reset,
    listBackups,
    getBackup,
    createBackup,
    restoreBackup,
    deleteBackup,
    downloadBackup,
  };
});
