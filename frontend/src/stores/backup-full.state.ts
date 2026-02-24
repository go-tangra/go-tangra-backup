import { defineStore } from 'pinia';

import {
  FullBackupService,
  type CreateFullBackupRequest,
  type CreateFullBackupResponse,
  type DownloadFullBackupResponse,
  type FullBackupInfo,
  type GetFullBackupResponse,
  type ListFullBackupsResponse,
  type RestoreFullBackupRequest,
  type RestoreFullBackupResponse,
} from '../api/services';

export const useBackupFullStore = defineStore('backup-full', () => {
  async function listFullBackups(
    paging?: { page?: number; pageSize?: number },
  ): Promise<ListFullBackupsResponse> {
    return await FullBackupService.list({
      page: paging?.page,
      page_size: paging?.pageSize,
    });
  }

  async function getFullBackup(id: string): Promise<FullBackupInfo> {
    const resp: GetFullBackupResponse = await FullBackupService.get(id);
    return resp.backup;
  }

  async function createFullBackup(
    data: CreateFullBackupRequest,
  ): Promise<CreateFullBackupResponse> {
    return await FullBackupService.create(data);
  }

  async function restoreFullBackup(
    backupId: string,
    data: RestoreFullBackupRequest,
  ): Promise<RestoreFullBackupResponse> {
    return await FullBackupService.restore(backupId, data);
  }

  async function deleteFullBackup(id: string): Promise<void> {
    return await FullBackupService.delete(id);
  }

  async function downloadFullBackup(
    id: string,
    password?: string,
  ): Promise<void> {
    const resp: DownloadFullBackupResponse = await FullBackupService.download(
      id,
      password ? { password } : {},
    );

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
    a.download = resp.filename || 'full-backup.json';
    document.body.append(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  }

  function $reset() {}

  return {
    $reset,
    listFullBackups,
    getFullBackup,
    createFullBackup,
    restoreFullBackup,
    deleteFullBackup,
    downloadFullBackup,
  };
});
