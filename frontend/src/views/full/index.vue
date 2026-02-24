<script lang="ts" setup>
import type { VxeGridProps } from 'shell/adapter/vxe-table';

import { h, ref } from 'vue';

import { Page, useVbenDrawer, type VbenFormProps } from 'shell/vben/common-ui';
import { LucideDownload, LucideEye, LucideLock, LucideTrash, LucideRotateCcw } from 'shell/vben/icons';

import { Modal, InputPassword, notification, Space, Button, Tag } from 'ant-design-vue';

import { useVbenVxeGrid } from 'shell/adapter/vxe-table';
import { $t } from 'shell/locales';
import { useBackupFullStore } from '../../stores/backup-full.state';
import type { FullBackupInfo } from '../../api/services';

import BackupDrawer from './backup-drawer.vue';
import CreateDrawer from './create-drawer.vue';
import RestoreDrawer from './restore-drawer.vue';
import UploadRestoreDrawer from './upload-restore-drawer.vue';

const fullStore = useBackupFullStore();

const formOptions: VbenFormProps = {
  collapsed: false,
  showCollapseButton: false,
  submitOnEnter: true,
  schema: [],
};

function statusColor(status: string) {
  switch (status) {
    case 'completed': {
      return 'green';
    }
    case 'failed': {
      return 'red';
    }
    case 'partial': {
      return 'orange';
    }
    default: {
      return 'default';
    }
  }
}

function formatBytes(bytes: number | string): string {
  const n = Number(bytes);
  if (!n || n === 0) return '0 B';
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(n) / Math.log(1024));
  return `${(n / 1024 ** i).toFixed(1)} ${sizes[i]}`;
}

const gridOptions: VxeGridProps<FullBackupInfo> = {
  height: 'auto',
  stripe: false,
  toolbarConfig: {
    custom: true,
    export: false,
    import: false,
    refresh: true,
    zoom: true,
  },
  rowConfig: {
    isHover: true,
  },
  pagerConfig: {
    enabled: true,
    pageSize: 20,
    pageSizes: [10, 20, 50, 100],
  },

  proxyConfig: {
    ajax: {
      query: async ({ page }) => {
        const resp = await fullStore.listFullBackups({
          page: page.currentPage,
          pageSize: page.pageSize,
        });
        return {
          items: resp.backups ?? [],
          total: resp.total ?? 0,
        };
      },
    },
  },

  columns: [
    { title: $t('ui.table.seq'), type: 'seq', width: 50 },
    {
      title: $t('backup.page.full.description'),
      field: 'description',
      minWidth: 200,
    },
    {
      title: $t('backup.page.full.status'),
      field: 'status',
      width: 110,
      slots: { default: 'status' },
    },
    {
      title: $t('backup.page.full.totalSize'),
      field: 'totalSizeBytes',
      width: 110,
      slots: { default: 'totalSizeBytes' },
    },
    {
      title: $t('backup.page.full.moduleCount'),
      field: 'moduleBackups',
      width: 100,
      slots: { default: 'module_count' },
    },
    {
      title: $t('backup.page.full.createdBy'),
      field: 'createdBy',
      width: 120,
    },
    {
      title: $t('backup.page.full.createdAt'),
      field: 'createdAt',
      width: 160,
      sortable: true,
    },
    {
      title: $t('backup.page.module.encrypted'),
      field: 'encrypted',
      width: 80,
      slots: { default: 'encrypted' },
    },
    {
      title: $t('ui.table.action'),
      field: 'action',
      fixed: 'right',
      slots: { default: 'action' },
      width: 190,
    },
  ],
};

const [Grid, gridApi] = useVbenVxeGrid({ gridOptions, formOptions });

const [BackupDrawerComponent, backupDrawerApi] = useVbenDrawer({
  connectedComponent: BackupDrawer,
});

const [CreateDrawerComponent, createDrawerApi] = useVbenDrawer({
  connectedComponent: CreateDrawer,
  onOpenChange(isOpen: boolean) {
    if (!isOpen) {
      gridApi.query();
    }
  },
});

const [RestoreDrawerComponent, restoreDrawerApi] = useVbenDrawer({
  connectedComponent: RestoreDrawer,
  onOpenChange(isOpen: boolean) {
    if (!isOpen) {
      gridApi.query();
    }
  },
});

const [UploadRestoreDrawerComponent, uploadRestoreDrawerApi] = useVbenDrawer({
  connectedComponent: UploadRestoreDrawer,
  onOpenChange(isOpen: boolean) {
    if (!isOpen) {
      gridApi.query();
    }
  },
});

function handleUploadRestore() {
  uploadRestoreDrawerApi.setData({});
  uploadRestoreDrawerApi.open();
}

function handleView(row: FullBackupInfo) {
  backupDrawerApi.setData({ row });
  backupDrawerApi.open();
}

function handleCreate() {
  createDrawerApi.setData({});
  createDrawerApi.open();
}

function handleRestore(row: FullBackupInfo) {
  restoreDrawerApi.setData({ row });
  restoreDrawerApi.open();
}

const downloadPassword = ref('');

function handleDownload(row: FullBackupInfo) {
  if (!row.id || row.status === 'failed') return;

  if (row.encrypted) {
    downloadPassword.value = '';
    Modal.confirm({
      title: $t('backup.page.module.downloadPasswordTitle'),
      content: () =>
        h('div', [
          h('p', { class: 'mb-2' }, $t('backup.page.module.downloadPasswordPrompt')),
          h(InputPassword, {
            value: downloadPassword.value,
            'onUpdate:value': (val: string) => {
              downloadPassword.value = val;
            },
            placeholder: $t('backup.page.module.enterPassword'),
          }),
        ]),
      okText: $t('backup.page.module.download'),
      async onOk() {
        try {
          await fullStore.downloadFullBackup(row.id, downloadPassword.value);
          notification.success({ message: $t('backup.page.module.downloadSuccess') });
        } catch {
          notification.error({ message: $t('backup.page.module.downloadFailed') });
        }
      },
    });
  } else {
    fullStore
      .downloadFullBackup(row.id)
      .then(() => {
        notification.success({ message: $t('backup.page.module.downloadSuccess') });
      })
      .catch(() => {
        notification.error({ message: $t('backup.page.module.downloadFailed') });
      });
  }
}

async function handleDelete(row: FullBackupInfo) {
  if (!row.id) return;
  try {
    await fullStore.deleteFullBackup(row.id);
    notification.success({
      message: $t('backup.page.full.deleteSuccess'),
    });
    await gridApi.query();
  } catch {
    notification.error({ message: $t('ui.notification.delete_failed') });
  }
}
</script>

<template>
  <Page auto-content-height>
    <Grid :table-title="$t('backup.page.full.title')">
      <template #toolbar-tools>
        <Button class="mr-2" @click="handleUploadRestore">
          {{ $t('backup.page.full.uploadRestore') }}
        </Button>
        <Button class="mr-2" type="primary" @click="handleCreate">
          {{ $t('backup.page.full.create') }}
        </Button>
      </template>
      <template #status="{ row }">
        <Tag :color="statusColor(row.status)">{{ row.status }}</Tag>
      </template>
      <template #totalSizeBytes="{ row }">
        {{ formatBytes(row.totalSizeBytes) }}
      </template>
      <template #module_count="{ row }">
        {{ row.moduleBackups?.length ?? 0 }}
      </template>
      <template #encrypted="{ row }">
        <Tag v-if="row.encrypted" color="blue">
          <component :is="LucideLock" class="inline-block h-3 w-3" />
        </Tag>
        <span v-else>-</span>
      </template>
      <template #action="{ row }">
        <Space>
          <Button
            type="link"
            size="small"
            :icon="h(LucideEye)"
            :title="$t('backup.page.full.view')"
            @click.stop="handleView(row)"
          />
          <Button
            type="link"
            size="small"
            :icon="h(LucideDownload)"
            :title="$t('backup.page.module.download')"
            :disabled="row.status === 'failed'"
            @click.stop="handleDownload(row)"
          />
          <Button
            type="link"
            size="small"
            :icon="h(LucideRotateCcw)"
            :title="$t('backup.page.full.restore')"
            :disabled="row.status !== 'completed'"
            @click.stop="handleRestore(row)"
          />
          <a-popconfirm
            :cancel-text="$t('ui.button.cancel')"
            :ok-text="$t('ui.button.ok')"
            :title="$t('backup.page.full.confirmDelete')"
            @confirm="handleDelete(row)"
          >
            <Button
              danger
              type="link"
              size="small"
              :icon="h(LucideTrash)"
              :title="$t('ui.button.delete', { moduleName: '' })"
            />
          </a-popconfirm>
        </Space>
      </template>
    </Grid>

    <BackupDrawerComponent />
    <CreateDrawerComponent />
    <RestoreDrawerComponent />
    <UploadRestoreDrawerComponent />
  </Page>
</template>
