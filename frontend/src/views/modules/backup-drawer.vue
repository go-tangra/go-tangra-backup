<script lang="ts" setup>
import { ref, computed } from 'vue';

import { useVbenDrawer } from 'shell/vben/common-ui';

import { Descriptions, DescriptionsItem, Tag, Space } from 'ant-design-vue';

import { $t } from 'shell/locales';
import type { BackupInfo } from '../../api/services';

const data = ref<{ row?: BackupInfo }>();

function formatBytes(bytes: number | string): string {
  const n = Number(bytes);
  if (!n || n === 0) return '0 B';
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(n) / Math.log(1024));
  return `${(n / 1024 ** i).toFixed(1)} ${sizes[i]}`;
}

function statusColor(status: string) {
  switch (status) {
    case 'completed': {
      return 'green';
    }
    case 'failed': {
      return 'red';
    }
    default: {
      return 'default';
    }
  }
}

const [Drawer, drawerApi] = useVbenDrawer({
  onCancel() {
    drawerApi.close();
  },
  async onOpenChange(isOpen) {
    if (isOpen) {
      data.value = drawerApi.getData() as { row?: BackupInfo };
    }
  },
});

const backup = computed(() => data.value?.row);

const entityCountEntries = computed(() => {
  if (!backup.value?.entityCounts) return [];
  return Object.entries(backup.value.entityCounts);
});
</script>

<template>
  <Drawer :title="$t('backup.page.module.view')" :footer="false">
    <template v-if="backup">
      <Descriptions :column="1" bordered size="small">
        <DescriptionsItem label="ID">
          {{ backup.id }}
        </DescriptionsItem>
        <DescriptionsItem :label="$t('backup.page.module.moduleId')">
          <Tag color="blue">{{ backup.moduleId }}</Tag>
        </DescriptionsItem>
        <DescriptionsItem :label="$t('backup.page.module.description')">
          {{ backup.description || '-' }}
        </DescriptionsItem>
        <DescriptionsItem :label="$t('backup.page.module.status')">
          <Tag :color="statusColor(backup.status)">{{ backup.status }}</Tag>
        </DescriptionsItem>
        <DescriptionsItem :label="$t('backup.page.module.sizeBytes')">
          {{ formatBytes(backup.sizeBytes) }}
        </DescriptionsItem>
        <DescriptionsItem :label="$t('backup.page.module.version')">
          {{ backup.version || '-' }}
        </DescriptionsItem>
        <DescriptionsItem :label="$t('backup.page.module.tenantId')">
          {{ backup.tenantId }}
        </DescriptionsItem>
        <DescriptionsItem :label="$t('backup.page.module.fullBackup')">
          <Tag :color="backup.fullBackup ? 'green' : 'default'">
            {{ backup.fullBackup ? $t('backup.page.module.yes') : $t('backup.page.module.no') }}
          </Tag>
        </DescriptionsItem>
        <DescriptionsItem :label="$t('backup.page.module.encrypted')">
          <Tag :color="backup.encrypted ? 'blue' : 'default'">
            {{ backup.encrypted ? $t('backup.page.module.yes') : $t('backup.page.module.no') }}
          </Tag>
        </DescriptionsItem>
        <DescriptionsItem :label="$t('backup.page.module.entityCounts')">
          <Space v-if="entityCountEntries.length" wrap>
            <Tag v-for="[key, count] in entityCountEntries" :key="key">
              {{ key }}: {{ count }}
            </Tag>
          </Space>
          <span v-else>-</span>
        </DescriptionsItem>
        <DescriptionsItem :label="$t('backup.page.module.createdBy')">
          {{ backup.createdBy || '-' }}
        </DescriptionsItem>
        <DescriptionsItem :label="$t('backup.page.module.createdAt')">
          {{ backup.createdAt || '-' }}
        </DescriptionsItem>
        <DescriptionsItem
          v-if="backup.warnings && backup.warnings.length"
          :label="$t('backup.page.module.warnings')"
        >
          <Tag v-for="(w, i) in backup.warnings" :key="i" color="orange">
            {{ w }}
          </Tag>
        </DescriptionsItem>
      </Descriptions>
    </template>
  </Drawer>
</template>
