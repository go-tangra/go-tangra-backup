<script lang="ts" setup>
import { ref, computed } from 'vue';

import { useVbenDrawer } from 'shell/vben/common-ui';

import {
  Descriptions,
  DescriptionsItem,
  Tag,
  Space,
  Collapse,
  CollapsePanel,
} from 'ant-design-vue';

import { $t } from 'shell/locales';
import type {
  FullBackupInfo,
  BackupInfo,
} from '../../api/services';

const data = ref<{ row?: FullBackupInfo }>();

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

function entityCountEntries(backup: BackupInfo) {
  if (!backup.entityCounts) return [];
  return Object.entries(backup.entityCounts);
}

const [Drawer, drawerApi] = useVbenDrawer({
  onCancel() {
    drawerApi.close();
  },
  async onOpenChange(isOpen) {
    if (isOpen) {
      data.value = drawerApi.getData() as { row?: FullBackupInfo };
    }
  },
});

const backup = computed(() => data.value?.row);
</script>

<template>
  <Drawer :title="$t('backup.page.full.view')" :footer="false">
    <template v-if="backup">
      <Descriptions :column="1" bordered size="small">
        <DescriptionsItem label="ID">
          {{ backup.id }}
        </DescriptionsItem>
        <DescriptionsItem :label="$t('backup.page.full.description')">
          {{ backup.description || '-' }}
        </DescriptionsItem>
        <DescriptionsItem :label="$t('backup.page.full.status')">
          <Tag :color="statusColor(backup.status)">{{ backup.status }}</Tag>
        </DescriptionsItem>
        <DescriptionsItem :label="$t('backup.page.full.totalSize')">
          {{ formatBytes(backup.totalSizeBytes) }}
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
        <DescriptionsItem :label="$t('backup.page.full.createdBy')">
          {{ backup.createdBy || '-' }}
        </DescriptionsItem>
        <DescriptionsItem :label="$t('backup.page.full.createdAt')">
          {{ backup.createdAt || '-' }}
        </DescriptionsItem>
        <DescriptionsItem
          v-if="backup.errors && backup.errors.length"
          :label="$t('backup.page.full.errors')"
        >
          <Tag v-for="(e, i) in backup.errors" :key="i" color="red">
            {{ e }}
          </Tag>
        </DescriptionsItem>
      </Descriptions>

      <div
        v-if="backup.moduleBackups && backup.moduleBackups.length"
        class="mt-4"
      >
        <h4 class="mb-2 font-medium">
          {{ $t('backup.page.full.moduleBackups') }}
        </h4>
        <Collapse>
          <CollapsePanel
            v-for="mb in backup.moduleBackups"
            :key="mb.id"
            :header="mb.moduleId"
          >
            <Descriptions :column="1" bordered size="small">
              <DescriptionsItem label="ID">{{ mb.id }}</DescriptionsItem>
              <DescriptionsItem :label="$t('backup.page.module.status')">
                <Tag :color="statusColor(mb.status)">{{ mb.status }}</Tag>
              </DescriptionsItem>
              <DescriptionsItem :label="$t('backup.page.module.sizeBytes')">
                {{ formatBytes(mb.sizeBytes) }}
              </DescriptionsItem>
              <DescriptionsItem :label="$t('backup.page.module.version')">
                {{ mb.version || '-' }}
              </DescriptionsItem>
              <DescriptionsItem :label="$t('backup.page.module.entityCounts')">
                <Space v-if="entityCountEntries(mb).length" wrap>
                  <Tag
                    v-for="[key, count] in entityCountEntries(mb)"
                    :key="key"
                  >
                    {{ key }}: {{ count }}
                  </Tag>
                </Space>
                <span v-else>-</span>
              </DescriptionsItem>
              <DescriptionsItem
                v-if="mb.warnings && mb.warnings.length"
                :label="$t('backup.page.module.warnings')"
              >
                <Tag
                  v-for="(w, i) in mb.warnings"
                  :key="i"
                  color="orange"
                >
                  {{ w }}
                </Tag>
              </DescriptionsItem>
            </Descriptions>
          </CollapsePanel>
        </Collapse>
      </div>
    </template>
  </Drawer>
</template>
