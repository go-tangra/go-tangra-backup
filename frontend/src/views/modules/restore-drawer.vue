<script lang="ts" setup>
import { ref, computed } from 'vue';

import { useVbenDrawer } from 'shell/vben/common-ui';

import {
  Form,
  FormItem,
  InputPassword,
  Select,
  SelectOption,
  Button,
  Space,
  Tag,
  notification,
} from 'ant-design-vue';

import { $t } from 'shell/locales';
import { useBackupModuleStore } from '../../stores/backup-module.state';
import { useModuleRegistrationStore } from 'shell/stores';
import type { BackupInfo, Module } from '../../api/services';

const backupStore = useBackupModuleStore();
const moduleRegStore = useModuleRegistrationStore();
const loading = ref(false);
const modules = ref<Module[]>([]);

const data = ref<{ row?: BackupInfo }>();

const formState = ref({
  mode: 'RESTORE_MODE_SKIP' as 'RESTORE_MODE_OVERWRITE' | 'RESTORE_MODE_SKIP',
  password: '',
});

const backup = computed(() => data.value?.row);

const targetModule = computed(() =>
  modules.value.find((m) => m.moduleId === backup.value?.moduleId),
);

async function handleSubmit() {
  if (!backup.value?.id || !backup.value?.moduleId || !targetModule.value?.grpcEndpoint) return;
  loading.value = true;
  try {
    const resp = await backupStore.restoreBackup(backup.value.id, {
      target: {
        moduleId: backup.value.moduleId,
        grpcEndpoint: targetModule.value.grpcEndpoint,
      },
      mode: formState.value.mode,
      password: formState.value.password || undefined,
    });
    if (resp.success) {
      notification.success({
        message: $t('backup.page.module.restoreSuccess'),
      });
    } else {
      notification.warning({
        message: 'Restore completed with issues',
        description: resp.warnings?.join(', '),
      });
    }
    drawerApi.close();
  } catch (e) {
    console.error('Failed to restore backup:', e);
    notification.error({ message: 'Restore failed' });
  } finally {
    loading.value = false;
  }
}

const [Drawer, drawerApi] = useVbenDrawer({
  onCancel() {
    drawerApi.close();
  },
  async onOpenChange(isOpen) {
    if (isOpen) {
      data.value = drawerApi.getData() as { row?: BackupInfo };
      formState.value = {
        mode: 'RESTORE_MODE_SKIP',
        password: '',
      };
      modules.value = await moduleRegStore.listModules();
    }
  },
});
</script>

<template>
  <Drawer :title="$t('backup.page.module.restore')" :footer="false">
    <template v-if="backup">
      <div class="mb-4">
        <Tag color="blue">{{ backup.moduleId }}</Tag>
        <span class="ml-2 text-muted-foreground">{{ backup.id }}</span>
        <div v-if="targetModule" class="text-muted-foreground mt-1 text-xs">
          {{ $t('backup.page.module.grpcEndpoint') }}: {{ targetModule.grpcEndpoint }}
        </div>
      </div>

      <Form layout="vertical" :model="formState" @finish="handleSubmit">
        <FormItem
          :label="$t('backup.page.module.restoreMode')"
          name="mode"
        >
          <Select v-model:value="formState.mode">
            <SelectOption value="RESTORE_MODE_SKIP">
              {{ $t('backup.page.module.restoreModeSkip') }}
            </SelectOption>
            <SelectOption value="RESTORE_MODE_OVERWRITE">
              {{ $t('backup.page.module.restoreModeOverwrite') }}
            </SelectOption>
          </Select>
        </FormItem>

        <FormItem
          v-if="backup.encrypted"
          :label="$t('backup.page.module.decryptionPassword')"
          name="password"
          :rules="[{ required: true, message: $t('ui.formRules.required') }]"
        >
          <InputPassword
            v-model:value="formState.password"
            :placeholder="$t('backup.page.module.enterPassword')"
          />
        </FormItem>

        <FormItem>
          <Space>
            <Button
              type="primary"
              html-type="submit"
              :loading="loading"
              :disabled="!targetModule"
            >
              {{ $t('backup.page.module.restore') }}
            </Button>
          </Space>
        </FormItem>
      </Form>
    </template>
  </Drawer>
</template>
