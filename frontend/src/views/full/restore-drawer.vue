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
import { useBackupFullStore } from '../../stores/backup-full.state';
import { useModuleRegistrationStore } from 'shell/stores';
import type { FullBackupInfo, Module } from '../../api/services';

const fullStore = useBackupFullStore();
const moduleRegStore = useModuleRegistrationStore();
const loading = ref(false);
const modules = ref<Module[]>([]);

const data = ref<{ row?: FullBackupInfo }>();

const formState = ref({
  mode: 'RESTORE_MODE_SKIP' as 'RESTORE_MODE_OVERWRITE' | 'RESTORE_MODE_SKIP',
  password: '',
});

const backup = computed(() => data.value?.row);

const moduleTargets = computed(() => {
  if (!backup.value?.moduleBackups) return [];
  return backup.value.moduleBackups.map((mb) => {
    const registered = modules.value.find((m) => m.moduleId === mb.moduleId);
    return {
      moduleId: mb.moduleId,
      grpcEndpoint: registered?.grpcEndpoint ?? '',
      found: !!registered,
    };
  });
});

const allModulesResolved = computed(() =>
  moduleTargets.value.every((t) => t.found && t.grpcEndpoint),
);

async function handleSubmit() {
  if (!backup.value?.id || !allModulesResolved.value) return;

  const targets = moduleTargets.value
    .filter((t) => t.grpcEndpoint)
    .map((t) => ({
      moduleId: t.moduleId,
      grpcEndpoint: t.grpcEndpoint,
    }));

  if (targets.length === 0) return;

  loading.value = true;
  try {
    const resp = await fullStore.restoreFullBackup(backup.value.id, {
      targets,
      mode: formState.value.mode,
      password: formState.value.password || undefined,
    });
    if (resp.success) {
      notification.success({
        message: $t('backup.page.full.restoreSuccess'),
      });
    } else {
      notification.warning({
        message: 'Restore completed with issues',
        description: resp.moduleResults
          ?.filter((r) => !r.success)
          .map((r) => `${r.moduleId}: ${r.error}`)
          .join(', '),
      });
    }
    drawerApi.close();
  } catch (e) {
    console.error('Failed to restore full backup:', e);
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
      data.value = drawerApi.getData() as { row?: FullBackupInfo };
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
  <Drawer :title="$t('backup.page.full.restore')" :footer="false">
    <template v-if="backup">
      <div class="mb-4">
        <span class="text-muted-foreground">{{ backup.id }}</span>
      </div>

      <div class="mb-4">
        <span class="mb-2 block font-medium">
          {{ $t('backup.page.full.targets') }}
        </span>
        <div class="flex flex-col gap-1">
          <div
            v-for="target in moduleTargets"
            :key="target.moduleId"
            class="flex items-center gap-2"
          >
            <Tag :color="target.found ? 'blue' : 'red'">
              {{ target.moduleId }}
            </Tag>
            <span v-if="target.found" class="text-muted-foreground text-xs">
              {{ target.grpcEndpoint }}
            </span>
            <span v-else class="text-xs text-red-500">
              Module not registered
            </span>
          </div>
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
              :disabled="!allModulesResolved"
            >
              {{ $t('backup.page.full.restore') }}
            </Button>
          </Space>
        </FormItem>
      </Form>
    </template>
  </Drawer>
</template>
