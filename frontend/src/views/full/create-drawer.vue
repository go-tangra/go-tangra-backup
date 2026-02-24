<script lang="ts" setup>
import { ref } from 'vue';

import { useVbenDrawer } from 'shell/vben/common-ui';

import {
  Form,
  FormItem,
  Input,
  InputPassword,
  Button,
  Space,
  Checkbox,
  CheckboxGroup,
  notification,
} from 'ant-design-vue';

import { $t } from 'shell/locales';
import { useBackupFullStore } from '../../stores/backup-full.state';
import { useModuleRegistrationStore } from 'shell/stores';
import type { Module } from '../../api/services';

const fullStore = useBackupFullStore();
const moduleRegStore = useModuleRegistrationStore();
const loading = ref(false);
const modules = ref<Module[]>([]);

const formState = ref({
  description: '',
  selectedModuleIds: [] as string[],
  includeSecrets: false,
  password: '',
});

function resetForm() {
  formState.value = {
    description: '',
    selectedModuleIds: [],
    includeSecrets: false,
    password: '',
  };
}

function selectAll() {
  formState.value.selectedModuleIds = modules.value
    .map((m) => m.moduleId)
    .filter(Boolean) as string[];
}

async function handleSubmit() {
  const targets = modules.value
    .filter(
      (m) =>
        m.moduleId &&
        m.grpcEndpoint &&
        formState.value.selectedModuleIds.includes(m.moduleId),
    )
    .map((m) => ({
      moduleId: m.moduleId!,
      grpcEndpoint: m.grpcEndpoint!,
    }));

  if (targets.length === 0) return;

  loading.value = true;
  try {
    const resp = await fullStore.createFullBackup({
      targets,
      description: formState.value.description,
      includeSecrets: formState.value.includeSecrets,
      password: formState.value.password || undefined,
    });
    if (resp.backup.status === 'failed') {
      notification.warning({
        message: `Full backup ${resp.backup.status}`,
        description: resp.backup.errors?.join(', '),
      });
    } else {
      notification.success({
        message: $t('backup.page.full.createSuccess'),
      });
    }
    drawerApi.close();
  } catch (e) {
    console.error('Failed to create full backup:', e);
    notification.error({ message: $t('ui.notification.create_failed') });
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
      resetForm();
      modules.value = (await moduleRegStore.listModules()).filter(
        (m) => m.moduleId !== 'backup',
      );
    }
  },
});
</script>

<template>
  <Drawer :title="$t('backup.page.full.create')" :footer="false">
    <Form layout="vertical" :model="formState" @finish="handleSubmit">
      <FormItem
        :label="$t('backup.page.full.description')"
        name="description"
      >
        <Input
          v-model:value="formState.description"
          :placeholder="$t('backup.page.full.descriptionPlaceholder')"
        />
      </FormItem>

      <FormItem
        :label="$t('backup.page.full.targets')"
        name="selectedModuleIds"
        :rules="[{ required: true, message: $t('ui.formRules.required'), type: 'array', min: 1 }]"
      >
        <div class="mb-2">
          <Button size="small" type="link" @click="selectAll">
            {{ $t('ui.button.select_all') }}
          </Button>
        </div>
        <CheckboxGroup v-model:value="formState.selectedModuleIds" class="w-full">
          <div class="flex flex-col gap-2">
            <Checkbox
              v-for="mod in modules"
              :key="mod.moduleId"
              :value="mod.moduleId"
            >
              <span class="font-medium">{{ mod.moduleName || mod.moduleId }}</span>
              <span class="text-muted-foreground ml-1 text-xs">
                ({{ mod.grpcEndpoint }})
              </span>
            </Checkbox>
          </div>
        </CheckboxGroup>
      </FormItem>

      <FormItem name="includeSecrets">
        <Checkbox v-model:checked="formState.includeSecrets">
          {{ $t('backup.page.module.includeSecrets') }}
        </Checkbox>
      </FormItem>

      <FormItem
        :label="$t('backup.page.module.encryptionPassword')"
        name="password"
      >
        <InputPassword
          v-model:value="formState.password"
          :placeholder="$t('backup.page.module.encryptionPasswordPlaceholder')"
          autocomplete="new-password"
        />
        <div class="text-muted-foreground mt-1 text-xs">
          {{ $t('backup.page.module.encryptionPasswordHint') }}
        </div>
      </FormItem>

      <FormItem>
        <Space>
          <Button type="primary" html-type="submit" :loading="loading">
            {{ $t('backup.page.full.create') }}
          </Button>
        </Space>
      </FormItem>
    </Form>
  </Drawer>
</template>
