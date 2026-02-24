<script lang="ts" setup>
import { ref, computed } from 'vue';

import { useVbenDrawer } from 'shell/vben/common-ui';
import { LucideUpload, LucideFileJson, LucideX } from 'shell/vben/icons';
import { useAccessStore } from 'shell/vben/stores';

import {
  Upload,
  Form,
  FormItem,
  Select,
  SelectOption,
  Button,
  Space,
  Alert,
  notification,
} from 'ant-design-vue';
import type { UploadChangeParam } from 'ant-design-vue';

import { $t } from 'shell/locales';
import { useModuleRegistrationStore } from 'shell/stores';
import type { Module } from '../../api/services';

const moduleRegStore = useModuleRegistrationStore();
const loading = ref(false);
const modules = ref<Module[]>([]);

const fileContent = ref<string>('');
const fileName = ref<string>('');

const formState = ref({
  moduleId: undefined as string | undefined,
  mode: 'RESTORE_MODE_SKIP' as 'RESTORE_MODE_OVERWRITE' | 'RESTORE_MODE_SKIP',
});

const availableModules = computed(() =>
  modules.value.filter((m) => m.moduleId && m.moduleId !== 'backup'),
);

const canSubmit = computed(
  () => fileContent.value && formState.value.moduleId,
);

function handleFileChange(info: UploadChangeParam) {
  let file: File | undefined;

  if (info.file instanceof File) {
    file = info.file;
  } else if (info.file?.originFileObj) {
    file = info.file.originFileObj;
  } else if (info.fileList?.[0]?.originFileObj) {
    file = info.fileList[0].originFileObj;
  }

  if (!file) return;
  readFile(file);
}

function handleDrop(e: DragEvent) {
  e.preventDefault();
  const file = e.dataTransfer?.files[0];
  if (!file) return;

  if (!file.name.endsWith('.json')) {
    notification.error({ message: $t('backup.page.module.supportedFormat') });
    return;
  }

  readFile(file);
}

function readFile(file: File) {
  fileName.value = file.name;
  const reader = new FileReader();
  reader.onload = (e) => {
    fileContent.value = e.target?.result as string;
  };
  reader.readAsText(file);
}

function resetFile() {
  fileContent.value = '';
  fileName.value = '';
}

async function handleSubmit() {
  if (!canSubmit.value) return;
  loading.value = true;

  try {
    const accessStore = useAccessStore();
    const token = accessStore.accessToken;
    const url = `/admin/v1/modules/${formState.value.moduleId}/v1/backup/import`;

    const base64data = btoa(
      new Uint8Array(new TextEncoder().encode(fileContent.value)).reduce(
        (data, byte) => data + String.fromCodePoint(byte),
        '',
      ),
    );

    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
      },
      body: JSON.stringify({
        data: base64data,
        mode: formState.value.mode,
      }),
    });

    if (!response.ok) {
      const errorBody = await response.text();
      throw new Error(errorBody || `HTTP ${response.status}`);
    }

    const result = await response.json();

    if (result.warnings?.length) {
      notification.warning({
        message: $t('backup.page.module.uploadRestoreSuccess'),
        description: result.warnings.join(', '),
      });
    } else {
      notification.success({
        message: $t('backup.page.module.uploadRestoreSuccess'),
      });
    }

    drawerApi.close();
  } catch (e: any) {
    console.error('Upload restore failed:', e);
    notification.error({ message: e.message || 'Restore failed' });
  } finally {
    loading.value = false;
  }
}

function resetState() {
  fileContent.value = '';
  fileName.value = '';
  formState.value = {
    moduleId: undefined,
    mode: 'RESTORE_MODE_SKIP',
  };
}

const [Drawer, drawerApi] = useVbenDrawer({
  onCancel() {
    drawerApi.close();
  },
  async onOpenChange(isOpen) {
    if (isOpen) {
      resetState();
      modules.value = await moduleRegStore.listModules();
    }
  },
});
</script>

<template>
  <Drawer :title="$t('backup.page.module.uploadRestoreTitle')" :footer="false">
    <div class="space-y-4">
      <!-- File Upload Area -->
      <div
        v-if="!fileContent"
        class="border-2 border-dashed border-gray-300 rounded-lg p-8 text-center hover:border-primary transition-colors cursor-pointer"
        @drop="handleDrop"
        @dragover.prevent
      >
        <Upload
          accept=".json"
          :show-upload-list="false"
          :before-upload="() => false"
          @change="handleFileChange"
        >
          <div class="flex flex-col items-center gap-4">
            <LucideUpload class="size-12 text-gray-400" />
            <div>
              <p class="text-base font-medium">
                {{ $t('backup.page.module.dropBackupFile') }}
              </p>
              <p class="text-sm text-gray-500">
                {{ $t('backup.page.module.supportedFormat') }}
              </p>
            </div>
          </div>
        </Upload>
      </div>

      <!-- File Info -->
      <div v-else class="flex items-center gap-2 p-3 bg-gray-50 rounded-lg">
        <LucideFileJson class="size-5 text-blue-500" />
        <span class="font-medium">{{ fileName }}</span>
        <button
          class="ml-auto text-gray-400 hover:text-red-500"
          @click="resetFile"
        >
          <LucideX class="size-4" />
        </button>
      </div>

      <!-- Form -->
      <Form layout="vertical" :model="formState" @finish="handleSubmit">
        <FormItem
          :label="$t('backup.page.module.selectModule')"
          name="moduleId"
          required
        >
          <Select
            v-model:value="formState.moduleId"
            :placeholder="$t('backup.page.module.moduleIdPlaceholder')"
            show-search
            option-filter-prop="label"
          >
            <SelectOption
              v-for="m in availableModules"
              :key="m.moduleId"
              :value="m.moduleId"
              :label="m.moduleName || m.moduleId"
            >
              {{ m.moduleName || m.moduleId }}
            </SelectOption>
          </Select>
        </FormItem>

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

        <Alert
          v-if="fileContent && formState.moduleId"
          class="mb-4"
          type="info"
          show-icon
          :message="
            $t('backup.page.module.restore') +
            ' → ' +
            formState.moduleId
          "
        />

        <FormItem>
          <Space>
            <Button
              type="primary"
              html-type="submit"
              :loading="loading"
              :disabled="!canSubmit"
            >
              {{ $t('backup.page.module.restore') }}
            </Button>
          </Space>
        </FormItem>
      </Form>
    </div>
  </Drawer>
</template>
