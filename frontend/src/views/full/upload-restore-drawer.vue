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
  Tag,
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
const detectedModules = ref<string[]>([]);

const formState = ref({
  mode: 'RESTORE_MODE_SKIP' as 'RESTORE_MODE_OVERWRITE' | 'RESTORE_MODE_SKIP',
});

const moduleTargets = computed(() => {
  return detectedModules.value.map((moduleId) => {
    const registered = modules.value.find((m) => m.moduleId === moduleId);
    return {
      moduleId,
      grpcEndpoint: registered?.grpcEndpoint ?? '',
      found: !!registered,
    };
  });
});

const allModulesResolved = computed(() =>
  moduleTargets.value.length > 0 &&
  moduleTargets.value.every((t) => t.found && t.grpcEndpoint),
);

const canSubmit = computed(
  () => fileContent.value && allModulesResolved.value,
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
    const content = e.target?.result as string;
    fileContent.value = content;

    // Parse to detect modules
    try {
      const parsed = JSON.parse(content);
      if (parsed.modules && typeof parsed.modules === 'object') {
        detectedModules.value = Object.keys(parsed.modules);
      } else {
        detectedModules.value = [];
        notification.warning({
          message: $t('backup.page.full.invalidFileFormat'),
        });
      }
    } catch {
      detectedModules.value = [];
      notification.error({
        message: $t('backup.page.full.invalidFileFormat'),
      });
    }
  };
  reader.readAsText(file);
}

function resetFile() {
  fileContent.value = '';
  fileName.value = '';
  detectedModules.value = [];
}

async function handleSubmit() {
  if (!canSubmit.value) return;
  loading.value = true;

  const accessStore = useAccessStore();
  const token = accessStore.accessToken;

  try {
    const parsed = JSON.parse(fileContent.value);
    const results: { moduleId: string; success: boolean; error?: string }[] = [];

    for (const target of moduleTargets.value) {
      if (!target.found || !target.grpcEndpoint) continue;

      const moduleData = parsed.modules[target.moduleId];
      if (!moduleData) continue;

      const url = `/admin/v1/modules/${target.moduleId}/v1/backup/import`;
      const base64data = btoa(
        new Uint8Array(
          new TextEncoder().encode(JSON.stringify(moduleData)),
        ).reduce((data, byte) => data + String.fromCodePoint(byte), ''),
      );

      try {
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
          results.push({
            moduleId: target.moduleId,
            success: false,
            error: errorBody || `HTTP ${response.status}`,
          });
        } else {
          results.push({ moduleId: target.moduleId, success: true });
        }
      } catch (e: any) {
        results.push({
          moduleId: target.moduleId,
          success: false,
          error: e.message,
        });
      }
    }

    const failed = results.filter((r) => !r.success);
    if (failed.length === 0) {
      notification.success({
        message: $t('backup.page.full.uploadRestoreSuccess'),
      });
    } else if (failed.length < results.length) {
      notification.warning({
        message: $t('backup.page.full.uploadRestorePartial'),
        description: failed
          .map((r) => `${r.moduleId}: ${r.error}`)
          .join(', '),
      });
    } else {
      notification.error({
        message: $t('backup.page.full.uploadRestoreFailed'),
        description: failed
          .map((r) => `${r.moduleId}: ${r.error}`)
          .join(', '),
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
  detectedModules.value = [];
  formState.value = {
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
  <Drawer :title="$t('backup.page.full.uploadRestoreTitle')" :footer="false">
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
                {{ $t('backup.page.full.dropBackupFile') }}
              </p>
              <p class="text-sm text-gray-500">
                {{ $t('backup.page.full.supportedFormat') }}
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

      <!-- Detected Modules -->
      <div v-if="detectedModules.length" class="mb-4">
        <span class="mb-2 block font-medium">
          {{ $t('backup.page.full.detectedModules') }}
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
              {{ $t('backup.page.full.moduleNotRegistered') }}
            </span>
          </div>
        </div>
      </div>

      <!-- Form -->
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

        <Alert
          v-if="fileContent && !allModulesResolved && detectedModules.length"
          class="mb-4"
          type="warning"
          show-icon
          :message="$t('backup.page.full.unresolvedModules')"
        />

        <FormItem>
          <Space>
            <Button
              type="primary"
              html-type="submit"
              :loading="loading"
              :disabled="!canSubmit"
            >
              {{ $t('backup.page.full.restore') }}
            </Button>
          </Space>
        </FormItem>
      </Form>
    </div>
  </Drawer>
</template>
