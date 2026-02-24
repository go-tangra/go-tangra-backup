import type { TangraModule } from './sdk';
import routes from './routes';
import { useBackupModuleStore } from './stores/backup-module.state';
import { useBackupFullStore } from './stores/backup-full.state';
import enUS from './locales/en-US.json';

const backupModule: TangraModule = {
  id: 'backup',
  version: '1.0.0',
  routes,
  stores: {
    'backup-module': useBackupModuleStore,
    'backup-full': useBackupFullStore,
  },
  locales: {
    'en-US': enUS,
  },
};

export default backupModule;
