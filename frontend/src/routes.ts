import type { RouteRecordRaw } from 'vue-router';

const routes: RouteRecordRaw[] = [
  {
    path: '/backup',
    name: 'Backup',
    component: () => import('shell/app-layout'),
    redirect: '/backup/modules',
    meta: {
      order: 2090,
      icon: 'lucide:archive',
      title: 'backup.menu.backup',
      keepAlive: true,
      authority: ['platform:admin'],
    },
    children: [
      {
        path: 'modules',
        name: 'BackupModules',
        meta: {
          icon: 'lucide:database',
          title: 'backup.menu.modules',
          authority: ['platform:admin'],
        },
        component: () => import('./views/modules/index.vue'),
      },
      {
        path: 'full',
        name: 'BackupFull',
        meta: {
          icon: 'lucide:hard-drive',
          title: 'backup.menu.full',
          authority: ['platform:admin'],
        },
        component: () => import('./views/full/index.vue'),
      },
    ],
  },
];

export default routes;
