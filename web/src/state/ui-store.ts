import { create } from 'zustand';
import { SarcObject } from '../types';

interface UIStore {
  // Modals
  commandPaletteOpen: boolean;
  uploadModalOpen: boolean;
  securityModalObject: SarcObject | null;
  previewObject: SarcObject | null;

  // UI state
  viewMode: 'list' | 'grid';
  sidebarCollapsed: boolean;

  // Actions
  openCommandPalette: () => void;
  closeCommandPalette: () => void;
  toggleCommandPalette: () => void;
  openUploadModal: () => void;
  closeUploadModal: () => void;
  showSecurityModal: (obj: SarcObject) => void;
  closeSecurityModal: () => void;
  showPreview: (obj: SarcObject) => void;
  closePreview: () => void;
  setViewMode: (mode: 'list' | 'grid') => void;
  toggleSidebar: () => void;
}

export const useUIStore = create<UIStore>((set) => ({
  // Initial state
  commandPaletteOpen: false,
  uploadModalOpen: false,
  securityModalObject: null,
  previewObject: null,
  viewMode: 'list',
  sidebarCollapsed: false,

  // Modal actions
  openCommandPalette: () => set({ commandPaletteOpen: true }),
  closeCommandPalette: () => set({ commandPaletteOpen: false }),
  toggleCommandPalette: () => set((state) => ({ commandPaletteOpen: !state.commandPaletteOpen })),

  openUploadModal: () => set({ uploadModalOpen: true }),
  closeUploadModal: () => set({ uploadModalOpen: false }),

  showSecurityModal: (obj) => set({ securityModalObject: obj }),
  closeSecurityModal: () => set({ securityModalObject: null }),

  showPreview: (obj) => set({ previewObject: obj }),
  closePreview: () => set({ previewObject: null }),

  // UI state
  setViewMode: (mode) => set({ viewMode: mode }),
  toggleSidebar: () => set((state) => ({ sidebarCollapsed: !state.sidebarCollapsed })),
}));
