import { create } from "zustand";
import { SarcObject, UploadTask } from "../types";
import { api } from "../api";

interface AppStore {
  // Zone state
  currentZone: number;
  setCurrentZone: (zone: number) => void;

  // Objects state
  objects: SarcObject[];
  selectedObjects: Set<string>;
  loadingObjects: boolean;

  // Search state
  searchQuery: string;
  searchResults: SarcObject[];
  searching: boolean;

  // Upload state
  uploads: UploadTask[];

  // Status
  status: string;

  // Actions
  loadObjects: () => Promise<void>;
  refreshObjects: () => Promise<void>;
  deleteObjects: (hashes: string[]) => Promise<void>;
  selectObject: (hash: string) => void;
  toggleSelectObject: (hash: string) => void;
  selectAll: () => void;
  clearSelection: () => void;
  setSearchQuery: (query: string) => Promise<void>;
  addUpload: (file: File) => void;
  setStatus: (status: string) => void;
}

export const useAppStore = create<AppStore>((set, get) => ({
  // initial state
  currentZone: 1,
  objects: [],
  selectedObjects: new Set(),
  loadingObjects: false,
  searchQuery: "",
  searchResults: [],
  searching: false,
  uploads: [],
  status: "",

  // zone management
  setCurrentZone: (zone) => {
    set({ currentZone: zone, selectedObjects: new Set() });
    get().loadObjects();
  },

  // load objects for current zone
  loadObjects: async () => {
    set({ loadingObjects: true });
    try {
      const data = await api.listObjects(get().currentZone);
      set({ objects: data.results || [], status: "Synced" });
    } catch (err) {
      console.error("Load objects error:", err);
      set({ objects: [], status: "Connection Error" });
    } finally {
      set({ loadingObjects: false });
    }
  },

  // Refresh current zone
  refreshObjects: async () => {
    await get().loadObjects();
  },

  // Delete multiple objs
  deleteObjects: async (hashes) => {
    const zone = get().currentZone;
    try {
      await Promise.all(hashes.map((hash) => api.deleteObject(zone, hash)));
      set({ status: `Deleted ${hashes.length} object(s)` });
      get().clearSelection();
      await get().loadObjects();
    } catch (err) {
      console.error("Delete error:", err);
      set({ status: "Delete failed" });
    }
  },

  // Selection management
  selectObject: (hash) => {
    set({ selectedObjects: new Set([hash]) });
  },

  toggleSelectObject: (hash) => {
    const selected = new Set(get().selectedObjects);
    if (selected.has(hash)) {
      selected.delete(hash);
    } else {
      selected.add(hash);
    }
    set({ selectedObjects: selected });
  },

  selectAll: () => {
    const allHashes = new Set(get().objects.map((obj) => obj.hash));
    set({ selectedObjects: allHashes });
  },

  clearSelection: () => {
    set({ selectedObjects: new Set() });
  },

  // Search (FTS5 backend search)
  setSearchQuery: async (query) => {
    set({ searchQuery: query });
    if (!query) {
      set({ searchResults: [], searching: false });
      return;
    }

    set({ searching: true, searchResults: [] });
    try {
      // Try FTS5 backend search first
      const data = await api.searchObjects(get().currentZone, query);
      set({ searchResults: data.results || [] });
    } catch (err) {
      console.log('FTS5 search not available, falling back to client-side filter');
      // Fallback to client-side filter if backend doesn't support FTS5
      const results = get().objects.filter(
        (obj) =>
          obj.filename?.toLowerCase().includes(query.toLowerCase()) ||
          obj.hash.includes(query) ||
          obj.mime_type?.toLowerCase().includes(query.toLowerCase()),
      );
      set({ searchResults: results });
    } finally {
      set({ searching: false });
    }
  },

  // Upload management
  addUpload: (file) => {
    const task: UploadTask = {
      id: Math.random().toString(36).substring(7),
      file,
      zone: get().currentZone,
      progress: 0,
      status: "pending",
    };
    set({ uploads: [...get().uploads, task] });
  },

  // Status msgs
  setStatus: (status) => set({ status }),
}));
