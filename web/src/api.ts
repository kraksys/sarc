import axios from 'axios';
import type { SarcObject, QueryResult } from './types';

// Proxy redirects /api to http://localhost:8080
const API_BASE = '/api';

export interface SarcObject {
  zone: number;
  hash: string;
  size?: number;
  created_at?: number;
  filename?: string;
  mime_type?: string;
}

export interface ObjectMeta {
  size: number;
  refcount: number;
  created_at: number;
  updated_at: number;
}

export interface UploadResult {
  key: SarcObject;
  meta: ObjectMeta;
  deduplicated: boolean;
}

export interface QueryResult {
  results: SarcObject[];
  count: number;
}

export const api = {
  // Health Check
  getHealth: async () => {
    const res = await axios.get(`${API_BASE}/health`);
    return res.data;
  },

  // Upload Object (PUT)
  uploadObject: async (zone: number, file: File) => {
    const url = `${API_BASE}/objects`;
    const params = new URLSearchParams({
      zone: zone.toString(),
      filename: file.name,
      mime_type: file.type || 'application/octet-stream',
    });

    // SARC expects the raw binary body
    const res = await axios.put(`${url}?${params.toString()}`, file, {
      headers: {
        'Content-Type': 'application/octet-stream',
      },
    });
    return res.data as UploadResult;
  },

  // List Objects (GET Query)
  listObjects: async (zone: number, limit = 100) => {
    const res = await axios.get(`${API_BASE}/zones/${zone}/objects`, {
      params: { limit },
    });
    return res.data as QueryResult;
  },

  // Construct Download URL
  getObjectUrl: (zone: number, hash: string) => {
    return `${API_BASE}/objects/${zone}/${hash}`;
  },

  // Delete Object (DELETE)
  deleteObject: async (zone: number, hash: string) => {
    await axios.delete(`${API_BASE}/objects/${zone}/${hash}`);
  },

  // Trigger Garbage Collection (POST)
  triggerGc: async (zone: number) => {
    const res = await axios.post(`${API_BASE}/gc/${zone}`);
    return res.data;
  },

  // Search Objects (FTS5 Full-Text Search)
  searchObjects: async (zone: number, query: string, limit = 100) => {
    const res = await axios.get(`${API_BASE}/search`, {
      params: { zone, q: query, limit },
    });
    return res.data as QueryResult;
  },

  // Get Object by Filename
  getByFilename: async (zone: number, filename: string) => {
    const res = await axios.get(`${API_BASE}/zones/${zone}/by-filename/${encodeURIComponent(filename)}`);
    return res.data as SarcObject;
  },

  // Verify Hash Integrity
  verifyHash: async (zone: number, hash: string) => {
    const res = await axios.get(`${API_BASE}/objects/${zone}/${hash}/verify`);
    return res.data as { valid: boolean; computed_hash: string };
  },

  // Get Zone Statistics
  getZoneStats: async (zone: number) => {
    const res = await axios.get(`${API_BASE}/zones/${zone}/stats`);
    return res.data as { object_count: number; total_size: number; unique_objects: number };
  },

  // Batch Delete Objects
  batchDelete: async (zone: number, hashes: string[]) => {
    const res = await axios.post(`${API_BASE}/batch/delete`, { zone, hashes });
    return res.data;
  },

  // Download Object with Progress
  downloadObject: async (zone: number, hash: string, onProgress?: (progress: number) => void) => {
    const res = await axios.get(`${API_BASE}/objects/${zone}/${hash}`, {
      responseType: 'blob',
      onDownloadProgress: (progressEvent) => {
        if (onProgress && progressEvent.total) {
          const progress = Math.round((progressEvent.loaded * 100) / progressEvent.total);
          onProgress(progress);
        }
      },
    });
    return res.data;
  },
};
