import axios from 'axios';

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
  }
};
