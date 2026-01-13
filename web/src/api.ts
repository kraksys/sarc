import axios from 'axios';
import type { QueryResult, SarcObject, UploadResult, ZoneStats, ZoneMember, ZoneAuditEntry } from './types';
import { buildAuthHeaders, encodeBody, getDevicePublicKeyB64 } from './remote';

// Proxy redirects /api to http://localhost:8080
const API_BASE = '/api';

type RequestOptions = {
  data?: string | Uint8Array | Blob;
  headers?: Record<string, string>;
  responseType?: 'json' | 'blob' | 'text';
  onDownloadProgress?: (progressEvent: { loaded: number; total?: number }) => void;
};

function buildQuery(params: Record<string, string | number | boolean | undefined>): string {
  const qs = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined) qs.set(key, String(value));
  });
  const query = qs.toString();
  return query ? `?${query}` : '';
}

function buildPath(path: string, query?: string): string {
  const normalized = path.startsWith('/') ? path : `/${path}`;
  return query ? `${normalized}${query}` : normalized;
}

async function signedRequest<T>(method: string, path: string, options: RequestOptions = {}) {
  const bodyBytes =
    options.data instanceof Uint8Array
      ? options.data
      : typeof options.data === 'string'
        ? encodeBody(options.data)
        : new Uint8Array();

  const headers = await buildAuthHeaders(method, path, bodyBytes);
  const res = await axios.request<T>({
    method,
    url: `${API_BASE}${path}`,
    data: options.data,
    headers: {
      ...headers,
      ...options.headers,
    },
    responseType: options.responseType,
    onDownloadProgress: options.onDownloadProgress,
  });
  return res.data;
}

async function signedRequestWithBodyBytes<T>(
  method: string,
  path: string,
  bodyBytes: Uint8Array,
  options: RequestOptions = {},
) {
  const headers = await buildAuthHeaders(method, path, bodyBytes);
  const res = await axios.request<T>({
    method,
    url: `${API_BASE}${path}`,
    data: options.data,
    headers: {
      ...headers,
      ...options.headers,
    },
    responseType: options.responseType,
    onDownloadProgress: options.onDownloadProgress,
  });
  return res.data;
}

async function unsignedJsonPost<T>(path: string, body: Record<string, unknown>) {
  const data = JSON.stringify(body);
  const res = await axios.post<T>(`${API_BASE}${path}`, data, {
    headers: { 'Content-Type': 'application/json' },
  });
  return res.data;
}

export const api = {
  // Health Check
  getHealth: async () => {
    return signedRequest(`${'GET'}`, buildPath('/health'));
  },

  // Upload Object (PUT)
  uploadObject: async (zone: number, file: File) => {
    const query = buildQuery({
      zone: zone.toString(),
      filename: file.name,
      mime_type: file.type || 'application/octet-stream',
    });
    const path = buildPath('/objects', query);

    const buffer = await file.arrayBuffer();
    const bodyBytes = new Uint8Array(buffer);

    const data = file;
    return signedRequestWithBodyBytes<UploadResult>('PUT', path, bodyBytes, {
      data,
      headers: { 'Content-Type': 'application/octet-stream' },
    });
  },

  // List Objects (GET Query)
  listObjects: async (zone: number, limit = 100) => {
    const query = buildQuery({ limit });
    const path = buildPath(`/zones/${zone}/objects`, query);
    return signedRequest<QueryResult>('GET', path);
  },

  // Delete Object (DELETE)
  deleteObject: async (zone: number, hash: string) => {
    const path = buildPath(`/objects/${zone}/${hash}`);
    await signedRequest('DELETE', path);
  },

  // Trigger Garbage Collection (POST)
  triggerGc: async (zone: number) => {
    const path = buildPath(`/gc/${zone}`);
    return signedRequest<{ deleted_count: number }>('POST', path);
  },

  // Search Objects (FTS5 Full-Text Search)
  searchObjects: async (zone: number, query: string, limit = 100) => {
    const qs = buildQuery({ zone, q: query, limit });
    const path = buildPath('/search', qs);
    return signedRequest<QueryResult>('GET', path);
  },

  // Get Object by Filename
  getByFilename: async (zone: number, filename: string) => {
    const safe = encodeURIComponent(filename);
    const path = buildPath(`/zones/${zone}/by-filename/${safe}`);
    return signedRequest<SarcObject>('GET', path);
  },

  // Verify Hash Integrity
  verifyHash: async (zone: number, hash: string) => {
    const path = buildPath(`/objects/${zone}/${hash}/verify`);
    return signedRequest<{ valid: boolean; computed_hash: string }>('GET', path);
  },

  // Get Zone Statistics
  getZoneStats: async (zone: number) => {
    const path = buildPath(`/zones/${zone}/stats`);
    return signedRequest<ZoneStats>('GET', path);
  },

  // Batch Delete Objects
  batchDelete: async (zone: number, hashes: string[]) => {
    const path = buildPath('/batch/delete');
    const body = JSON.stringify({ zone, hashes });
    return signedRequest('POST', path, {
      data: body,
      headers: { 'Content-Type': 'application/json' },
    });
  },

  // Download Object with Progress
  downloadObject: async (zone: number, hash: string, onProgress?: (progress: number) => void) => {
    const path = buildPath(`/objects/${zone}/${hash}`);
    return signedRequest<Blob>('GET', path, {
      responseType: 'blob',
      onDownloadProgress: (progressEvent) => {
        if (onProgress && progressEvent.total) {
          const progress = Math.round((progressEvent.loaded * 100) / progressEvent.total);
          onProgress(progress);
        }
      },
    });
  },

  // Remote/Auth: initialize admin (no auth required)
  initAdmin: async (label: string) => {
    const pubkey = await getDevicePublicKeyB64();
    return unsignedJsonPost<{ user_id: number; fingerprint: string }>('/auth/init', {
      pubkey,
      label,
    });
  },

  // Remote/Auth: request pairing code (no auth required)
  requestPairCode: async (label: string) => {
    const pubkey = await getDevicePublicKeyB64();
    return unsignedJsonPost<{ code: string; expires_in: number }>('/auth/pair/request', {
      pubkey,
      label,
    });
  },

  // Remote/Auth: approve pairing code (admin signed)
  approvePairCode: async (code: string) => {
    const path = buildPath('/auth/pair/approve');
    const body = JSON.stringify({ code });
    return signedRequest<{ user_id: number; fingerprint: string }>('POST', path, {
      data: body,
      headers: { 'Content-Type': 'application/json' },
    });
  },

  // Zone sharing (admin signed)
  setZoneShared: async (zone: number, shared: boolean) => {
    const path = buildPath(`/zones/${zone}/share`);
    const body = JSON.stringify({ shared });
    return signedRequest<{ zone: number; shared: boolean }>('POST', path, {
      data: body,
      headers: { 'Content-Type': 'application/json' },
    });
  },

  // Zone members (admin signed)
  listZoneMembers: async (zone: number) => {
    const path = buildPath(`/zones/${zone}/members`);
    return signedRequest<{ zone: number; members: ZoneMember[] }>('GET', path);
  },

  // Zone audit (admin signed)
  listZoneAudit: async (zone: number, limit = 50) => {
    const query = buildQuery({ limit });
    const path = buildPath(`/zones/${zone}/audit`, query);
    return signedRequest<{ zone: number; entries: ZoneAuditEntry[] }>('GET', path);
  },
};
