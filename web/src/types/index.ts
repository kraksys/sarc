// Core Types
export interface SarcObject {
  zone: number;
  hash: string;
  size?: number;
  created_at?: number;
  filename?: string;
  mime_type?: string;
  origin_path?: string;
}

export interface ObjectMeta {
  size: number;
  refcount: number;
  created_at: number;
  updated_at: number;
}

export interface UploadResult {
  key: { zone: number; hash: string };
  meta: ObjectMeta;
  deduplicated: boolean;
}

export interface QueryResult {
  results: SarcObject[];
  count: number;
}

export interface UploadTask {
  id: string;
  file: File;
  zone: number;
  progress: number;
  status: 'pending' | 'uploading' | 'completed' | 'failed';
  error?: string;
  result?: UploadResult;
}

export interface ZoneStats {
  object_count: number;
  total_size: number;
  unique_objects: number;
}

export interface ZoneMember {
  user_id: number;
  label?: string;
  is_admin?: boolean;
  fingerprint?: string;
}

export interface ZoneAuditEntry {
  ts: number;
  user_id: number;
  zone: number;
  action: string;
  status: string;
  meta?: Record<string, unknown>;
  ip?: string;
  ua?: string;
}
