import { useEffect, useState } from 'react';
import { X } from 'lucide-react';
import axios from 'axios';
import { api } from '../api';
import { getDeviceInfo, setStoredLabel } from '../remote';
import type { ZoneMember, ZoneAuditEntry } from '../types';

interface RemoteModalProps {
  open: boolean;
  onClose: () => void;
  zone: number;
}

function formatTs(ts: number) {
  if (!ts) return '';
  const date = new Date(ts);
  return isNaN(date.getTime()) ? String(ts) : date.toLocaleString();
}

function errorMessage(err: unknown) {
  if (axios.isAxiosError(err)) {
    const data = err.response?.data as { error?: string } | undefined;
    return data?.error || err.message;
  }
  if (err instanceof Error) return err.message;
  return 'Unknown error';
}

export function RemoteModal({ open, onClose, zone }: RemoteModalProps) {
  const [label, setLabel] = useState('');
  const [fingerprint, setFingerprint] = useState('');
  const [pairInfo, setPairInfo] = useState<{ code: string; expires_in: number } | null>(null);
  const [approveCode, setApproveCode] = useState('');
  const [status, setStatus] = useState('');
  const [members, setMembers] = useState<ZoneMember[] | null>(null);
  const [audit, setAudit] = useState<ZoneAuditEntry[] | null>(null);

  useEffect(() => {
    if (!open) return;
    const load = async () => {
      const info = await getDeviceInfo();
      setLabel(info.label);
      setFingerprint(info.fingerprint);
    };
    load();
  }, [open]);

  if (!open) return null;

  const handleInitAdmin = async () => {
    try {
      setStatus('Initializing admin...');
      const res = await api.initAdmin(label);
      setStatus(`Admin initialized: user_id=${res.user_id} fingerprint=${res.fingerprint}`);
    } catch (err) {
      setStatus(`Init failed: ${errorMessage(err)}`);
    }
  };

  const handlePairRequest = async () => {
    try {
      setStatus('Requesting pair code...');
      const res = await api.requestPairCode(label);
      setPairInfo(res);
      setStatus(`Pair code issued (expires in ${res.expires_in}s)`);
    } catch (err) {
      setStatus(`Pair request failed: ${errorMessage(err)}`);
    }
  };

  const handleApprove = async () => {
    if (!approveCode) return;
    try {
      setStatus('Approving pair code...');
      const res = await api.approvePairCode(approveCode);
      setStatus(`Approved: user_id=${res.user_id} fingerprint=${res.fingerprint}`);
      setApproveCode('');
    } catch (err) {
      setStatus(`Approve failed: ${errorMessage(err)}`);
    }
  };

  const handleShare = async (shared: boolean) => {
    try {
      setStatus(shared ? 'Sharing zone...' : 'Unsharing zone...');
      const res = await api.setZoneShared(zone, shared);
      setStatus(`Zone ${res.zone} shared=${res.shared}`);
    } catch (err) {
      setStatus(`Share failed: ${errorMessage(err)}`);
    }
  };

  const handleLoadMembers = async () => {
    try {
      setStatus('Loading members...');
      const res = await api.listZoneMembers(zone);
      setMembers(res.members || []);
      setStatus(`Members loaded (${res.members?.length ?? 0})`);
    } catch (err) {
      setStatus(`Members failed: ${errorMessage(err)}`);
    }
  };

  const handleLoadAudit = async () => {
    try {
      setStatus('Loading audit...');
      const res = await api.listZoneAudit(zone, 50);
      setAudit(res.entries || []);
      setStatus(`Audit loaded (${res.entries?.length ?? 0})`);
    } catch (err) {
      setStatus(`Audit failed: ${errorMessage(err)}`);
    }
  };

  const handleLabelChange = (value: string) => {
    setLabel(value);
    setStoredLabel(value);
  };

  return (
    <div className="fixed inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center z-50 p-4">
      <div className="border-4 border-white bg-black text-white w-full max-w-3xl p-6 font-mono">
        <div className="flex items-center justify-between border-b-2 border-white pb-2 mb-4">
          <h3 className="text-lg tracking-widest">REMOTE ACCESS</h3>
          <button onClick={onClose} className="px-2 border border-white vintage-outline">
            <X className="w-4 h-4" />
          </button>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
          <div className="border-2 border-white p-3">
            <div className="text-xs uppercase mb-2">Device</div>
            <label className="block text-xs mb-2">
              Label
              <input
                value={label}
                onChange={(e) => handleLabelChange(e.target.value)}
                className="mt-1 w-full bg-black border border-white p-1"
                placeholder="e.g. admin-laptop"
              />
            </label>
            <div className="text-xs">
              Fingerprint
              <div className="mt-1 text-[10px] break-all border border-white p-1">
                {fingerprint || '...' }
              </div>
            </div>
          </div>

          <div className="border-2 border-white p-3">
            <div className="text-xs uppercase mb-2">Pairing</div>
            <div className="flex gap-2 mb-2">
              <button
                onClick={handleInitAdmin}
                className="px-2 py-1 border border-white vintage-outline text-xs"
              >
                Init Admin
              </button>
              <button
                onClick={handlePairRequest}
                className="px-2 py-1 border border-white vintage-outline text-xs"
              >
                Request Code
              </button>
            </div>
            {pairInfo && (
              <div className="text-xs border border-white p-2 mb-2">
                Code: <span className="font-bold">{pairInfo.code}</span>
                <span className="ml-2 text-gray-400">({pairInfo.expires_in}s)</span>
              </div>
            )}
            <div className="flex gap-2">
              <input
                value={approveCode}
                onChange={(e) => setApproveCode(e.target.value)}
                className="flex-1 bg-black border border-white p-1 text-xs"
                placeholder="Approve code"
              />
              <button
                onClick={handleApprove}
                className="px-2 py-1 border border-white vintage-outline text-xs"
              >
                Approve
              </button>
            </div>
          </div>

          <div className="border-2 border-white p-3">
            <div className="text-xs uppercase mb-2">Zone Sharing (Zone {zone})</div>
            <div className="flex gap-2">
              <button
                onClick={() => handleShare(true)}
                className="px-2 py-1 border border-white vintage-outline text-xs"
              >
                Share
              </button>
              <button
                onClick={() => handleShare(false)}
                className="px-2 py-1 border border-white vintage-outline text-xs"
              >
                Unshare
              </button>
            </div>
          </div>

          <div className="border-2 border-white p-3">
            <div className="text-xs uppercase mb-2">Audit & Members</div>
            <div className="flex gap-2">
              <button
                onClick={handleLoadMembers}
                className="px-2 py-1 border border-white vintage-outline text-xs"
              >
                Members
              </button>
              <button
                onClick={handleLoadAudit}
                className="px-2 py-1 border border-white vintage-outline text-xs"
              >
                Audit
              </button>
            </div>
          </div>
        </div>

        {status && (
          <div className="mt-4 border-2 border-white p-2 text-xs">{status}</div>
        )}

        {members && (
          <div className="mt-4 border-2 border-white p-2 text-xs">
            <div className="uppercase mb-2">Members</div>
            <div className="space-y-1">
              {members.length === 0 && <div className="text-gray-400">No members</div>}
              {members.map((m) => (
                <div key={`${m.user_id}-${m.fingerprint}`} className="flex justify-between gap-2">
                  <span>#{m.user_id} {m.is_admin ? '(admin)' : ''} {m.label || ''}</span>
                  <span className="text-gray-400">{m.fingerprint}</span>
                </div>
              ))}
            </div>
          </div>
        )}

        {audit && (
          <div className="mt-4 border-2 border-white p-2 text-xs max-h-56 overflow-auto">
            <div className="uppercase mb-2">Audit (latest)</div>
            <div className="space-y-1">
              {audit.length === 0 && <div className="text-gray-400">No entries</div>}
              {audit.map((entry, idx) => (
                <div key={`${entry.ts}-${idx}`} className="flex justify-between gap-2">
                  <span>{formatTs(entry.ts)} #{entry.user_id}</span>
                  <span>{entry.action}</span>
                  <span className="text-gray-400">{entry.status}</span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
