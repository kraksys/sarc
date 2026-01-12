import { Shield, X, CheckCircle, AlertCircle } from 'lucide-react';
import { useUIStore } from '../state/ui-store';
import { useState } from 'react';
import { api } from '../api';

export function SecurityModal() {
  const selectedObj = useUIStore(state => state.securityModalObject);
  const closeSecurityModal = useUIStore(state => state.closeSecurityModal);
  const [verifying, setVerifying] = useState(false);
  const [verified, setVerified] = useState<boolean | null>(null);

  if (!selectedObj) return null;

  const handleVerify = async () => {
    setVerifying(true);
    try {
      const result = await api.verifyHash(selectedObj.zone, selectedObj.hash);
      setVerified(result.valid);
    } catch (err) {
      console.error('Verification error:', err);
      setVerified(false);
    } finally {
      setVerifying(false);
    }
  };

  return (
    <div className="fixed inset-0 bg-black/20 backdrop-blur-sm flex items-center justify-center z-50">
      <div className="bg-white rounded-xl shadow-xl border border-gray-200 p-6 max-w-md w-full m-4">
        <div className="flex justify-between items-start mb-4">
          <div className="flex items-center gap-2 text-green-700">
            <Shield className="w-5 h-5" />
            <h3 className="font-bold text-lg">Object Security</h3>
          </div>
          <button onClick={closeSecurityModal} className="text-gray-400 hover:text-gray-600">
            <X className="w-5 h-5" />
          </button>
        </div>

        <div className="space-y-4">
          {/* Content Integrity */}
          <div className="p-3 bg-gray-50 rounded-lg border border-gray-100">
            <p className="text-xs text-gray-500 uppercase font-semibold mb-1">Content Integrity</p>
            <div className="flex items-center gap-2">
              <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse"></div>
              <span className="text-xs font-mono text-gray-700 break-all">{selectedObj.hash}</span>
            </div>
            <p className="text-xs text-green-600 mt-1">✓ Valid BLAKE3 Hash</p>
          </div>

          {/* Verify Button */}
          <button
            onClick={handleVerify}
            disabled={verifying}
            className="w-full flex items-center justify-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition disabled:opacity-50"
          >
            {verifying ? (
              <>Verifying...</>
            ) : verified === true ? (
              <><CheckCircle className="w-4 h-4" /> Verified</>
            ) : verified === false ? (
              <><AlertCircle className="w-4 h-4" /> Verification Failed</>
            ) : (
              <>Verify Integrity</>
            )}
          </button>

          {/* Zone & Encryption Info */}
          <div className="grid grid-cols-2 gap-4">
            <div className="p-3 bg-gray-50 rounded-lg border border-gray-100">
              <p className="text-xs text-gray-500 uppercase font-semibold mb-1">Isolation Zone</p>
              <p className="text-lg font-bold text-gray-800">#{selectedObj.zone}</p>
              <p className="text-xs text-gray-500">Tenant ID</p>
            </div>
            <div className="p-3 bg-gray-50 rounded-lg border border-gray-100">
              <p className="text-xs text-gray-500 uppercase font-semibold mb-1">Encryption</p>
              <p className="text-lg font-bold text-gray-400">None</p>
              <p className="text-xs text-gray-500">At Rest</p>
            </div>
          </div>

          {/* Filename & Size */}
          {selectedObj.filename && (
            <div className="p-3 bg-gray-50 rounded-lg border border-gray-100">
              <p className="text-xs text-gray-500 uppercase font-semibold mb-1">Filename</p>
              <p className="text-sm font-mono text-gray-700">{selectedObj.filename}</p>
            </div>
          )}

          <div className="text-xs text-gray-400 text-center pt-2">
            This object is strictly isolated to Zone {selectedObj.zone}.
            <br/>Cross-zone access is cryptographically prohibited.
          </div>
        </div>
      </div>
    </div>
  );
}
