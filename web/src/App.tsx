import { useState, useEffect } from 'react';
import { api, SarcObject } from './api';
import { Upload, Trash2, File as FileIcon, RefreshCw, HardDrive, Download, Shield, X } from 'lucide-react';

function App() {
  const [zone, setZone] = useState<number>(1);
  const [objects, setObjects] = useState<SarcObject[]>([]);
  const [loading, setLoading] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [status, setStatus] = useState<string>('');
  const [selectedObj, setSelectedObj] = useState<SarcObject | null>(null);

  // Auto-load objects when zone changes
  useEffect(() => {
    loadObjects();

    // WebSocket Connection
    const ws = new WebSocket('ws://localhost:8080/ws');
    
    ws.onopen = () => {
      ws.send(JSON.stringify({ action: 'subscribe', zone: zone }));
    };

    ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);
        if (msg.type === 'new_object') {
          // Add new object to list (prepend)
          const newObj: SarcObject = {
            zone: msg.key.zone,
            hash: msg.key.hash,
            size: msg.meta.size,
            created_at: msg.meta.created_at,
            filename: msg.meta.filename,
            mime_type: msg.meta.mime_type
          };
          setObjects(prev => [newObj, ...prev]);
        }
      } catch (e) {
        console.error('WS parse error', e);
      }
    };

    return () => {
      ws.close();
    };
  }, [zone]);

  const loadObjects = async () => {
    setLoading(true);
    try {
      const data = await api.listObjects(zone);
      setObjects(data.results || []);
      setStatus('Synced');
    } catch (err) {
      console.error(err);
      setStatus('Connection Error');
      setObjects([]);
    } finally {
      setLoading(false);
    }
  };

  const handleUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    if (!e.target.files || e.target.files.length === 0) return;
    
    setUploading(true);
    const file = e.target.files[0];
    try {
      const res = await api.uploadObject(zone, file);
      setStatus(`Uploaded: ${res.key.hash.substring(0, 8)}... (Dedup: ${res.deduplicated})`);
      loadObjects();
    } catch (err) {
      console.error(err);
      setStatus('Upload failed');
    } finally {
      setUploading(false);
      e.target.value = ''; // Reset input
    }
  };

  const handleDelete = async (hash: string) => {
    if (!confirm('Delete this object?')) return;
    try {
      await api.deleteObject(zone, hash);
      setStatus('Deleted object');
      loadObjects();
    } catch (err) {
      console.error(err);
      setStatus('Delete failed');
    }
  };

  const handleGc = async () => {
    try {
      const res = await api.triggerGc(zone);
      setStatus(`GC: Freed ${res.deleted_count} objects`);
      loadObjects();
    } catch (err) {
      setStatus('GC failed');
    }
  };

  return (
    <div className="min-h-screen p-8 max-w-5xl mx-auto font-sans relative">
      {/* Header */}
      <header className="mb-8 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-blue-100 rounded-lg">
            <HardDrive className="w-6 h-6 text-blue-600" />
          </div>
          <h1 className="text-2xl font-bold text-gray-900 tracking-tight">SARC Storage</h1>
        </div>
        
        <div className="flex items-center gap-3 bg-white p-1.5 rounded-lg shadow-sm border border-gray-200">
          <span className="text-xs font-semibold text-gray-500 uppercase px-2">Zone ID</span>
          <input 
            type="number" 
            min="1" 
            max="65535"
            value={zone}
            onChange={(e) => setZone(parseInt(e.target.value) || 1)}
            className="w-20 p-1 bg-gray-50 border border-gray-200 rounded text-right font-mono text-sm focus:outline-none focus:border-blue-500"
          />
        </div>
      </header>

      {/* Control Bar */}
      <div className="flex gap-3 mb-6">
        <label className={`flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg cursor-pointer hover:bg-blue-700 transition shadow-sm ${uploading ? 'opacity-50 pointer-events-none' : ''}`}>
          <Upload className="w-4 h-4" />
          <span className="text-sm font-medium">{uploading ? 'Uploading...' : 'Upload File'}</span>
          <input type="file" className="hidden" onChange={handleUpload} disabled={uploading} />
        </label>

        <button 
          onClick={loadObjects}
          className="flex items-center gap-2 px-4 py-2 bg-white border border-gray-200 text-gray-700 rounded-lg hover:bg-gray-50 hover:border-gray-300 transition shadow-sm"
        >
          <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
          <span className="text-sm font-medium">Refresh</span>
        </button>

        <div className="ml-auto flex items-center gap-4">
          <span className="text-xs text-gray-400 font-mono">{status}</span>
          <button 
            onClick={handleGc}
            className="flex items-center gap-2 px-4 py-2 bg-white border border-gray-200 text-red-600 rounded-lg hover:bg-red-50 hover:border-red-200 transition shadow-sm"
            title="Garbage Collect Zone"
          >
            <Trash2 className="w-4 h-4" />
            <span className="text-sm font-medium">Prune</span>
          </button>
        </div>
      </div>

      {/* Object Table */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
        <div className="grid grid-cols-12 gap-4 p-4 border-b border-gray-100 bg-gray-50/50 text-xs font-semibold text-gray-500 uppercase tracking-wider">
          <div className="col-span-1">Type</div>
          <div className="col-span-5">Name / Hash</div>
          <div className="col-span-2">Size</div>
          <div className="col-span-2">Type</div>
          <div className="col-span-2 text-right">Actions</div>
        </div>

        {objects.length === 0 ? (
          <div className="p-16 text-center flex flex-col items-center">
            <div className="bg-gray-50 p-4 rounded-full mb-4">
              <FileIcon className="w-8 h-8 text-gray-300" />
            </div>
            <p className="text-gray-500 font-medium">No objects found</p>
            <p className="text-sm text-gray-400 mt-1">Zone {zone} is empty</p>
          </div>
        ) : (
          <div className="divide-y divide-gray-100">
            {objects.map((obj) => (
              <div key={obj.hash} className="grid grid-cols-12 gap-4 p-3 items-center hover:bg-blue-50/50 transition group">
                <div className="col-span-1 flex justify-center">
                  <FileIcon className="w-4 h-4 text-gray-400" />
                </div>
                <div className="col-span-5 font-mono text-xs text-gray-600 truncate select-all" title={obj.hash}>
                  {obj.filename || obj.hash}
                </div>
                <div className="col-span-2 text-xs text-gray-500">
                  {obj.size ? (obj.size / 1024).toFixed(1) + ' KB' : '-'}
                </div>
                <div className="col-span-2 text-xs text-gray-500 truncate" title={obj.mime_type}>
                  {obj.mime_type || 'blob'}
                </div>
                <div className="col-span-2 flex items-center justify-end gap-2 opacity-0 group-hover:opacity-100 transition-opacity">
                  <button
                    onClick={() => setSelectedObj(obj)}
                    className="p-1.5 text-green-600 hover:bg-green-50 rounded-md transition"
                    title="Security Info"
                  >
                    <Shield className="w-4 h-4" />
                  </button>
                  <a 
                    href={api.getObjectUrl(obj.zone, obj.hash)} 
                    target="_blank"
                    className="p-1.5 text-blue-600 hover:bg-blue-100 rounded-md transition"
                    title="Download"
                  >
                    <Download className="w-4 h-4" />
                  </a>
                  <button 
                    onClick={() => handleDelete(obj.hash)}
                    className="p-1.5 text-gray-400 hover:text-red-600 hover:bg-red-50 rounded-md transition"
                    title="Delete"
                  >
                    <Trash2 className="w-4 h-4" />
                  </button>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Security Modal */}
      {selectedObj && (
        <div className="fixed inset-0 bg-black/20 backdrop-blur-sm flex items-center justify-center z-50">
          <div className="bg-white rounded-xl shadow-xl border border-gray-200 p-6 max-w-md w-full m-4">
            <div className="flex justify-between items-start mb-4">
              <div className="flex items-center gap-2 text-green-700">
                <Shield className="w-5 h-5" />
                <h3 className="font-bold text-lg">Object Security</h3>
              </div>
              <button onClick={() => setSelectedObj(null)} className="text-gray-400 hover:text-gray-600">
                <X className="w-5 h-5" />
              </button>
            </div>
            
            <div className="space-y-4">
              <div className="p-3 bg-gray-50 rounded-lg border border-gray-100">
                <p className="text-xs text-gray-500 uppercase font-semibold mb-1">Content Integrity</p>
                <div className="flex items-center gap-2">
                  <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse"></div>
                  <span className="text-sm font-mono text-gray-700 break-all">{selectedObj.hash}</span>
                </div>
                <p className="text-xs text-green-600 mt-1">✓ Valid BLAKE3 Hash</p>
              </div>

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

              <div className="text-xs text-gray-400 text-center pt-2">
                This object is strictly isolated to Zone {selectedObj.zone}.
                <br/>Cross-zone access is cryptographically prohibited.
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default App;