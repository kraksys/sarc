import { useEffect, useState } from 'react';
import { HardDrive, RefreshCw, Trash2, Radio } from 'lucide-react';
import { useAppStore } from './state/store';
import { useUIStore } from './state/ui-store';
import { useWebSocket } from './hooks/useWebSocket';
import { ObjectList } from './components/ObjectList';
import { BatchActions } from './components/BatchActions';
import { UploadZone } from './components/UploadZone';
import { SecurityModal } from './components/SecurityModal';
import { PreviewModal } from './components/PreviewModal';
import { CommandPalette } from './components/CommandPalette';
import { VimCommandLine } from './components/VimCommandLine';
import { RemoteModal } from './components/RemoteModal';
import { api } from './api';

type VimMode = 'normal' | 'search' | 'find';

function App() {
  // Store hooks
  const currentZone = useAppStore(state => state.currentZone);
  const setCurrentZone = useAppStore(state => state.setCurrentZone);
  const loadObjects = useAppStore(state => state.loadObjects);
  const refreshObjects = useAppStore(state => state.refreshObjects);
  const loading = useAppStore(state => state.loadingObjects);
  const status = useAppStore(state => state.status);
  const selectAll = useAppStore(state => state.selectAll);
  const clearSelection = useAppStore(state => state.clearSelection);
  const selectedObjects = useAppStore(state => state.selectedObjects);
  const setStatus = useAppStore(state => state.setStatus);
  const setSearchQuery = useAppStore(state => state.setSearchQuery);
  const searchQuery = useAppStore(state => state.searchQuery);
  const objects = useAppStore(state => state.objects);
  const searchResults = useAppStore(state => state.searchResults);
  const openUploadModal = useUIStore(state => state.openUploadModal);
  const uploadModalOpen = useUIStore(state => state.uploadModalOpen);
  const closeUploadModal = useUIStore(state => state.closeUploadModal);
  const toggleCommandPalette = useUIStore(state => state.toggleCommandPalette);

  // Vim mode state
  const [vimMode, setVimMode] = useState<VimMode>('normal');
  const [remoteOpen, setRemoteOpen] = useState(false);

  // WebSocket connection
  useWebSocket();

  // Load objects on mount
  useEffect(() => {
    loadObjects();
  }, [loadObjects]);

  // Vim keyboard shortcuts
  useEffect(() => {
    const handleKeyDown = async (e: KeyboardEvent) => {
      // If in a modal or input, don't handle vim keys
      const target = e.target as HTMLElement;
      if (target.tagName === 'INPUT' || target.tagName === 'TEXTAREA') {
        // Only handle Escape in inputs
        if (e.key === 'Escape') {
          setVimMode('normal');
          setSearchQuery('');
          target.blur();
        }
        return;
      }

      // Normal mode keys
      if (vimMode === 'normal') {
        switch (e.key) {
          case '/':
            e.preventDefault();
            setVimMode('search');
            break;
          case 'f':
            e.preventDefault();
            setVimMode('find');
            break;
          case 'u':
            e.preventDefault();
            openUploadModal();
            break;
          case 'r':
            e.preventDefault();
            await refreshObjects();
            break;
          case 'g':
            if (e.shiftKey) {
              e.preventDefault();
              handleGc();
            }
            break;
          case 'a':
            if (e.shiftKey) {
              e.preventDefault();
              selectAll();
            }
            break;
          case 'd':
            if (e.shiftKey && selectedObjects.size > 0) {
              e.preventDefault();
              handleBulkDelete();
            }
            break;
          case 'k':
            if (e.metaKey || e.ctrlKey) {
              e.preventDefault();
              toggleCommandPalette();
            }
            break;
          case 'Escape':
            clearSelection();
            setSearchQuery('');
            break;
        }
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [vimMode, selectAll, clearSelection, selectedObjects, toggleCommandPalette, openUploadModal, refreshObjects]);

  const handleGc = async () => {
    try {
      const res = await api.triggerGc(currentZone);
      setStatus(`GC: Freed ${res.deleted_count} objects`);
      await refreshObjects();
    } catch (err) {
      setStatus('GC failed');
    }
  };

  const handleBulkDelete = async () => {
    if (selectedObjects.size === 0) return;
    if (!confirm(`Delete ${selectedObjects.size} object(s)?`)) return;

    const deleteObjects = useAppStore.getState().deleteObjects;
    await deleteObjects(Array.from(selectedObjects));
  };

  const handleVimCommand = async (command: string) => {
    const scrollToObject = (hash: string) => {
      requestAnimationFrame(() => {
        document
          .querySelector<HTMLElement>(`[data-object-hash="${hash}"]`)
          ?.scrollIntoView({ block: 'center' });
      });
    };

    if (vimMode === 'search') {
      // FTS5 Search
      await setSearchQuery(command);
    } else if (vimMode === 'find') {
      // Find by RID or first letter
      const rid = parseInt(command);
      if (!isNaN(rid) && rid > 0 && rid <= objects.length) {
        // Find by RID (1-indexed)
        const obj = objects[rid - 1];
        if (obj) {
          const selectObject = useAppStore.getState().selectObject;
          selectObject(obj.hash);
          setStatus(`Selected: ${obj.filename || obj.hash.substring(0, 12)}`);
          scrollToObject(obj.hash);
        }
      } else if (command.length === 1) {
        // Find by first letter
        const matches = objects.filter(obj =>
          obj.filename?.toLowerCase().startsWith(command.toLowerCase())
        );
        if (matches.length > 0) {
          const selectObject = useAppStore.getState().selectObject;
          selectObject(matches[0].hash);
          setStatus(`Found ${matches.length} match(es) starting with '${command}'`);
          scrollToObject(matches[0].hash);
        } else {
          setStatus(`No files starting with '${command}'`);
        }
      }
    }
    setVimMode('normal');
  };

  const handleVimEscape = () => {
    setVimMode('normal');
    if (vimMode === 'search') {
      setSearchQuery('');
    }
  };

  // Display search results if searching
  const displayedObjects = searchQuery ? searchResults : objects;

  return (
    <div className="min-h-screen p-8 max-w-7xl mx-auto font-mono pb-24">
      {/* Vim Mode Indicator */}
      <div className="mode-indicator">
        -- {vimMode.toUpperCase()} --
      </div>

      {/* Header */}
      <header className="mb-8 border-2 border-white p-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <div className="border-2 border-white p-2">
              <HardDrive className="w-6 h-6" />
            </div>
            <div>
              <h1 className="text-xl tracking-widest">SARC STORAGE SYSTEM</h1>
              <p className="text-sm mt-1">CONTENT-ADDRESSABLE OBJECT STORE v1.0</p>
            </div>
          </div>

          <div className="flex items-center gap-2">
            <button
              onClick={() => setRemoteOpen(true)}
              className="flex items-center gap-2 border-2 border-white px-3 py-2 hover:bg-gray-800"
              title="Remote access"
            >
              <Radio className="w-4 h-4" />
              REMOTE
            </button>
            <div className="flex items-center gap-2 border-2 border-white p-2">
              <span className="text-sm">ZONE:</span>
              <input
                type="number"
                min="1"
                max="65535"
                value={currentZone}
                onChange={(e) => setCurrentZone(parseInt(e.target.value) || 1)}
                className="w-16 p-1 text-right"
              />
            </div>
          </div>
        </div>
      </header>

      {/* Control Bar */}
      <div className="flex gap-2 mb-6 border-2 border-white p-2">
        <button
          onClick={openUploadModal}
          className="px-4 py-2 border-2 border-white vintage-invert"
          title="Press 'u' to upload"
        >
          [U]PLOAD
        </button>

        <button
          onClick={refreshObjects}
          className="px-4 py-2 border-2 border-white hover:bg-gray-800"
          title="Press 'r' to refresh"
        >
          <RefreshCw className={`w-4 h-4 inline ${loading ? 'animate-spin' : ''}`} />
          {' '}[R]EFRESH
        </button>

        <button
          onClick={handleGc}
          className="px-4 py-2 border-2 border-white hover:bg-gray-800"
          title="Press 'G' to garbage collect"
        >
          <Trash2 className="w-4 h-4 inline" />
          {' '}[G]C
        </button>

        <div className="ml-auto flex items-center">
          <span className="text-sm px-4">
            {displayedObjects.length} OBJECT(S) | {selectedObjects.size} SELECTED
          </span>
        </div>
      </div>

      {/* Status Bar */}
      {status && (
        <div className="mb-4 p-2 border-2 border-white">
          &gt; {status}
        </div>
      )}

      {/* Help Text */}
      <div className="mb-4 p-2 border-2 border-white text-sm">
        <div className="grid grid-cols-4 gap-2">
          <span>[/] SEARCH</span>
          <span>[F] FIND</span>
          <span>[U] UPLOAD</span>
          <span>[R] REFRESH</span>
          <span>[G] GC</span>
          <span>[A] SELECT ALL</span>
          <span>[D] DELETE SEL</span>
          <span>[ESC] CLEAR</span>
        </div>
      </div>

      {/* Object Table */}
      <div className="border-2 border-white">
        <div className="grid grid-cols-12 gap-4 p-3 border-b-2 border-white bg-black uppercase tracking-wider text-sm">
          <div className="col-span-1">[#]</div>
          <div className="col-span-4">NAME / HASH</div>
          <div className="col-span-2">SIZE</div>
          <div className="col-span-2">TYPE</div>
          <div className="col-span-3 text-right">ACTIONS</div>
        </div>

        <ObjectList />
      </div>

      {/* Upload Modal */}
      {uploadModalOpen && (
        <div className="fixed inset-0 bg-black bg-opacity-90 flex items-center justify-center z-50">
          <div className="border-4 border-white p-6 max-w-2xl w-full m-4 bg-black">
            <div className="flex justify-between items-center mb-4 pb-2 border-b-2 border-white">
              <h3 className="text-lg tracking-widest">UPLOAD FILES</h3>
              <button onClick={closeUploadModal} className="text-2xl px-2 border border-white vintage-outline">
                [X]
              </button>
            </div>
            <UploadZone />
          </div>
        </div>
      )}

      {/* Batch Actions Bar */}
      <BatchActions />

      {/* Security Modal */}
      <SecurityModal />

      {/* Preview Modal */}
      <PreviewModal />

      {/* Command Palette */}
      <CommandPalette />

      {/* Vim Command Line */}
      <VimCommandLine
        mode={vimMode}
        onEscape={handleVimEscape}
        onExecute={handleVimCommand}
      />

      {/* Remote Modal */}
      <RemoteModal open={remoteOpen} onClose={() => setRemoteOpen(false)} zone={currentZone} />

      {/* Status Bar at bottom */}
      {vimMode === 'normal' && (
        <div className="status-bar">
          SARC v1.0 | ZONE {currentZone} | MODE: {vimMode.toUpperCase()} | OBJECTS: {displayedObjects.length}
        </div>
      )}
    </div>
  );
}

export default App;
