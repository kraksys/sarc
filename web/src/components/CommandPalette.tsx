import { Command } from 'cmdk';
import { useUIStore } from '../state/ui-store';
import { useAppStore } from '../state/store';
import { Upload, Search, Trash2, RefreshCw, Download } from 'lucide-react';
import { api } from '../api';

export function CommandPalette() {
  const open = useUIStore(state => state.commandPaletteOpen);
  const closeCommandPalette = useUIStore(state => state.closeCommandPalette);
  const objects = useAppStore(state => state.objects);
  const currentZone = useAppStore(state => state.currentZone);
  const openUploadModal = useUIStore(state => state.openUploadModal);
  const refreshObjects = useAppStore(state => state.refreshObjects);
  const setStatus = useAppStore(state => state.setStatus);

  if (!open) return null;

  const handleAction = async (action: string, hash?: string) => {
    closeCommandPalette();

    switch (action) {
      case 'upload':
        openUploadModal();
        break;
      case 'refresh':
        await refreshObjects();
        break;
      case 'gc':
        const res = await api.triggerGc(currentZone);
        setStatus(`GC: Freed ${res.deleted_count} objects`);
        await refreshObjects();
        break;
      case 'download':
        if (hash) {
          const obj = objects.find(o => o.hash === hash);
          if (obj) {
            window.open(api.getObjectUrl(currentZone, hash), '_blank');
          }
        }
        break;
    }
  };

  return (
    <div className="fixed inset-0 bg-black/50 backdrop-blur-sm flex items-start justify-center pt-32 z-50">
      <Command className="bg-white rounded-xl shadow-2xl border border-gray-200 w-full max-w-2xl overflow-hidden">
        <Command.Input
          placeholder="Type a command or search..."
          className="w-full px-4 py-3 border-b border-gray-200 focus:outline-none"
        />
        <Command.List className="max-h-96 overflow-auto p-2">
          <Command.Empty className="py-6 text-center text-sm text-gray-500">
            No results found.
          </Command.Empty>

          <Command.Group heading="Actions" className="text-xs text-gray-500 uppercase px-2 py-1">
            <Command.Item
              onSelect={() => handleAction('upload')}
              className="flex items-center gap-3 px-3 py-2 rounded-md cursor-pointer hover:bg-blue-50 data-[selected=true]:bg-blue-50"
            >
              <Upload className="w-4 h-4 text-blue-600" />
              <span>Upload Files</span>
            </Command.Item>
            <Command.Item
              onSelect={() => handleAction('refresh')}
              className="flex items-center gap-3 px-3 py-2 rounded-md cursor-pointer hover:bg-blue-50 data-[selected=true]:bg-blue-50"
            >
              <RefreshCw className="w-4 h-4 text-green-600" />
              <span>Refresh Objects</span>
            </Command.Item>
            <Command.Item
              onSelect={() => handleAction('gc')}
              className="flex items-center gap-3 px-3 py-2 rounded-md cursor-pointer hover:bg-blue-50 data-[selected=true]:bg-blue-50"
            >
              <Trash2 className="w-4 h-4 text-red-600" />
              <span>Garbage Collect</span>
            </Command.Item>
          </Command.Group>

          <Command.Group heading="Objects" className="text-xs text-gray-500 uppercase px-2 py-1 mt-4">
            {objects.slice(0, 10).map(obj => (
              <Command.Item
                key={obj.hash}
                onSelect={() => handleAction('download', obj.hash)}
                className="flex items-center gap-3 px-3 py-2 rounded-md cursor-pointer hover:bg-blue-50 data-[selected=true]:bg-blue-50"
              >
                <Download className="w-4 h-4 text-gray-400" />
                <div className="flex-1 truncate">
                  <div className="text-sm font-medium">{obj.filename || 'Unnamed'}</div>
                  <div className="text-xs text-gray-500 font-mono">{obj.hash.substring(0, 16)}...</div>
                </div>
              </Command.Item>
            ))}
          </Command.Group>
        </Command.List>
      </Command>

      <button
        onClick={closeCommandPalette}
        className="absolute inset-0 -z-10"
        aria-label="Close"
      />
    </div>
  );
}
