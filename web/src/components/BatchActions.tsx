import { Trash2, Download, X } from 'lucide-react';
import { useAppStore } from '../state/store';
import { saveAs } from 'file-saver';
import { api } from '../api';

export function BatchActions() {
  const selectedObjects = useAppStore(state => state.selectedObjects);
  const objects = useAppStore(state => state.objects);
  const deleteObjects = useAppStore(state => state.deleteObjects);
  const clearSelection = useAppStore(state => state.clearSelection);
  const currentZone = useAppStore(state => state.currentZone);

  if (selectedObjects.size === 0) return null;

  const selectedItems = objects.filter(obj => selectedObjects.has(obj.hash));

  const handleBulkDelete = async () => {
    if (!confirm(`Delete ${selectedObjects.size} object(s)?`)) return;
    await deleteObjects(Array.from(selectedObjects));
  };

  const handleBulkDownload = async () => {
    // Download each file individually (ZIP download requires backend support)
    for (const obj of selectedItems) {
      const blob = await api.downloadObject(currentZone, obj.hash);
      saveAs(blob, obj.filename || `${obj.hash.substring(0, 12)}.bin`);
    }
  };

  return (
    <div className="fixed bottom-6 left-1/2 -translate-x-1/2 bg-white shadow-xl rounded-lg border border-gray-200 p-4 flex items-center gap-4 z-40">
      <span className="text-sm font-medium text-gray-700">
        {selectedObjects.size} selected
      </span>

      <div className="flex gap-2">
        <button
          onClick={handleBulkDownload}
          className="flex items-center gap-2 px-3 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition text-sm"
        >
          <Download className="w-4 h-4" />
          Download
        </button>

        <button
          onClick={handleBulkDelete}
          className="flex items-center gap-2 px-3 py-2 bg-red-600 text-white rounded-md hover:bg-red-700 transition text-sm"
        >
          <Trash2 className="w-4 h-4" />
          Delete
        </button>
      </div>

      <button
        onClick={clearSelection}
        className="ml-2 p-2 text-gray-400 hover:text-gray-600 transition"
        title="Clear selection"
      >
        <X className="w-4 h-4" />
      </button>
    </div>
  );
}
