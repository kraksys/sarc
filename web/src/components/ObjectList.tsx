import { Download, Trash2, Shield, FileIcon, Eye } from 'lucide-react';
import { useAppStore } from '../state/store';
import { useUIStore } from '../state/ui-store';
import { api } from '../api';
import type { SarcObject } from '../types';

export function ObjectList() {
  const objects = useAppStore(state => state.objects);
  const searchResults = useAppStore(state => state.searchResults);
  const selectedObjects = useAppStore(state => state.selectedObjects);
  const toggleSelectObject = useAppStore(state => state.toggleSelectObject);
  const showSecurityModal = useUIStore(state => state.showSecurityModal);
  const showPreview = useUIStore(state => state.showPreview);

  // Display search results if available
  const displayObjects = searchResults.length > 0 ? searchResults : objects;

  if (displayObjects.length === 0) {
    return (
      <div className="p-16 text-center">
        <FileIcon className="w-8 h-8 mx-auto mb-4" />
        <p className="font-medium">NO OBJECTS FOUND</p>
        <p className="text-sm mt-2">PRESS [U] TO UPLOAD FILES</p>
      </div>
    );
  }

  return (
    <div>
      {displayObjects.map((obj, index) => (
        <ObjectRow
          key={obj.hash}
          object={obj}
          rid={index + 1}
          selected={selectedObjects.has(obj.hash)}
          onToggleSelect={() => toggleSelectObject(obj.hash)}
          onShowSecurity={() => showSecurityModal(obj)}
          onPreview={() => showPreview(obj)}
        />
      ))}
    </div>
  );
}

interface ObjectRowProps {
  object: SarcObject;
  rid: number;
  selected: boolean;
  onToggleSelect: () => void;
  onShowSecurity: () => void;
  onPreview: () => void;
}

function ObjectRow({ object, rid, selected, onToggleSelect, onShowSecurity, onPreview }: ObjectRowProps) {
  const deleteObjects = useAppStore(state => state.deleteObjects);

  const handleDelete = async () => {
    if (!confirm(`Delete "${object.filename || object.hash.substring(0, 12)}"?`)) return;
    await deleteObjects([object.hash]);
  };

  const formatSize = (bytes?: number) => {
    if (!bytes) return '-';
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
  };

  const isPreviewable = (mime?: string) => {
    if (!mime) return false;
    return mime.startsWith('image/') ||
           mime.startsWith('text/') ||
           mime === 'application/pdf' ||
           mime === 'application/json';
  };

  return (
    <div
      className={`grid grid-cols-12 gap-4 p-2 items-center border-b border-white hover:bg-gray-800 ${
        selected ? 'bg-white text-black' : ''
      }`}
    >
      {/* RID */}
      <div className="col-span-1 text-center font-mono">
        {rid.toString().padStart(3, '0')}
      </div>

      {/* Filename / Hash */}
      <div className="col-span-4 font-mono text-xs text-gray-600 truncate select-all" title={object.hash}>
        {object.filename || object.hash}
      </div>

      {/* Size */}
      <div className="col-span-2 text-xs text-gray-500">
        {formatSize(object.size)}
      </div>

      {/* MIME Type */}
      <div className="col-span-2 text-xs text-gray-500 truncate" title={object.mime_type}>
        {object.mime_type || 'blob'}
      </div>

      {/* Actions */}
      <div className="col-span-3 flex items-center justify-end gap-1 text-xs">
        {isPreviewable(object.mime_type) && (
          <button
            onClick={onPreview}
            className="px-2 py-1 border border-white hover:bg-white hover:text-black"
            title="Preview"
          >
            VIEW
          </button>
        )}
        <button
          onClick={onShowSecurity}
          className="px-2 py-1 border border-white hover:bg-white hover:text-black"
          title="Security Info"
        >
          SEC
        </button>
        <a
          href={api.getObjectUrl(object.zone, object.hash)}
          download={object.filename}
          className="px-2 py-1 border border-white hover:bg-white hover:text-black"
          title="Download"
        >
          DL
        </a>
        <button
          onClick={handleDelete}
          className="px-2 py-1 border border-white hover:bg-white hover:text-black"
          title="Delete"
        >
          DEL
        </button>
        <button
          onClick={onToggleSelect}
          className={`px-2 py-1 border border-white ${selected ? 'bg-black text-white' : 'hover:bg-white hover:text-black'}`}
          title="Select/Unselect"
        >
          [X]
        </button>
      </div>
    </div>
  );
}
