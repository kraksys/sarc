import { FileIcon } from 'lucide-react';
import { useAppStore } from '../state/store';
import { useUIStore } from '../state/ui-store';
import { api } from '../api';
import type { SarcObject } from '../types';

export function ObjectList() {
  const objects = useAppStore(state => state.objects);
  const searchQuery = useAppStore(state => state.searchQuery);
  const searchResults = useAppStore(state => state.searchResults);
  const searching = useAppStore(state => state.searching);
  const selectedObjects = useAppStore(state => state.selectedObjects);
  const toggleSelectObject = useAppStore(state => state.toggleSelectObject);
  const showSecurityModal = useUIStore(state => state.showSecurityModal);
  const showPreview = useUIStore(state => state.showPreview);

  const displayObjects = searchQuery ? searchResults : objects;

  if (displayObjects.length === 0) {
    return (
      <div className="p-16 text-center">
        <FileIcon className="w-8 h-8 mx-auto mb-4" />
        {searching ? (
          <>
            <p className="font-medium">SEARCHING...</p>
            <p className="text-sm mt-2">PRESS [ESC] TO CANCEL</p>
          </>
        ) : searchQuery ? (
          <>
            <p className="font-medium">NO MATCHES</p>
            <p className="text-sm mt-2">PRESS [ESC] TO CLEAR SEARCH</p>
          </>
        ) : (
          <>
            <p className="font-medium">NO OBJECTS FOUND</p>
            <p className="text-sm mt-2">PRESS [U] TO UPLOAD FILES</p>
          </>
        )}
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
  const borderColor = selected ? 'border-black' : 'border-white';
  const dimText = selected ? 'text-black' : 'text-gray-400';
  const mainText = selected ? 'text-black' : 'text-white';

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
      data-object-hash={object.hash}
      className={`grid grid-cols-12 gap-4 p-2 items-center border-b ${borderColor} ${
        selected ? 'vintage-invert' : 'hover:bg-gray-800'
      }`}
    >
      {/* RID */}
      <div className={`col-span-1 text-center font-mono ${mainText}`}>
        {rid.toString().padStart(3, '0')}
      </div>

      {/* Filename / Hash */}
      <div className={`col-span-4 font-mono text-xs truncate select-all ${mainText}`} title={object.hash}>
        {object.filename || object.hash}
      </div>

      {/* Size */}
      <div className={`col-span-2 text-xs ${dimText}`}>
        {formatSize(object.size)}
      </div>

      {/* MIME Type */}
      <div className={`col-span-2 text-xs truncate ${dimText}`} title={object.mime_type}>
        {object.mime_type || 'blob'}
      </div>

      {/* Actions */}
      <div className="col-span-3 flex items-center justify-end gap-1 text-xs">
        {isPreviewable(object.mime_type) && (
          <button
            onClick={onPreview}
            className={`px-2 py-1 border ${borderColor} vintage-outline`}
            title="Preview"
          >
            VIEW
          </button>
        )}
        <button
          onClick={onShowSecurity}
          className={`px-2 py-1 border ${borderColor} vintage-outline`}
          title="Security Info"
        >
          SEC
        </button>
        <a
          href={api.getObjectUrl(object.zone, object.hash)}
          download={object.filename}
          className={`px-2 py-1 border ${borderColor} vintage-outline`}
          title="Download"
        >
          DL
        </a>
        <button
          onClick={handleDelete}
          className={`px-2 py-1 border ${borderColor} vintage-outline`}
          title="Delete"
        >
          DEL
        </button>
        <button
          onClick={onToggleSelect}
          className={`px-2 py-1 border ${borderColor} ${selected ? 'bg-black text-white' : 'vintage-outline'}`}
          title="Select/Unselect"
        >
          [X]
        </button>
      </div>
    </div>
  );
}
