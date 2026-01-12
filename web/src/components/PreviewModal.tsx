import { X, Download } from 'lucide-react';
import { useUIStore } from '../state/ui-store';
import { api } from '../api';
import { useState, useEffect } from 'react';

export function PreviewModal() {
  const previewObject = useUIStore(state => state.previewObject);
  const closePreview = useUIStore(state => state.closePreview);
  const [content, setContent] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!previewObject) return;

    const loadPreview = async () => {
      setLoading(true);
      try {
        const blob = await api.downloadObject(previewObject.zone, previewObject.hash);

        // Handle different MIME types
        if (previewObject.mime_type?.startsWith('text/') ||
            previewObject.mime_type === 'application/json') {
          const text = await blob.text();
          setContent(text);
        } else if (previewObject.mime_type?.startsWith('image/')) {
          const url = URL.createObjectURL(blob);
          setContent(url);
        } else if (previewObject.mime_type === 'application/pdf') {
          const url = URL.createObjectURL(blob);
          setContent(url);
        }
      } catch (err) {
        console.error('Preview load error:', err);
      } finally {
        setLoading(false);
      }
    };

    loadPreview();
  }, [previewObject]);

  if (!previewObject) return null;

  const renderPreview = () => {
    if (loading) return <div className="text-center py-8">Loading preview...</div>;
    if (!content) return <div className="text-center py-8 text-gray-500">Preview not available</div>;

    if (previewObject.mime_type?.startsWith('image/')) {
      return <img src={content} alt={previewObject.filename} className="max-w-full max-h-[70vh] mx-auto" />;
    }

    if (previewObject.mime_type === 'application/pdf') {
      return <iframe src={content} className="w-full h-[70vh] border-0" />;
    }

    if (previewObject.mime_type?.startsWith('text/') || previewObject.mime_type === 'application/json') {
      return (
        <pre className="bg-gray-50 p-4 rounded-lg overflow-auto max-h-[70vh] text-xs">
          <code>{content}</code>
        </pre>
      );
    }

    return <div className="text-center py-8 text-gray-500">Preview not supported for this file type</div>;
  };

  return (
    <div className="fixed inset-0 bg-black/50 backdrop-blur-sm flex items-center justify-center z-50 p-4">
      <div className="bg-white rounded-xl shadow-xl border border-gray-200 max-w-5xl w-full max-h-[90vh] overflow-hidden">
        {/* Header */}
        <div className="flex justify-between items-center p-4 border-b border-gray-200">
          <div>
            <h3 className="font-bold text-lg">{previewObject.filename || 'Preview'}</h3>
            <p className="text-sm text-gray-500">{previewObject.mime_type}</p>
          </div>
          <div className="flex gap-2">
            <a
              href={api.getObjectUrl(previewObject.zone, previewObject.hash)}
              download={previewObject.filename}
              className="p-2 text-blue-600 hover:bg-blue-50 rounded-md transition"
              title="Download"
            >
              <Download className="w-5 h-5" />
            </a>
            <button onClick={closePreview} className="p-2 text-gray-400 hover:text-gray-600">
              <X className="w-5 h-5" />
            </button>
          </div>
        </div>

        {/* Preview Content */}
        <div className="p-4 overflow-auto">
          {renderPreview()}
        </div>
      </div>
    </div>
  );
}
