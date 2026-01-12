import { Upload, Folder, FileCheck } from 'lucide-react';
import { useAppStore } from '../state/store';
import { api } from '../api';
import { useState } from 'react';

interface UploadProgress {
  current: number;
  total: number;
  currentFile: string;
  failed: number;
}

export function UploadZone() {
  const [uploading, setUploading] = useState(false);
  const [dragOver, setDragOver] = useState(false);
  const [progress, setProgress] = useState<UploadProgress | null>(null);
  const currentZone = useAppStore(state => state.currentZone);
  const setStatus = useAppStore(state => state.setStatus);
  const loadObjects = useAppStore(state => state.loadObjects);

  const handleUpload = async (files: FileList | null, isDirectory = false) => {
    if (!files || files.length === 0) return;

    setUploading(true);
    const fileArray = Array.from(files);

    setProgress({
      current: 0,
      total: fileArray.length,
      currentFile: fileArray[0].name,
      failed: 0,
    });

    let successCount = 0;
    let failedCount = 0;

    try {
      for (let i = 0; i < fileArray.length; i++) {
        const file = fileArray[i];

        // Update progress
        setProgress({
          current: i + 1,
          total: fileArray.length,
          currentFile: file.name,
          failed: failedCount,
        });

        try {
          // For directory uploads, preserve the relative path in the filename
          let filename = file.name;
          if (isDirectory && (file as any).webkitRelativePath) {
            filename = (file as any).webkitRelativePath;
          }

          // Create a new File object with the full path as name if needed
          const fileToUpload = isDirectory && (file as any).webkitRelativePath
            ? new File([file], filename, { type: file.type })
            : file;

          const res = await api.uploadObject(currentZone, fileToUpload);
          successCount++;

          // Only update status for last few files to avoid spam
          if (i >= fileArray.length - 3) {
            setStatus(`Uploaded: ${filename} (Dedup: ${res.deduplicated})`);
          }
        } catch (err) {
          console.error(`Failed to upload ${file.name}:`, err);
          failedCount++;
          setProgress(prev => prev ? { ...prev, failed: failedCount } : null);
        }
      }

      // Final status
      if (isDirectory) {
        setStatus(`Directory upload complete: ${successCount} files uploaded${failedCount > 0 ? `, ${failedCount} failed` : ''}`);
      } else {
        setStatus(`Batch upload complete: ${successCount}/${fileArray.length} files uploaded${failedCount > 0 ? ` (${failedCount} failed)` : ''}`);
      }

      await loadObjects();
    } catch (err) {
      console.error('Upload error:', err);
      setStatus('Upload failed');
    } finally {
      setUploading(false);
      setProgress(null);
    }
  };

  const handleDrop = async (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);

    // Check if directory was dropped (Chrome/Edge support DataTransferItem API)
    const items = e.dataTransfer.items;
    if (items) {
      const files: File[] = [];

      for (let i = 0; i < items.length; i++) {
        const item = items[i];
        if (item.kind === 'file') {
          const entry = item.webkitGetAsEntry?.();
          if (entry) {
            if (entry.isDirectory) {
              // Directory dropped, traverse recursively
              await traverseDirectory(entry as any, '', files);
            } else {
              // Single file dropped
              const file = item.getAsFile();
              if (file) files.push(file);
            }
          }
        }
      }

      if (files.length > 0) {
        const fileList = createFileList(files);
        await handleUpload(fileList, files.length > 1);
      }
    } else {
      // Fallback for browsers without DataTransferItem API
      handleUpload(e.dataTransfer.files);
    }
  };

  const handleFileInput = (e: React.ChangeEvent<HTMLInputElement>) => {
    handleUpload(e.target.files, false);
    e.target.value = ''; // Reset input
  };

  const handleDirectoryInput = (e: React.ChangeEvent<HTMLInputElement>) => {
    handleUpload(e.target.files, true);
    e.target.value = ''; // Reset input
  };

  // Helper to traverse directory recursively (for drag-and-drop)
  const traverseDirectory = async (entry: any, path: string, files: File[]) => {
    if (entry.isFile) {
      return new Promise<void>((resolve) => {
        entry.file((file: File) => {
          // Create a new File with the full path
          const fullPath = path + file.name;
          const fileWithPath = new File([file], fullPath, { type: file.type });
          (fileWithPath as any).webkitRelativePath = fullPath;
          files.push(fileWithPath);
          resolve();
        });
      });
    } else if (entry.isDirectory) {
      const dirReader = entry.createReader();
      return new Promise<void>((resolve) => {
        dirReader.readEntries(async (entries: any[]) => {
          for (const childEntry of entries) {
            await traverseDirectory(childEntry, path + entry.name + '/', files);
          }
          resolve();
        });
      });
    }
  };

  // Helper to create FileList-like object from array
  const createFileList = (files: File[]): FileList => {
    const dataTransfer = new DataTransfer();
    files.forEach(file => dataTransfer.items.add(file));
    return dataTransfer.files;
  };

  return (
    <div
      onDrop={handleDrop}
      onDragOver={(e) => { e.preventDefault(); setDragOver(true); }}
      onDragLeave={() => setDragOver(false)}
      className={`border-2 border-dashed rounded-lg p-8 text-center transition ${
        dragOver ? 'border-blue-500 bg-blue-50' : 'border-gray-300 hover:border-gray-400'
      } ${uploading ? 'opacity-50 pointer-events-none' : ''}`}
    >
      <Upload className="w-12 h-12 text-gray-400 mx-auto mb-4" />

      {uploading && progress ? (
        // Upload progress display
        <div className="space-y-4">
          <div className="flex items-center justify-center gap-2 text-blue-600">
            <FileCheck className="w-5 h-5 animate-pulse" />
            <p className="text-gray-600 font-medium">
              Uploading {progress.current} of {progress.total} files...
            </p>
          </div>

          <div className="w-full bg-gray-200 rounded-full h-2.5">
            <div
              className="bg-blue-600 h-2.5 rounded-full transition-all duration-300"
              style={{ width: `${(progress.current / progress.total) * 100}%` }}
            ></div>
          </div>

          <div className="text-xs text-gray-500 space-y-1">
            <p className="truncate font-mono">Current: {progress.currentFile}</p>
            {progress.failed > 0 && (
              <p className="text-red-600">{progress.failed} failed</p>
            )}
          </div>
        </div>
      ) : (
        // Upload options display
        <>
          <p className="text-gray-600 font-medium mb-2">
            Drop files or folders here
          </p>
          <p className="text-sm text-gray-400 mb-6">
            Supports multiple files and recursive directory uploads<br/>
            Auto-deduplication enabled
          </p>

          <div className="flex gap-3 justify-center">
            <label className="inline-flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg cursor-pointer hover:bg-blue-700 transition">
              <Upload className="w-4 h-4" />
              Select Files
              <input
                type="file"
                multiple
                className="hidden"
                onChange={handleFileInput}
                disabled={uploading}
              />
            </label>

            <label className="inline-flex items-center gap-2 px-4 py-2 bg-purple-600 text-white rounded-lg cursor-pointer hover:bg-purple-700 transition">
              <Folder className="w-4 h-4" />
              Select Folder
              <input
                type="file"
                // @ts-ignore - webkitdirectory is not in TypeScript types but supported by modern browsers
                webkitdirectory=""
                directory=""
                multiple
                className="hidden"
                onChange={handleDirectoryInput}
                disabled={uploading}
              />
            </label>
          </div>
        </>
      )}
    </div>
  );
}
