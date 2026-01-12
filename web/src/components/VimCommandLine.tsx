import { useState, useEffect, useRef } from 'react';
import { useAppStore } from '../state/store';

type VimMode = 'normal' | 'search' | 'find';

interface VimCommandLineProps {
  mode: VimMode;
  onEscape: () => void;
  onExecute: (command: string) => void;
}

export function VimCommandLine({ mode, onEscape, onExecute }: VimCommandLineProps) {
  const [input, setInput] = useState('');
  const inputRef = useRef<HTMLInputElement>(null);
  const objects = useAppStore(state => state.objects);
  const searchResults = useAppStore(state => state.searchResults);

  // Focus input when mode changes
  useEffect(() => {
    if (mode !== 'normal' && inputRef.current) {
      inputRef.current.focus();
    }
  }, [mode]);

  // Reset input when mode changes
  useEffect(() => {
    setInput('');
  }, [mode]);

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Escape') {
      onEscape();
    } else if (e.key === 'Enter') {
      onExecute(input);
      setInput('');
    }
  };

  if (mode === 'normal') return null;

  const getPromptSymbol = () => {
    switch (mode) {
      case 'search': return '/';
      case 'find': return 'f';
      default: return ':';
    }
  };

  const getPlaceholder = () => {
    switch (mode) {
      case 'search': return 'Search objects (FTS5)...';
      case 'find': return 'Find by RID or first letter...';
      default: return 'Command...';
    }
  };

  const resultCount = mode === 'search' && input ? searchResults.length : objects.length;

  return (
    <div className="fixed bottom-0 left-0 right-0 bg-black border-t-2 border-white px-4 py-2 font-mono z-50">
      <div className="max-w-7xl mx-auto flex items-center gap-2">
        <span className="text-white text-lg">{getPromptSymbol()}</span>
        <input
          ref={inputRef}
          type="text"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder={getPlaceholder()}
          className="flex-1 bg-black text-white border-none outline-none placeholder-gray-600 caret-white"
          autoComplete="off"
        />
        {mode === 'search' && input && (
          <span className="text-gray-500 text-sm">
            [{resultCount} found]
          </span>
        )}
      </div>
    </div>
  );
}
