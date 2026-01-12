import { useEffect, useRef } from 'react';
import { useAppStore } from '../state/store';
import { SarcObject } from '../types';

export function useWebSocket() {
  const wsRef = useRef<WebSocket | null>(null);
  const currentZone = useAppStore(state => state.currentZone);
  const objects = useAppStore(state => state.objects);
  const setStatus = useAppStore(state => state.setStatus);

  useEffect(() => {
    const ws = new WebSocket('ws://localhost:8080/ws');
    wsRef.current = ws;

    ws.onopen = () => {
      console.log('WebSocket connected');
      ws.send(JSON.stringify({ action: 'subscribe', zone: currentZone }));
      setStatus('Connected');
    };

    ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);

        if (msg.type === 'new_object') {
          const newObj: SarcObject = {
            zone: msg.key.zone,
            hash: msg.key.hash,
            size: msg.meta.size,
            created_at: msg.meta.created_at,
            filename: msg.meta.filename,
            mime_type: msg.meta.mime_type,
          };

          // Only add if not already in list
          if (!objects.find(obj => obj.hash === newObj.hash)) {
            useAppStore.setState({ objects: [newObj, ...objects] });
            setStatus(`New object: ${newObj.filename || newObj.hash.substring(0, 8)}`);
          }
        } else if (msg.type === 'object_deleted') {
          useAppStore.setState({
            objects: objects.filter(obj => obj.hash !== msg.hash)
          });
        }
      } catch (e) {
        console.error('WS parse error', e);
      }
    };

    ws.onerror = (error) => {
      console.error('WebSocket error:', error);
      setStatus('Connection error');
    };

    ws.onclose = () => {
      console.log('WebSocket disconnected');
      setStatus('Disconnected');
    };

    return () => {
      ws.close();
    };
  }, [currentZone]);

  return wsRef.current;
}
