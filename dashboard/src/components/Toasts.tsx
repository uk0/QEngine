import { createContext, useCallback, useContext, useState } from 'react';

export interface ToastItem {
  id: number;
  kind: 'ok' | 'err';
  text: string;
}

export function useToasts() {
  const [items, setItems] = useState<ToastItem[]>([]);
  const push = useCallback((kind: 'ok' | 'err', text: string) => {
    const id = Date.now() + Math.random();
    setItems(v => [...v, { id, kind, text }]);
    // Auto-dismiss after 3.2s so the stack never grows without bound.
    setTimeout(() => {
      setItems(v => v.filter(t => t.id !== id));
    }, 3200);
  }, []);
  return { items, ok: (t: string) => push('ok', t), err: (t: string) => push('err', t) };
}

export const ToastCtx = createContext<ReturnType<typeof useToasts> | null>(null);

export function useToastCtx() {
  const c = useContext(ToastCtx);
  if (!c) throw new Error('Toast context missing');
  return c;
}

export function Toasts({ items }: { items: ToastItem[] }) {
  return (
    <div className="toast-stack">
      {items.map(t => (
        <div key={t.id} className={`toast ${t.kind}`}>
          {t.text}
        </div>
      ))}
    </div>
  );
}
