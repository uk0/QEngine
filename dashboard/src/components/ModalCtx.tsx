import { createContext, ReactNode, useContext } from 'react';
import { ModalHost, useModal } from './Modal';

/* Context wrapper so any descendant can await modal.confirm()/.form()
 * without prop-drilling.  Host renders the actual dialog and lives in
 * App at the root. */

type ModalApi = ReturnType<typeof useModal>;

export const ModalCtx = createContext<ModalApi | null>(null);

export function useModalCtx(): ModalApi {
  const c = useContext(ModalCtx);
  if (!c) throw new Error('ModalCtx not mounted');
  return c;
}

export function ModalProvider({ children }: { children: ReactNode }) {
  const modal = useModal();
  return (
    <ModalCtx.Provider value={modal}>
      {children}
      <ModalHost state={modal.open} />
    </ModalCtx.Provider>
  );
}

export { ModalHost, useModal };
