import { FormEvent, ReactNode, useEffect, useRef, useState } from 'react';

/* ── Modal — reusable dialog replacing alert() / prompt() / confirm(). ──
 *
 * Two shapes:
 *   - confirm:  title + description + danger / default button
 *   - form:     title + list of text fields, returns filled values
 *
 * Both are driven by the useModal() hook which exposes async .confirm()
 * and .form() helpers so callers can `await` a response.  Backdrop click
 * + Escape key dismiss.  Autofocuses first input / primary button.
 */

interface FormField {
  name: string;
  label: string;
  value?: string;
  placeholder?: string;
  readonly?: boolean;
  type?: 'text' | 'password';
  /** Row below the label, e.g. "Parent database: prod". */
  help?: string;
}

type Kind = 'confirm' | 'form';

interface ConfirmArgs {
  kind: 'confirm';
  title: string;
  body: ReactNode;
  confirmText?: string;
  cancelText?: string;
  danger?: boolean;
  resolve: (ok: boolean) => void;
}
interface FormArgs {
  kind: 'form';
  title: string;
  body?: ReactNode;
  fields: FormField[];
  submitText?: string;
  resolve: (values: Record<string, string> | null) => void;
}
type OpenArgs = ConfirmArgs | FormArgs;

interface Modal {
  open: OpenArgs | null;
  confirm: (
    title: string,
    body?: ReactNode,
    opts?: { danger?: boolean; confirmText?: string; cancelText?: string },
  ) => Promise<boolean>;
  form: (
    title: string,
    fields: FormField[],
    opts?: { submitText?: string; body?: ReactNode },
  ) => Promise<Record<string, string> | null>;
}

export function useModal(): Modal {
  const [open, setOpen] = useState<OpenArgs | null>(null);
  const modal: Modal = {
    open,
    confirm(title, body = null, opts = {}) {
      return new Promise<boolean>(resolve => {
        setOpen({
          kind: 'confirm',
          title,
          body,
          confirmText: opts.confirmText ?? 'Confirm',
          cancelText: opts.cancelText ?? 'Cancel',
          danger: opts.danger,
          resolve: ok => {
            setOpen(null);
            resolve(ok);
          },
        });
      });
    },
    form(title, fields, opts = {}) {
      return new Promise<Record<string, string> | null>(resolve => {
        setOpen({
          kind: 'form',
          title,
          body: opts.body,
          fields,
          submitText: opts.submitText ?? 'Create',
          resolve: values => {
            setOpen(null);
            resolve(values);
          },
        });
      });
    },
  };
  return modal;
}

export function ModalHost({ state }: { state: OpenArgs | null }) {
  if (!state) return null;
  return <ModalImpl state={state} />;
}

function ModalImpl({ state }: { state: OpenArgs }) {
  const ref = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const onKey = (ev: KeyboardEvent) => {
      if (ev.key === 'Escape') {
        if (state.kind === 'confirm') state.resolve(false);
        else state.resolve(null);
      }
    };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [state]);

  const onBackdrop = (ev: React.MouseEvent<HTMLDivElement>) => {
    if (ev.target === ev.currentTarget) {
      if (state.kind === 'confirm') state.resolve(false);
      else state.resolve(null);
    }
  };

  const kind: Kind = state.kind;

  return (
    <div className="modal-backdrop" onMouseDown={onBackdrop}>
      <div className="modal" ref={ref} role="dialog" aria-modal="true">
        <div className="modal-head">
          <div className="modal-title">{state.title}</div>
        </div>
        {kind === 'confirm' ? (
          <ConfirmBody state={state as ConfirmArgs} />
        ) : (
          <FormBody state={state as FormArgs} />
        )}
      </div>
    </div>
  );
}

function ConfirmBody({ state }: { state: ConfirmArgs }) {
  const primary = useRef<HTMLButtonElement | null>(null);
  useEffect(() => { primary.current?.focus(); }, []);
  return (
    <>
      {state.body && <div className="modal-body">{state.body}</div>}
      <div className="modal-actions">
        <button onClick={() => state.resolve(false)}>{state.cancelText}</button>
        <button
          ref={primary}
          className={state.danger ? 'danger' : 'primary'}
          onClick={() => state.resolve(true)}
        >
          {state.confirmText}
        </button>
      </div>
    </>
  );
}

function FormBody({ state }: { state: FormArgs }) {
  const [values, setValues] = useState<Record<string, string>>(() =>
    Object.fromEntries(state.fields.map(f => [f.name, f.value ?? ''])),
  );
  const first = useRef<HTMLInputElement | null>(null);
  useEffect(() => { first.current?.focus(); first.current?.select(); }, []);

  const submit = (ev: FormEvent) => {
    ev.preventDefault();
    state.resolve(values);
  };
  return (
    <form onSubmit={submit}>
      {state.body && <div className="modal-body">{state.body}</div>}
      <div className="modal-fields">
        {state.fields.map((f, i) => (
          <div className="modal-field" key={f.name}>
            <label>{f.label}</label>
            <input
              ref={i === 0 ? first : undefined}
              type={f.type ?? 'text'}
              value={values[f.name] ?? ''}
              placeholder={f.placeholder}
              readOnly={f.readonly}
              onChange={e => setValues(v => ({ ...v, [f.name]: e.target.value }))}
            />
            {f.help && <div className="modal-field-help">{f.help}</div>}
          </div>
        ))}
      </div>
      <div className="modal-actions">
        <button type="button" onClick={() => state.resolve(null)}>Cancel</button>
        <button type="submit" className="primary">{state.submitText}</button>
      </div>
    </form>
  );
}
