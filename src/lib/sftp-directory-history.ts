// Per-server SFTP directory history + pinned directories.
//
// Each server (keyed by its stable connection id — the same id the file browser
// receives as `connectionId`, which equals the saved connection's id) gets its
// OWN recent-directory list and its OWN pinned list. History from one server is
// therefore never shown while browsing another server.
//
// Persisted to localStorage so both the recent list and the pins survive app
// restarts. A same-window event is dispatched on every mutation so open file
// browsers refresh reactively (the native `storage` event only fires in OTHER
// windows, so it can't cover the current one).

const STORAGE_KEY = 'r-shell-sftp-directory-history';
/** Cap on the recent list per server — keeps localStorage bounded and the
 *  dropdown readable. */
const MAX_RECENT = 30;
/** Cap on the pinned list per server. Pins are user-curated so this is generous,
 *  but bounded so a runaway can't grow localStorage without limit. */
const MAX_PINNED = 100;

/** Fired on the current window after any mutation so mounted browsers refresh. */
export const SFTP_DIR_HISTORY_CHANGED_EVENT = 'r-shell-sftp-dir-history-changed';

/**
 * Reduce a tab's connectionId to a stable SERVER key.
 *
 * Duplicated tabs get ids like `${connection.id}-dup-${timestamp}` (see
 * App.tsx handleDuplicateTab). Stripping that suffix means every tab of the
 * SAME saved connection shares one history/pin set — the feature is "per
 * server", not "per tab" — and lets deleteConnection(id) purge it via
 * forgetServer(id).
 */
export function serverKey(connectionId: string): string {
  return connectionId.replace(/-dup-\d+$/, '');
}

export interface ServerDirState {
  /** Visited directories, most-recent first, de-duplicated. */
  recent: string[];
  /** User-pinned directories, in the order they were pinned. */
  pinned: string[];
}

type Store = Record<string, ServerDirState>;

/** Normalise a remote path so `/home` and `/home/` (and `//home`) are one key. */
export function normalizePath(path: string): string {
  if (!path) return '/';
  let p = path.trim();
  if (!p.startsWith('/')) p = '/' + p;
  p = p.replace(/\/+/g, '/'); // collapse duplicate slashes
  if (p.length > 1) p = p.replace(/\/+$/, ''); // strip trailing slash (keep root)
  return p || '/';
}

function readStore(): Store {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw) as unknown;
    if (parsed && typeof parsed === 'object') return parsed as Store;
  } catch {
    // Corrupt JSON — treat as empty rather than crashing the browser.
  }
  return {};
}

function writeStore(store: Store): void {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(store));
  } catch {
    // Quota exceeded / storage unavailable — history is best-effort.
  }
  try {
    window.dispatchEvent(new Event(SFTP_DIR_HISTORY_CHANGED_EVENT));
  } catch {
    // Non-browser environment (tests) — safe to ignore.
  }
}

function emptyState(): ServerDirState {
  return { recent: [], pinned: [] };
}

/** Read a server's history + pins. Always returns arrays, even for unknown ids. */
export function getDirState(connectionId: string | undefined | null): ServerDirState {
  if (!connectionId) return emptyState();
  const s = readStore()[serverKey(connectionId)];
  if (!s) return emptyState();
  return {
    recent: Array.isArray(s.recent) ? s.recent : [],
    pinned: Array.isArray(s.pinned) ? s.pinned : [],
  };
}

/** Record a successfully-visited directory at the front of the recent list. */
export function recordVisit(connectionId: string | undefined | null, path: string): void {
  if (!connectionId) return;
  const key = serverKey(connectionId);
  const p = normalizePath(path);
  const store = readStore();
  const state = store[key] ?? emptyState();
  const recent = Array.isArray(state.recent) ? state.recent : [];
  // No-op if it's already the most-recent entry — avoids redundant writes and
  // event churn when the same directory is reloaded (tab switches, refresh).
  if (recent[0] === p) return;
  store[key] = {
    recent: [p, ...recent.filter((x) => x !== p)].slice(0, MAX_RECENT),
    pinned: Array.isArray(state.pinned) ? state.pinned : [],
  };
  writeStore(store);
}

/** Pin a directory for this server (idempotent). */
export function pinDir(connectionId: string | undefined | null, path: string): void {
  if (!connectionId) return;
  const key = serverKey(connectionId);
  const p = normalizePath(path);
  const store = readStore();
  const state = store[key] ?? emptyState();
  const pinned = Array.isArray(state.pinned) ? state.pinned : [];
  if (pinned.includes(p)) return;
  store[key] = {
    recent: Array.isArray(state.recent) ? state.recent : [],
    // Bounded: drop the oldest pin if the (generous) cap is exceeded.
    pinned: [...pinned, p].slice(-MAX_PINNED),
  };
  writeStore(store);
}

/** Remove a pin for this server. */
export function unpinDir(connectionId: string | undefined | null, path: string): void {
  if (!connectionId) return;
  const key = serverKey(connectionId);
  const p = normalizePath(path);
  const store = readStore();
  const state = store[key];
  if (!state) return;
  const pinned = (Array.isArray(state.pinned) ? state.pinned : []).filter((x) => x !== p);
  store[key] = { recent: Array.isArray(state.recent) ? state.recent : [], pinned };
  writeStore(store);
}

/** True when `path` is pinned for this server. */
export function isPinned(connectionId: string | undefined | null, path: string): boolean {
  if (!connectionId) return false;
  return getDirState(connectionId).pinned.includes(normalizePath(path));
}

/** Drop a single entry from the recent list (does not touch pins). */
export function removeRecent(connectionId: string | undefined | null, path: string): void {
  if (!connectionId) return;
  const key = serverKey(connectionId);
  const p = normalizePath(path);
  const store = readStore();
  const state = store[key];
  if (!state) return;
  const recent = (Array.isArray(state.recent) ? state.recent : []).filter((x) => x !== p);
  store[key] = { recent, pinned: Array.isArray(state.pinned) ? state.pinned : [] };
  writeStore(store);
}

/** Clear the recent list for this server (pins are kept). */
export function clearRecent(connectionId: string | undefined | null): void {
  if (!connectionId) return;
  const key = serverKey(connectionId);
  const store = readStore();
  const state = store[key];
  if (!state) return;
  store[key] = { recent: [], pinned: Array.isArray(state.pinned) ? state.pinned : [] };
  writeStore(store);
}

/**
 * Forget ALL history + pins for a server. Call this when a saved connection is
 * deleted so its entry doesn't linger orphaned in localStorage forever.
 */
export function forgetServer(connectionId: string | undefined | null): void {
  if (!connectionId) return;
  const key = serverKey(connectionId);
  const store = readStore();
  if (!(key in store)) return;
  delete store[key];
  writeStore(store);
}
