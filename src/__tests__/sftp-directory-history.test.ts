import { describe, it, expect, beforeEach } from 'vitest';
import {
  getDirState,
  recordVisit,
  pinDir,
  unpinDir,
  isPinned,
  removeRecent,
  clearRecent,
  normalizePath,
  serverKey,
  forgetServer,
} from '../lib/sftp-directory-history';

const A = 'conn-A';
const B = 'conn-B';

beforeEach(() => {
  localStorage.clear();
});

describe('normalizePath', () => {
  it('adds a leading slash and strips a trailing one', () => {
    expect(normalizePath('home/user')).toBe('/home/user');
    expect(normalizePath('/home/user/')).toBe('/home/user');
  });
  it('collapses duplicate slashes and keeps root', () => {
    expect(normalizePath('//home///user//')).toBe('/home/user');
    expect(normalizePath('/')).toBe('/');
    expect(normalizePath('')).toBe('/');
  });
  it('treats trailing-slash and non-trailing variants as one key', () => {
    recordVisit(A, '/var/log');
    recordVisit(A, '/var/log/');
    expect(getDirState(A).recent).toEqual(['/var/log']);
  });
});

describe('recordVisit', () => {
  it('adds most-recent first and de-duplicates (move to front)', () => {
    recordVisit(A, '/home');
    recordVisit(A, '/etc');
    recordVisit(A, '/home');
    expect(getDirState(A).recent).toEqual(['/home', '/etc']);
  });
  it('caps the recent list at 30 entries', () => {
    for (let i = 0; i < 40; i++) recordVisit(A, `/dir${i}`);
    const { recent } = getDirState(A);
    expect(recent).toHaveLength(30);
    expect(recent[0]).toBe('/dir39');
    expect(recent).not.toContain('/dir9');
  });
  it('is a no-op for an empty connectionId', () => {
    recordVisit(undefined, '/home');
    recordVisit('', '/home');
    expect(getDirState(undefined).recent).toEqual([]);
  });
});

describe('per-server isolation', () => {
  it('never mixes one server history into another', () => {
    recordVisit(A, '/srv/a-only');
    recordVisit(B, '/srv/b-only');
    expect(getDirState(A).recent).toEqual(['/srv/a-only']);
    expect(getDirState(B).recent).toEqual(['/srv/b-only']);
    expect(getDirState(A).recent).not.toContain('/srv/b-only');
    expect(getDirState(B).recent).not.toContain('/srv/a-only');
  });
  it('keeps pins isolated per server', () => {
    pinDir(A, '/opt/app');
    expect(isPinned(A, '/opt/app')).toBe(true);
    expect(isPinned(B, '/opt/app')).toBe(false);
  });
});

describe('pin / unpin', () => {
  it('pins idempotently and unpins', () => {
    pinDir(A, '/data');
    pinDir(A, '/data');
    expect(getDirState(A).pinned).toEqual(['/data']);
    unpinDir(A, '/data');
    expect(getDirState(A).pinned).toEqual([]);
    expect(isPinned(A, '/data')).toBe(false);
  });
  it('normalizes on pin so trailing-slash matches', () => {
    pinDir(A, '/data/');
    expect(isPinned(A, '/data')).toBe(true);
  });
});

describe('removeRecent / clearRecent', () => {
  it('removes a single recent entry', () => {
    recordVisit(A, '/a');
    recordVisit(A, '/b');
    removeRecent(A, '/a');
    expect(getDirState(A).recent).toEqual(['/b']);
  });
  it('clearRecent empties recent but keeps pins', () => {
    recordVisit(A, '/a');
    pinDir(A, '/keep');
    clearRecent(A);
    expect(getDirState(A).recent).toEqual([]);
    expect(getDirState(A).pinned).toEqual(['/keep']);
  });
});

describe('serverKey (per-server, not per-tab)', () => {
  it('strips the -dup-<timestamp> suffix so duplicated tabs share one history', () => {
    expect(serverKey('conn-123')).toBe('conn-123');
    expect(serverKey('conn-123-dup-1700000000000')).toBe('conn-123');
  });
  it('a duplicated tab sees and appends to the base connection history', () => {
    recordVisit('conn-123', '/base');
    // Duplicated tab of the same server uses `${id}-dup-${ts}`.
    expect(getDirState('conn-123-dup-1700000000000').recent).toEqual(['/base']);
    recordVisit('conn-123-dup-1700000000000', '/from-dup');
    expect(getDirState('conn-123').recent).toEqual(['/from-dup', '/base']);
  });
});

describe('forgetServer', () => {
  it('purges a server entry entirely (history + pins)', () => {
    recordVisit(A, '/a');
    pinDir(A, '/p');
    recordVisit(B, '/b');
    forgetServer(A);
    expect(getDirState(A)).toEqual({ recent: [], pinned: [] });
    // Other servers untouched.
    expect(getDirState(B).recent).toEqual(['/b']);
  });
  it('forgets via the base id even for a duplicated-tab id', () => {
    recordVisit('conn-9', '/x');
    forgetServer('conn-9-dup-42');
    expect(getDirState('conn-9')).toEqual({ recent: [], pinned: [] });
  });
});

describe('pinned cap', () => {
  it('bounds the pinned list (drops the oldest beyond the cap)', () => {
    for (let i = 0; i < 120; i++) pinDir(A, `/pin${i}`);
    const { pinned } = getDirState(A);
    expect(pinned.length).toBe(100);
    expect(pinned).toContain('/pin119');
    expect(pinned).not.toContain('/pin0');
  });
});

describe('persistence', () => {
  it('survives via localStorage (a fresh read returns stored state)', () => {
    recordVisit(A, '/persisted');
    pinDir(A, '/pinned');
    // Simulate a fresh read (e.g. after app restart) — no in-memory state.
    expect(getDirState(A)).toEqual({ recent: ['/persisted'], pinned: ['/pinned'] });
  });
  it('tolerates corrupt localStorage without throwing', () => {
    localStorage.setItem('r-shell-sftp-directory-history', '{not valid json');
    expect(() => getDirState(A)).not.toThrow();
    expect(getDirState(A)).toEqual({ recent: [], pinned: [] });
  });
});
