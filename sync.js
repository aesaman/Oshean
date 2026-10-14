/** Firebase Realtime sync — offline-first, multi-device. Fast path: modules cached. */

let app = null;
let dbRef = null;
let unsub = null;
let liveUnsub = null;
let inboxUnsub = null;
let lastNoteUnsub = null;
let fbMods = null; // cached firebase modules

export function hasConfig(cfg) {
  return !!(cfg && cfg.apiKey && cfg.databaseURL && cfg.projectId);
}

async function loadMods() {
  if (fbMods) return fbMods;
  const appMod = await import("https://www.gstatic.com/firebasejs/10.14.1/firebase-app.js");
  const dbMod = await import("https://www.gstatic.com/firebasejs/10.14.1/firebase-database.js");
  fbMods = { ...appMod, ...dbMod };
  return fbMods;
}

export async function initSync(cfg) {
  if (!hasConfig(cfg)) { app = null; dbRef = null; return false; }
  try {
    const { initializeApp, getApps, getDatabase } = await loadMods();
    const apps = getApps();
    app = apps.length ? apps[0] : initializeApp(cfg);
    dbRef = getDatabase(app);
    return true;
  } catch (e) {
    console.warn("Firebase init failed", e);
    app = null; dbRef = null;
    return false;
  }
}

function codePath(syncCode) {
  return "rooms/" + String(syncCode || "").trim().toUpperCase();
}

export async function pushBundle(syncCode, bundle) {
  if (!dbRef || !syncCode) return false;
  try {
    const { ref, set } = await loadMods();
    await set(ref(dbRef, codePath(syncCode) + "/bundle"), { ...bundle, pushedAt: Date.now() });
    return true;
  } catch (e) {
    console.warn("push failed", e);
    return false;
  }
}

export async function pullBundle(syncCode) {
  if (!dbRef || !syncCode) return null;
  try {
    const { ref, get } = await loadMods();
    const snap = await get(ref(dbRef, codePath(syncCode) + "/bundle"));
    return snap.exists() ? snap.val() : null;
  } catch (e) {
    console.warn("pull failed", e);
    return null;
  }
}

export async function subscribeBundle(syncCode, onData) {
  if (unsub) { try { unsub(); } catch {} unsub = null; }
  if (!dbRef || !syncCode) return;
  const { ref, onValue, off } = await loadMods();
  const r = ref(dbRef, codePath(syncCode) + "/bundle");
  const handler = (snap) => { if (snap.exists()) onData(snap.val()); };
  onValue(r, handler);
  unsub = () => off(r, "value", handler);
}

export async function pushLiveDay(syncCode, date, payload) {
  if (!dbRef || !syncCode || !date) return false;
  try {
    const { ref, set } = await loadMods();
    const path = codePath(syncCode) + "/live/" + date;
    // strip undefined so Firebase accepts
    const clean = JSON.parse(JSON.stringify({ ...payload, updatedAt: Date.now() }));
    await set(ref(dbRef, path), clean);
    return true;
  } catch (e) {
    console.warn("live push failed", e);
    return false;
  }
}

export async function subscribeLiveDay(syncCode, date, onData) {
  if (liveUnsub) { try { liveUnsub(); } catch {} liveUnsub = null; }
  if (!dbRef || !syncCode || !date) return;
  const { ref, onValue, off, get } = await loadMods();
  const r = ref(dbRef, codePath(syncCode) + "/live/" + date);
  try {
    const snap = await get(r);
    if (snap.exists()) onData(snap.val());
  } catch {}
  const handler = (snap) => { if (snap.exists()) onData(snap.val()); };
  onValue(r, handler);
  liveUnsub = () => off(r, "value", handler);
}

export async function pushAdvisorNote(syncCode, note) {
  if (!dbRef || !syncCode) return false;
  const code = String(syncCode).trim().toUpperCase();
  try {
    const { ref, set } = await loadMods();
    const id = "n" + Date.now() + "_" + Math.random().toString(36).slice(2, 7);
    const payload = {
      text: String(note.text || "").slice(0, 2000),
      from: note.from || "مشاور",
      date: note.date || "",
      at: Date.now()
    };
    if (note.voice) {
      payload.voice = note.voice;
      payload.voiceMime = note.voiceMime || "audio/webm";
    }
    // lastNote first = instant UI on student, inbox second
    await set(ref(dbRef, codePath(code) + "/lastNote"), { ...payload, id });
    set(ref(dbRef, codePath(code) + "/inbox/" + id), payload).catch(() => {});
    return true;
  } catch (e) {
    console.warn("note push failed", e);
    return false;
  }
}

export async function fetchInbox(syncCode) {
  if (!dbRef || !syncCode) return [];
  try {
    const { ref, get } = await loadMods();
    const snap = await get(ref(dbRef, codePath(syncCode) + "/inbox"));
    if (!snap.exists()) return [];
    const val = snap.val() || {};
    return Object.entries(val).map(([id, v]) => ({ id, ...(v || {}) })).sort((a, b) => (b.at || 0) - (a.at || 0));
  } catch (e) {
    console.warn("fetch inbox failed", e);
    return [];
  }
}

export async function subscribeInbox(syncCode, onData) {
  if (inboxUnsub) { try { inboxUnsub(); } catch {} inboxUnsub = null; }
  if (!dbRef || !syncCode) return () => {};
  const { ref, onValue, off, get } = await loadMods();
  const r = ref(dbRef, codePath(syncCode) + "/inbox");
  try {
    const snap = await get(r);
    const val = snap.exists() ? (snap.val() || {}) : {};
    onData(Object.entries(val).map(([id, v]) => ({ id, ...(v || {}) })).sort((a, b) => (b.at || 0) - (a.at || 0)));
  } catch { onData([]); }
  const handler = (snap) => {
    const val = snap.val() || {};
    onData(Object.entries(val).map(([id, v]) => ({ id, ...(v || {}) })).sort((a, b) => (b.at || 0) - (a.at || 0)));
  };
  onValue(r, handler);
  inboxUnsub = () => off(r, "value", handler);
  return inboxUnsub;
}

/** Instant path: listen only to lastNote for near-realtime advisor messages */
export async function subscribeLastNote(syncCode, onNote) {
  if (lastNoteUnsub) { try { lastNoteUnsub(); } catch {} lastNoteUnsub = null; }
  if (!dbRef || !syncCode) return () => {};
  const { ref, onValue, off } = await loadMods();
  const r = ref(dbRef, codePath(syncCode) + "/lastNote");
  const handler = (snap) => {
    if (snap.exists()) onNote(snap.val());
  };
  onValue(r, handler);
  lastNoteUnsub = () => off(r, "value", handler);
  return lastNoteUnsub;
}

export async function setViewerPresence(syncCode, date, info) {
  if (!dbRef || !syncCode) return;
  try {
    const { ref, set } = await loadMods();
    await set(ref(dbRef, codePath(syncCode) + "/presence/" + date), { ...info, at: Date.now() });
  } catch {}
}

export async function subscribePresence(syncCode, date, onData) {
  if (!dbRef || !syncCode) return () => {};
  const { ref, onValue, off } = await loadMods();
  const r = ref(dbRef, codePath(syncCode) + "/presence/" + date);
  const handler = (snap) => { if (snap.exists()) onData(snap.val()); };
  onValue(r, handler);
  return () => off(r, "value", handler);
}

export async function setLiveMeta(syncCode, meta) {
  if (!dbRef || !syncCode) return false;
  try {
    const { ref, set } = await loadMods();
    await set(ref(dbRef, codePath(syncCode) + "/meta"), { ...meta, at: Date.now() });
    return true;
  } catch (e) { console.warn(e); return false; }
}

export async function getLiveMeta(syncCode) {
  if (!dbRef || !syncCode) return null;
  try {
    const { ref, get } = await loadMods();
    const snap = await get(ref(dbRef, codePath(syncCode) + "/meta"));
    return snap.exists() ? snap.val() : null;
  } catch { return null; }
}

export async function revokeLive(syncCode, date) {
  if (!dbRef || !syncCode) return false;
  try {
    const { ref, set, remove } = await loadMods();
    await set(ref(dbRef, codePath(syncCode) + "/meta"), { revoked: true, revokedAt: Date.now(), revokedDate: date || "" });
    if (date) {
      try { await remove(ref(dbRef, codePath(syncCode) + "/live/" + date)); } catch {}
    }
    return true;
  } catch (e) { console.warn(e); return false; }
}

export async function publishCfg(syncCode, cfg) {
  if (!dbRef || !syncCode || !cfg) return false;
  try {
    const { ref, set } = await loadMods();
    await set(ref(dbRef, codePath(syncCode) + "/cfg"), {
      apiKey: cfg.apiKey,
      projectId: cfg.projectId,
      databaseURL: cfg.databaseURL,
      at: Date.now()
    });
    return true;
  } catch (e) { console.warn("publishCfg", e); return false; }
}

export async function fetchCfg(syncCode) {
  if (!dbRef || !syncCode) return null;
  try {
    const { ref, get } = await loadMods();
    const snap = await get(ref(dbRef, codePath(syncCode) + "/cfg"));
    return snap.exists() ? snap.val() : null;
  } catch { return null; }
}

export function genSyncCode() {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let s = "";
  for (let i = 0; i < 8; i++) s += chars[Math.floor(Math.random() * chars.length)];
  return s;
}
