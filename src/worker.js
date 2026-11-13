/**
 * Football Legacy 27 · servidor online sobre Cloudflare Workers + Durable Objects.
 * Port fiel de server.js (Socket.IO): mismas salas, mismos eventos y mismas reglas.
 *
 * Protocolo (JSON por WebSocket nativo):
 *   cliente → servidor: { e: "evento", d: payload, i: idAck? }
 *   servidor → cliente: { e: "evento", d: payload }  |  { a: idAck, d: respuesta }
 *   latido:             { e: "__ping" }  →  { e: "__pong" }
 */
import { DurableObject } from 'cloudflare:workers';

const MAX_STATE_BYTES = 1_500_000;     // estado de partido
const MAX_INPUT_BYTES = 16_000;        // controles del visitante
const MAX_CAREER_BYTES = 8_000_000;    // estado de carrera
const MAX_RAW_MESSAGE = 10_000_000;    // tamaño máximo de cualquier mensaje
const ROOM_IDLE_MS = 30 * 60 * 1000;   // salas sin actividad se borran
const DEAD_CONN_MS = 75_000;           // sin latido ni mensajes = conexión muerta
const MAX_EVENTS_PER_SEC = 240;        // anti-flood por conexión
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

const enc = new TextEncoder();
function payloadSize(value) {
  try { return enc.encode(JSON.stringify(value ?? null)).length; }
  catch (_) { return Infinity; }
}

function corsHeaders(request, env) {
  const allowed = parseOrigins(env);
  const origin = request.headers.get('Origin') || '';
  const ok = !allowed.length || allowed.includes(origin);
  return {
    'Access-Control-Allow-Origin': ok ? (origin || '*') : 'null',
    'Vary': 'Origin'
  };
}
function parseOrigins(env) {
  return String(env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);
}

// ───────────────────────── Worker (entrada) ─────────────────────────
export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === '/ws') {
      if (request.headers.get('Upgrade') !== 'websocket') {
        return new Response('Se esperaba una conexión WebSocket', { status: 426 });
      }
      const allowed = parseOrigins(env);
      const origin = request.headers.get('Origin');
      if (allowed.length && origin && !allowed.includes(origin)) {
        return new Response('Origen no permitido', { status: 403 });
      }
      return env.HUB.get(env.HUB.idFromName('main')).fetch(request);
    }

    if (url.pathname === '/health' || url.pathname === '/healthz') {
      const res = await env.HUB.get(env.HUB.idFromName('main')).fetch(new Request('https://hub/health'));
      return new Response(res.body, {
        status: res.status,
        headers: { 'Content-Type': 'application/json', ...corsHeaders(request, env) }
      });
    }

    // Cualquier otra ruta: la página del juego (public/).
    return env.ASSETS.fetch(request);
  }
};

// ───────────────────────── Durable Object ─────────────────────────
export class GameHub extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.conns = new Map();   // id → conexión
    this.rooms = new Map();   // código → sala
    this.startedAt = Date.now();
    this.timer = null;
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === '/health') {
      return Response.json({
        ok: true,
        rooms: this.rooms.size,
        clients: this.conns.size,
        uptime: Math.round((Date.now() - this.startedAt) / 1000)
      });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();
    this.onConnect(server);
    this.ensureTimer();
    return new Response(null, { status: 101, webSocket: client });
  }

  // ───── conexiones ─────
  onConnect(ws) {
    const conn = {
      id: crypto.randomUUID().replace(/-/g, '').slice(0, 12),
      ws, room: null, role: null,
      lastSeen: Date.now(), winStart: Date.now(), count: 0
    };
    this.conns.set(conn.id, conn);
    this.send(conn, '__hello', { id: conn.id });

    ws.addEventListener('message', ev => this.onMessage(conn, ev.data));
    const gone = () => this.onDisconnect(conn);
    ws.addEventListener('close', gone);
    ws.addEventListener('error', gone);
  }

  onDisconnect(conn) {
    if (!this.conns.has(conn.id)) return;
    this.leaveRoom(conn, true);
    this.conns.delete(conn.id);
    try { conn.ws.close(1000, 'bye'); } catch (_) {}
  }

  onMessage(conn, raw) {
    conn.lastSeen = Date.now();
    if (typeof raw !== 'string' || raw.length > MAX_RAW_MESSAGE) return;

    // Anti-flood: ventana de 1 s.
    const now = Date.now();
    if (now - conn.winStart >= 1000) { conn.winStart = now; conn.count = 0; }
    if (++conn.count > MAX_EVENTS_PER_SEC) return;

    let msg;
    try { msg = JSON.parse(raw); } catch (_) { return; }
    if (!msg || typeof msg.e !== 'string') return;

    if (msg.e === '__ping') return this.send(conn, '__pong', null);

    const handler = this.handlers[msg.e];
    if (!handler) return;
    const ack = msg.i != null
      ? data => this.sendAck(conn, msg.i, data)
      : undefined;
    try {
      handler.call(this, conn, msg.d, ack);
    } catch (err) {
      console.error('error en evento', msg.e, err);
    }
  }

  // ───── envío ─────
  send(conn, e, d) {
    try { conn.ws.send(JSON.stringify({ e, d })); } catch (_) {}
  }
  sendAck(conn, i, d) {
    try { conn.ws.send(JSON.stringify({ a: i, d })); } catch (_) {}
  }
  sendTo(id, e, d) {
    const c = this.conns.get(id);
    if (c) this.send(c, e, d);
  }
  toRoom(room, e, d, exceptId) {
    for (const id of [room.hostId, room.guestId]) {
      if (id && id !== exceptId) this.sendTo(id, e, d);
    }
  }
  error(conn, message) { this.send(conn, 'room-error', { message }); }

  // ───── salas ─────
  makeCode() {
    let code;
    do {
      code = Array.from({ length: 6 }, () => CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)]).join('');
    } while (this.rooms.has(code));
    return code;
  }

  newRoom(code, hostId, mode) {
    const room = {
      code, hostId, guestId: null, mode,
      started: false, settings: null,
      lobbyState: null, careerState: null,
      lastActivity: Date.now()
    };
    this.rooms.set(code, room);
    return room;
  }

  getRoom(conn) {
    const room = this.rooms.get(conn.room);
    if (room) room.lastActivity = Date.now();
    return room || null;
  }

  validRoomConn(conn, role) {
    const room = this.getRoom(conn);
    return room && room.started && (!role || conn.role === role) ? room : null;
  }

  leaveRoom(conn, notify = true) {
    const code = conn.room;
    if (!code) return;
    const room = this.rooms.get(code);
    conn.room = null;
    conn.role = null;
    if (!room) return;

    if (room.hostId === conn.id) {
      if (notify && room.guestId) this.sendTo(room.guestId, 'peer-left', null);
      this.rooms.delete(code);
    } else if (room.guestId === conn.id) {
      room.guestId = null;
      if (notify) this.sendTo(room.hostId, 'peer-left', null);
    }
  }

  cleanTeam(team) {
    if (!team || typeof team !== 'object') return null;
    const name = String(team.name || '').trim().slice(0, 100);
    if (!name) return null;
    return {
      name,
      color: String(team.color || '').slice(0, 32),
      style: String(team.style || 'solid').slice(0, 32)
    };
  }

  cleanLobbyState(p) {
    return {
      __flLobby: true,
      waiting: !!p.waiting,
      homeTeam: this.cleanTeam(p.homeTeam),
      awayTeam: this.cleanTeam(p.awayTeam),
      hostReady: !!p.hostReady,
      guestReady: !!p.guestReady
    };
  }

  // ───── mantenimiento ─────
  ensureTimer() {
    if (this.timer) return;
    this.timer = setInterval(() => {
      const now = Date.now();
      for (const conn of [...this.conns.values()]) {
        if (now - conn.lastSeen > DEAD_CONN_MS) this.onDisconnect(conn);
      }
      for (const [code, room] of [...this.rooms]) {
        if (!this.conns.has(room.hostId) || now - room.lastActivity > ROOM_IDLE_MS) {
          if (room.guestId) this.sendTo(room.guestId, 'peer-left', null);
          this.rooms.delete(code);
        }
      }
      if (!this.conns.size && !this.rooms.size) {
        clearInterval(this.timer);
        this.timer = null;
      }
    }, 20_000);
  }

  // ───── eventos (mismo protocolo que server.js) ─────
  get handlers() {
    return HANDLERS;
  }
}

const HANDLERS = {
  // Medición de latencia de aplicación.
  'ping-check'(conn, _p, ack) {
    if (ack) ack({ ok: true, serverTime: Date.now() });
  },

  // ── Partido 1v1 ──
  'create-room'(conn) {
    this.leaveRoom(conn, false);
    const code = this.makeCode();
    this.newRoom(code, conn.id, 'match');
    conn.room = code;
    conn.role = 'host';
    this.send(conn, 'room-created', { code });
  },

  'join-room'(conn, payload) {
    const code = String(payload?.code || '').trim().toUpperCase().slice(0, 12);
    const room = this.rooms.get(code);
    if (!room || room.mode !== 'match') {
      return this.error(conn, 'No existe esa sala. Comprueba el código.');
    }
    if (room.hostId === conn.id) {
      return this.error(conn, 'No puedes entrar en tu propia sala como invitado.');
    }
    if (room.guestId && room.guestId !== conn.id) {
      return this.error(conn, 'La sala ya está completa.');
    }

    this.leaveRoom(conn, false);
    room.lastActivity = Date.now();
    room.guestId = conn.id;
    conn.room = code;
    conn.role = 'guest';
    this.send(conn, 'room-joined', { code });
    this.sendTo(room.hostId, 'peer-joined', null);

    if (!room.started && room.lobbyState) this.send(conn, 'match-state', room.lobbyState);
    if (room.started) this.send(conn, 'match-start', { settings: room.settings });
  },

  // ── Modo carrera online ──
  'career-create-room'(conn, _p, ack) {
    this.leaveRoom(conn, false);
    const code = this.makeCode();
    this.newRoom(code, conn.id, 'career');
    conn.room = code;
    conn.role = 'host';
    this.send(conn, 'career-room-created', { code });
    if (ack) ack({ ok: true, code });
  },

  'career-join-room'(conn, payload, ack) {
    const code = String(payload?.code || '').trim().toUpperCase().slice(0, 12);
    const room = this.rooms.get(code);
    if (!room || room.mode !== 'career') {
      return this.error(conn, 'No existe una sala de carrera con ese código.');
    }
    if (room.hostId === conn.id) {
      return this.error(conn, 'Ya eres el anfitrión de esta carrera.');
    }
    if (room.guestId && room.guestId !== conn.id) {
      return this.error(conn, 'La carrera ya tiene dos participantes.');
    }

    this.leaveRoom(conn, false);
    room.lastActivity = Date.now();
    room.guestId = conn.id;
    conn.room = code;
    conn.role = 'guest';
    this.send(conn, 'career-room-joined', {
      code, hasCareer: !!room.careerState, career: room.careerState
    });
    if (ack) ack({ ok: true, code });
    this.sendTo(room.hostId, 'peer-joined', null);
  },

  'career-state'(conn, payload) {
    const room = this.getRoom(conn);
    if (!room || room.mode !== 'career' || conn.role !== 'host') return;
    const state = payload?.career;
    if (!state || typeof state !== 'object') return;
    if (payloadSize(state) > MAX_CAREER_BYTES) {
      return this.error(conn, 'La carrera ocupa demasiado espacio para sincronizarse.');
    }
    room.careerState = state;
    if (room.guestId) {
      this.sendTo(room.guestId, 'career-state', {
        career: state, updatedBy: conn.role, updatedAt: Date.now()
      });
    }
  },

  // ── Inicio de partido ──
  'start-match'(conn, payload) {
    const room = this.getRoom(conn);
    if (!room || conn.role !== 'host' || room.hostId !== conn.id) return;
    if (!room.guestId) return this.error(conn, 'Espera a que tu amigo entre en la sala.');
    if (room.started) return;

    if (room.mode === 'match') {
      const lobby = room.lobbyState;
      if (!lobby || !lobby.hostReady || !lobby.guestReady ||
          !lobby.homeTeam || !lobby.awayTeam ||
          lobby.homeTeam.name === lobby.awayTeam.name) {
        return this.error(conn, 'Los dos jugadores deben elegir equipos diferentes y pulsar ACEPTAR.');
      }
      room.lobbyState = null;
    }

    room.started = true;
    room.settings = payload?.settings || {};
    this.toRoom(room, 'match-start', { settings: room.settings });
  },

  // ── Controles del visitante ──
  'player-input'(conn, payload) {
    const room = this.getRoom(conn);
    if (!room || room.guestId !== conn.id) return;
    if (payloadSize(payload) > MAX_INPUT_BYTES) return;
    const input = payload?.input || {};

    if (!room.started) {
      if (room.mode !== 'match' || input.__flLobbyReady !== true) return;
      const team = this.cleanTeam(input.team);
      if (!team) return;

      const current = room.lobbyState || {
        __flLobby: true, waiting: false,
        homeTeam: null, awayTeam: null,
        hostReady: false, guestReady: false
      };
      current.awayTeam = team;
      current.guestReady = !input.cancelReady;
      room.lobbyState = current;
      this.sendTo(room.hostId, 'player-input', {
        input: { __flLobbyReady: true, team, cancelReady: !!input.cancelReady }
      });
      return;
    }

    this.sendTo(room.hostId, 'player-input', { input });
  },

  // Canal para modalidades de balón parado (el anfitrión es la autoridad).
  'special-mode-state'(conn, payload) {
    const room = this.validRoomConn(conn, 'host');
    if (!room || !['match', 'special'].includes(room.mode)) return;
    if (payloadSize(payload) > MAX_STATE_BYTES) return;
    if (room.guestId) this.sendTo(room.guestId, 'special-mode-state', payload);
  },
  'special-mode-input'(conn, payload) {
    const room = this.validRoomConn(conn, 'guest');
    if (!room || payloadSize(payload) > MAX_INPUT_BYTES) return;
    this.sendTo(room.hostId, 'special-mode-input', payload);
  },

  // ── Estado de partido (anfitrión → invitado) ──
  'match-state'(conn, payload) {
    const room = this.getRoom(conn);
    if (!room || room.hostId !== conn.id) return;

    if (room.mode === 'match' && payload && payload.__flLobby === true && !room.started) {
      const state = this.cleanLobbyState(payload);
      room.lobbyState = state;
      if (room.guestId) this.sendTo(room.guestId, 'match-state', state);
      return;
    }

    if (!room.started || payloadSize(payload) > MAX_STATE_BYTES) return;
    if (room.guestId) this.sendTo(room.guestId, 'match-state', payload);
  }
};
