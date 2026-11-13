'use strict';
/**
 * Football Legacy 27 · servidor online
 * Compatible con el protocolo del cliente HTML (socket.io 4.x).
 * Despliegue: Render / Railway / Fly.io / VPS (necesita Node.js con proceso persistente).
 */
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

// ───────── Configuración (variables de entorno opcionales) ─────────
const PORT = Number(process.env.PORT) || 3000;
// ALLOWED_ORIGINS="https://mijuego.pages.dev,https://otro.com"  (vacío = cualquiera)
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '')
  .split(',').map(s => s.trim()).filter(Boolean);
const MAX_STATE_BYTES = 1_500_000;      // estado de partido
const MAX_INPUT_BYTES = 16_000;         // controles del visitante
const MAX_CAREER_BYTES = 8_000_000;     // estado de carrera
const ROOM_IDLE_MS = 30 * 60 * 1000;    // salas sin actividad se borran
const MAX_EVENTS_PER_SEC = 240;         // anti-flood por socket
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

const app = express();
app.disable('x-powered-by');
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: ALLOWED_ORIGINS.length ? ALLOWED_ORIGINS : '*',
    methods: ['GET', 'POST']
  },
  transports: ['websocket', 'polling'],
  pingInterval: 25000,
  pingTimeout: 30000,
  // Imprescindible: el valor por defecto (1 MB) cortaba la conexión con carreras grandes.
  maxHttpBufferSize: 10_000_000,
  // Sin compresión por mensaje: menos latencia y menos CPU en planes gratuitos.
  perMessageDeflate: false,
  connectionStateRecovery: { maxDisconnectionDuration: 15_000 }
});

const rooms = new Map();

// ───────── HTTP ─────────
app.get('/', (_req, res) => res.type('text').send('Football Legacy 27 · servidor online activo'));
app.get(['/health', '/healthz'], (_req, res) => {
  res.json({
    ok: true,
    rooms: rooms.size,
    clients: io.engine.clientsCount,
    uptime: Math.round(process.uptime())
  });
});

// ───────── Utilidades ─────────
function payloadSize(value) {
  try { return Buffer.byteLength(JSON.stringify(value ?? null), 'utf8'); }
  catch (_) { return Infinity; }
}

function makeCode() {
  let code;
  do {
    code = Array.from({ length: 6 }, () => CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)]).join('');
  } while (rooms.has(code));
  return code;
}

function touch(room) { if (room) room.lastActivity = Date.now(); }

function newRoom(code, hostId, mode) {
  const room = {
    code, hostId, guestId: null,
    started: false, settings: null, mode,
    lobbyState: null, careerState: null,
    lastActivity: Date.now()
  };
  rooms.set(code, room);
  return room;
}

function getRoom(socket) {
  const room = rooms.get(socket.data.room);
  if (room) touch(room);
  return room || null;
}

function validRoomSocket(socket, role) {
  const room = getRoom(socket);
  return room && room.started && (!role || socket.data.role === role) ? room : null;
}

function errorTo(socket, message) {
  socket.emit('room-error', { message });
}

function leaveRoom(socket, notify = true) {
  const code = socket.data.room;
  if (!code) return;
  const room = rooms.get(code);
  socket.leave(code);
  socket.data.room = null;
  socket.data.role = null;
  if (!room) return;

  if (room.hostId === socket.id) {
    if (notify && room.guestId) io.to(room.guestId).emit('peer-left');
    rooms.delete(code);
  } else if (room.guestId === socket.id) {
    room.guestId = null;
    if (notify) io.to(room.hostId).emit('peer-left');
  }
}

function cleanTeam(team) {
  if (!team || typeof team !== 'object') return null;
  const name = String(team.name || '').trim().slice(0, 100);
  if (!name) return null;
  return {
    name,
    color: String(team.color || '').slice(0, 32),
    style: String(team.style || 'solid').slice(0, 32)
  };
}

function cleanLobbyState(payload) {
  return {
    __flLobby: true,
    waiting: !!payload.waiting,
    homeTeam: cleanTeam(payload.homeTeam),
    awayTeam: cleanTeam(payload.awayTeam),
    hostReady: !!payload.hostReady,
    guestReady: !!payload.guestReady
  };
}

// ───────── Sockets ─────────
io.on('connection', socket => {
  // Anti-flood: ventana de 1 s por socket.
  let windowStart = Date.now();
  let count = 0;
  socket.use((_packet, next) => {
    const now = Date.now();
    if (now - windowStart >= 1000) { windowStart = now; count = 0; }
    if (++count > MAX_EVENTS_PER_SEC) return; // se descarta el paquete
    next();
  });

  socket.on('error', () => {});

  // Medición de latencia de aplicación.
  socket.on('ping-check', (_payload, ack) => {
    if (typeof ack === 'function') ack({ ok: true, serverTime: Date.now() });
  });

  // ── Partido 1v1 ──
  socket.on('create-room', () => {
    leaveRoom(socket, false);
    const code = makeCode();
    newRoom(code, socket.id, 'match');
    socket.join(code);
    socket.data.room = code;
    socket.data.role = 'host';
    socket.emit('room-created', { code });
  });

  socket.on('join-room', payload => {
    const code = String(payload?.code || '').trim().toUpperCase().slice(0, 12);
    const room = rooms.get(code);
    if (!room || room.mode !== 'match') {
      return errorTo(socket, 'No existe esa sala. Comprueba el código.');
    }
    if (room.hostId === socket.id) {
      return errorTo(socket, 'No puedes entrar en tu propia sala como invitado.');
    }
    if (room.guestId && room.guestId !== socket.id) {
      return errorTo(socket, 'La sala ya está completa.');
    }

    leaveRoom(socket, false);
    touch(room);
    room.guestId = socket.id;
    socket.join(code);
    socket.data.room = code;
    socket.data.role = 'guest';
    socket.emit('room-joined', { code });
    io.to(room.hostId).emit('peer-joined');

    if (!room.started && room.lobbyState) socket.emit('match-state', room.lobbyState);
    if (room.started) socket.emit('match-start', { settings: room.settings });
  });

  // ── Modo carrera online ──
  socket.on('career-create-room', ack => {
    leaveRoom(socket, false);
    const code = makeCode();
    newRoom(code, socket.id, 'career');
    socket.join(code);
    socket.data.room = code;
    socket.data.role = 'host';
    socket.emit('career-room-created', { code });
    if (typeof ack === 'function') ack({ ok: true, code });
  });

  socket.on('career-join-room', (payload, ack) => {
    const code = String(payload?.code || '').trim().toUpperCase().slice(0, 12);
    const room = rooms.get(code);
    if (!room || room.mode !== 'career') {
      return errorTo(socket, 'No existe una sala de carrera con ese código.');
    }
    if (room.hostId === socket.id) {
      return errorTo(socket, 'Ya eres el anfitrión de esta carrera.');
    }
    if (room.guestId && room.guestId !== socket.id) {
      return errorTo(socket, 'La carrera ya tiene dos participantes.');
    }

    leaveRoom(socket, false);
    touch(room);
    room.guestId = socket.id;
    socket.join(code);
    socket.data.room = code;
    socket.data.role = 'guest';
    socket.emit('career-room-joined', {
      code, hasCareer: !!room.careerState, career: room.careerState
    });
    if (typeof ack === 'function') ack({ ok: true, code });
    io.to(room.hostId).emit('peer-joined');
  });

  socket.on('career-state', payload => {
    const room = getRoom(socket);
    if (!room || room.mode !== 'career' || socket.data.role !== 'host') return;
    const state = payload?.career;
    if (!state || typeof state !== 'object') return;

    if (payloadSize(state) > MAX_CAREER_BYTES) {
      return errorTo(socket, 'La carrera ocupa demasiado espacio para sincronizarse.');
    }
    room.careerState = state;
    socket.to(room.code).emit('career-state', {
      career: state, updatedBy: socket.data.role, updatedAt: Date.now()
    });
  });

  // ── Inicio de partido ──
  socket.on('start-match', payload => {
    const room = getRoom(socket);
    if (!room || socket.data.role !== 'host' || room.hostId !== socket.id) return;
    if (!room.guestId) return errorTo(socket, 'Espera a que tu amigo entre en la sala.');
    if (room.started) return;

    if (room.mode === 'match') {
      const lobby = room.lobbyState;
      if (!lobby || !lobby.hostReady || !lobby.guestReady ||
          !lobby.homeTeam || !lobby.awayTeam ||
          lobby.homeTeam.name === lobby.awayTeam.name) {
        return errorTo(socket, 'Los dos jugadores deben elegir equipos diferentes y pulsar ACEPTAR.');
      }
      room.lobbyState = null;
    }

    room.started = true;
    room.settings = payload?.settings || {};
    io.to(room.code).emit('match-start', { settings: room.settings });
  });

  // ── Controles del visitante ──
  socket.on('player-input', payload => {
    const room = getRoom(socket);
    if (!room || room.guestId !== socket.id) return;
    if (payloadSize(payload) > MAX_INPUT_BYTES) return;
    const input = payload?.input || {};

    if (!room.started) {
      if (room.mode !== 'match' || input.__flLobbyReady !== true) return;
      const team = cleanTeam(input.team);
      if (!team) return;

      const current = room.lobbyState || {
        __flLobby: true, waiting: false,
        homeTeam: null, awayTeam: null,
        hostReady: false, guestReady: false
      };
      current.awayTeam = team;
      current.guestReady = !input.cancelReady;
      room.lobbyState = current;
      io.to(room.hostId).emit('player-input', {
        input: { __flLobbyReady: true, team, cancelReady: !!input.cancelReady }
      });
      return;
    }

    io.to(room.hostId).emit('player-input', { input });
  });

  // Canal para modalidades de balón parado (host = autoridad).
  socket.on('special-mode-state', payload => {
    const room = validRoomSocket(socket, 'host');
    if (!room || !['match', 'special'].includes(room.mode)) return;
    if (payloadSize(payload) > MAX_STATE_BYTES) return;
    socket.to(room.code).volatile.emit('special-mode-state', payload);
  });
  socket.on('special-mode-input', payload => {
    const room = validRoomSocket(socket, 'guest');
    if (!room || payloadSize(payload) > MAX_INPUT_BYTES) return;
    io.to(room.hostId).emit('special-mode-input', payload);
  });

  // ── Estado de partido (host → invitado) ──
  socket.on('match-state', payload => {
    const room = getRoom(socket);
    if (!room || room.hostId !== socket.id) return;

    if (room.mode === 'match' && payload && payload.__flLobby === true && !room.started) {
      const state = cleanLobbyState(payload);
      room.lobbyState = state;
      if (room.guestId) io.to(room.guestId).emit('match-state', state);
      return;
    }

    if (!room.started || payloadSize(payload) > MAX_STATE_BYTES) return;
    socket.to(room.code).volatile.emit('match-state', payload);
  });

  socket.on('disconnect', () => leaveRoom(socket));
});

// ───────── Mantenimiento ─────────
// Borra salas abandonadas (host desconectado sin evento o sin actividad).
const cleanupTimer = setInterval(() => {
  const now = Date.now();
  for (const [code, room] of rooms) {
    const hostAlive = io.sockets.sockets.has(room.hostId);
    if (!hostAlive || now - room.lastActivity > ROOM_IDLE_MS) {
      if (room.guestId) io.to(room.guestId).emit('peer-left');
      rooms.delete(code);
    }
  }
}, 60_000);
cleanupTimer.unref();

// ───────── Arranque y cierre limpio ─────────
server.listen(PORT, '0.0.0.0', () => {
  console.log(`Football Legacy 27 online server listening on ${PORT}`);
});

function shutdown(signal) {
  console.log(`${signal} recibido, cerrando…`);
  clearInterval(cleanupTimer);
  io.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000).unref();
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('unhandledRejection', err => console.error('unhandledRejection:', err));
process.on('uncaughtException', err => console.error('uncaughtException:', err));
