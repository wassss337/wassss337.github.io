const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = 8080;

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/game.html') {
    fs.readFile(path.join(__dirname, 'game.html'), (err, data) => {
      if (err) { res.writeHead(500); res.end('game.html not found'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(data);
    });
  } else if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('OK players=' + players.size);
  } else { res.writeHead(404); res.end('Not found'); }
});

const wss = new WebSocketServer({ server, clientTracking: true });

const players = new Map();
const byName = new Map();
const byId = new Map();

function send(ws, o) { try { ws.send(JSON.stringify(o)); } catch(e){} }
function genId() { return 'p_' + Math.random().toString(36).substring(2, 10); }
function normalizeName(n) { return (n || '').trim().toLowerCase().replace(/ё/g, 'е'); }

function broadcastFriends(ws) {
  const me = players.get(ws);
  if (!me) return;
  const friends = me.friends.map(fid => {
    const fws = byId.get(fid);
    if (!fws || fws.readyState !== 1) return { id: fid, name: '?', online: false };
    const fp = players.get(fws);
    if (!fp) return { id: fid, name: '?', online: false };
    return { id: fp.id, name: fp.name, online: true, trophies: fp.trophies, inBattle: !!fp.inBattle };
  });
  send(ws, { type:'friendsList', friends });
  send(ws, { type:'friendRequests', requests: me.requests || [] });
}

function onlineList() {
  return [...players.values()].map(p => ({ id:p.id, name:p.name, trophies:p.trophies, inBattle:!!p.inBattle }));
}

setInterval(() => {
  for (const [ws, p] of players.entries()) {
    if (ws.readyState !== 1) {
      byName.delete(normalizeName(p.name));
      byId.delete(p.id);
      players.delete(ws);
    }
  }
}, 10000);

wss.on('connection', (ws) => {
  console.log('[WS] Новое соединение');
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (data) => {
    let msg; try { msg = JSON.parse(data); } catch(e) { return; }

    if (msg.type === 'hello') {
      const id = msg.id || genId();
      let name = (msg.name || 'Игрок').trim().slice(0, 16) || 'Игрок';
      if (ws._name) byName.delete(normalizeName(ws._name));
      let existing = byName.get(normalizeName(name));
      if (existing && existing !== ws) {
        let n = 2;
        let newName = name + n;
        while (byName.get(normalizeName(newName))) { n++; newName = name + n; }
        send(ws, { type:'nameTaken', suggested: newName });
        name = newName;
      }
      ws._name = name;
      const old = players.get(ws);
      const friends = old ? old.friends : (msg.friends || []);
      const requests = old ? old.requests : [];
      players.set(ws, {
        id, name, trophies: msg.trophies || 0,
        friends, requests, deck: msg.deck || [],
        inBattle: false, roomId: null
      });
      byName.set(normalizeName(name), ws);
      byId.set(id, ws);
      console.log(`[HELLO] ${name} (${id}) — онлайн: ${players.size}`);
      send(ws, { type:'welcome', id, name });
      broadcastFriends(ws);
      for (const [ows, op] of players.entries()) {
        if (ows !== ws && op.friends.includes(id)) broadcastFriends(ows);
      }
      return;
    }

    const me = players.get(ws);
    if (!me) { send(ws, { type:'error', message:'Сначала hello' }); return; }

    if (msg.type === 'addFriend') {
      const targetWs = byName.get(normalizeName(msg.name));
      if (!targetWs || targetWs.readyState !== 1) {
        console.log(`[FRIEND FAIL] ${msg.name}. Есть: ${[...byName.keys()].join(', ')}`);
        send(ws, { type:'error', message:'Игрок не в сети' });
        return;
      }
      const target = players.get(targetWs);
      if (!target) { send(ws, { type:'error', message:'Игрок не в сети' }); return; }
      if (target.id === me.id) { send(ws, { type:'error', message:'Это вы' }); return; }
      if (me.friends.includes(target.id)) { send(ws, { type:'error', message:'Уже друг' }); return; }
      target.requests = target.requests || [];
      if (!target.requests.includes(me.name)) target.requests.push(me.name);
      send(targetWs, { type:'friendRequest', from: me.name, fromId: me.id });
      send(ws, { type:'info', message:`Заявка → ${target.name}` });
      broadcastFriends(targetWs);
      console.log(`[FRIEND] ${me.name} → ${target.name}`);
      return;
    }

    if (msg.type === 'acceptFriend') {
      const fromWs = byName.get(normalizeName(msg.from));
      if (!fromWs || fromWs.readyState !== 1) { send(ws, { type:'error', message:'Игрок не в сети' }); return; }
      const from = players.get(fromWs);
      if (!from) return;
      me.friends = me.friends || [];
      from.friends = from.friends || [];
      if (!me.friends.includes(from.id)) me.friends.push(from.id);
      if (!from.friends.includes(me.id)) from.friends.push(me.id);
      me.requests = (me.requests || []).filter(n => n !== msg.from);
      send(ws, { type:'info', message:`${from.name} теперь друг!` });
      send(fromWs, { type:'info', message:`${me.name} принял заявку!` });
      broadcastFriends(ws);
      broadcastFriends(fromWs);
      return;
    }

    if (msg.type === 'declineFriend') {
      me.requests = (me.requests || []).filter(n => n !== msg.from);
      broadcastFriends(ws);
      return;
    }

    if (msg.type === 'removeFriend') {
      const targetWs = byId.get(msg.id);
      me.friends = (me.friends || []).filter(f => f !== msg.id);
      if (targetWs && targetWs.readyState === 1) {
        const t = players.get(targetWs);
        if (t) {
          t.friends = (t.friends || []).filter(f => f !== me.id);
          broadcastFriends(targetWs);
        }
      }
      broadcastFriends(ws);
      return;
    }

    if (msg.type === 'challenge') {
      const targetWs = byName.get(normalizeName(msg.name));
      if (!targetWs || targetWs.readyState !== 1) {
        console.log(`[CHALLENGE FAIL] ${msg.name}`);
        send(ws, { type:'error', message:'Игрок не в сети' });
        return;
      }
      const target = players.get(targetWs);
      if (!target) { send(ws, { type:'error', message:'Игрок не в сети' }); return; }
      if (target.id === me.id) { send(ws, { type:'error', message:'Нельзя вызвать себя' }); return; }
      if (target.inBattle) { send(ws, { type:'error', message:`${target.name} в бою` }); return; }
      if (me.inBattle) { send(ws, { type:'error', message:'Вы уже в бою' }); return; }
      me.inBattle = true;
      me.deck = msg.deck || [];
      target.inBattle = true;
      const roomId = 'r_' + Math.random().toString(36).substring(2, 10);
      me.roomId = roomId;
      target.roomId = roomId;
      ws._opponent = targetWs;
      targetWs._opponent = ws;
      ws._roomId = roomId;
      targetWs._roomId = roomId;
      send(targetWs, { type:'challengeIncoming', from: me.name, fromId: me.id, roomId });
      send(ws, { type:'challengeSent', to: target.name });
      console.log(`[CHALLENGE] ${me.name} → ${target.name} (${roomId})`);
      return;
    }

    if (msg.type === 'acceptChallenge') {
      const opp = ws._opponent;
      if (!opp || opp.readyState !== 1) { send(ws, { type:'error', message:'Вызов устарел' }); return; }
      const oppP = players.get(opp);
      if (!oppP) return;
      me.deck = msg.deck || me.deck;
      const guestSide = Math.random() < 0.5 ? 'player' : 'enemy';
      const hostSide = guestSide === 'player' ? 'enemy' : 'player';
      send(opp, { type:'start', mySide: hostSide, opponentName: me.name, opponentDeck: me.deck });
      send(ws, { type:'start', mySide: guestSide, opponentName: oppP.name, opponentDeck: oppP.deck });
      console.log(`[BATTLE START] ${oppP.name} vs ${me.name}`);
      return;
    }

    if (msg.type === 'declineChallenge') {
      const opp = ws._opponent;
      if (opp && opp.readyState === 1) {
        const oP = players.get(opp);
        if (oP) { oP.inBattle = false; oP.roomId = null; }
        send(opp, { type:'info', message:`${me.name} отклонил вызов` });
        opp._opponent = null;
      }
      me.inBattle = false;
      me.roomId = null;
      ws._opponent = null;
      return;
    }

    if (msg.type === 'move') {
      const opp = ws._opponent;
      if (!opp || opp.readyState !== 1) return;
      send(opp, { type:'opponentMove', cardId: msg.cardId, level: msg.level, x: msg.x, y: msg.y });
      return;
    }

    if (msg.type === 'battleEnd') {
      const opp = ws._opponent;
      if (msg.result === 'win') me.trophies += 30;
      else if (msg.result === 'lose') me.trophies = Math.max(0, me.trophies - 20);
      me.inBattle = false;
      me.roomId = null;
      ws._opponent = null;
      if (opp && opp.readyState === 1) {
        const oP = players.get(opp);
        if (oP) { oP.inBattle = false; oP.roomId = null; }
        send(opp, { type:'battleEnded', result: msg.result === 'win' ? 'lose' : msg.result === 'lose' ? 'win' : 'draw' });
        opp._opponent = null;
      }
      return;
    }

    if (msg.type === 'chat') {
      const opp = ws._opponent;
      if (!opp || opp.readyState !== 1) return;
      send(opp, { type:'chat', from: me.name, text: (msg.text||'').slice(0, 100) });
      return;
    }

    if (msg.type === 'onlineList') {
      send(ws, { type:'onlineList', players: onlineList() });
      return;
    }
  });

  ws.on('close', () => {
    console.log('[WS] Соединение закрыто');
    const me = players.get(ws);
    if (me) {
      byName.delete(normalizeName(me.name));
      byId.delete(me.id);
      players.delete(ws);
      for (const [ows, op] of players.entries()) {
        if (op.friends.includes(me.id)) broadcastFriends(ows);
      }
    }
    const opp = ws._opponent;
    if (opp && opp.readyState === 1) {
      const oP = players.get(opp);
      if (oP) { oP.inBattle = false; oP.roomId = null; }
      send(opp, { type:'opponentLeft' });
      opp._opponent = null;
    }
  });
});

setInterval(() => {
  wss.clients.forEach(ws => {
    if (ws.isAlive === false) { try { ws.terminate(); } catch(e){} return; }
    ws.isAlive = false;
    try { ws.ping(); } catch(e){}
  });
}, 30000);

server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n🎮 Clash Royale Mini v8 — сервер запущен`);
  console.log(`📡 Локально:  http://localhost:${PORT}`);
  console.log(`📡 По сети:   http://<ваш-IP>:${PORT}`);
  console.log(`\nОткрой игру на ВСЕХ устройствах по адресу http://<IP>:${PORT}\n`);
});