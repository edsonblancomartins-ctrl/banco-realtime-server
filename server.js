const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');
const path = require('path');

class RealtimeDatabase {
  constructor(filename = 'db.json') {
    this.filepath = path.join(__dirname, filename);
    this.data = this.loadFromFile();
  }

  loadFromFile() {
    try {
      if (fs.existsSync(this.filepath)) {
        return JSON.parse(fs.readFileSync(this.filepath, 'utf8'));
      }
    } catch (e) {
      console.error('Erro ao ler banco de dados:', e);
    }
    return {};
  }

  saveToFile() {
    try {
      fs.writeFileSync(this.filepath, JSON.stringify(this.data, null, 2));
    } catch (e) {
      console.error('Erro ao salvar dados:', e);
    }
  }

  set(pathStr, value) {
    const keys = pathStr.split('/').filter(Boolean);
    let current = this.data;

    for (let i = 0; i < keys.length - 1; i++) {
      if (!current[keys[i]] || typeof current[keys[i]] !== 'object') {
        current[keys[i]] = {};
      }
      current = current[keys[i]];
    }

    if (keys.length === 0) {
      this.data = value;
    } else {
      current[keys[keys.length - 1]] = value;
    }

    this.saveToFile();
    return value;
  }

  get(pathStr) {
    if (!pathStr || pathStr === '/') return this.data;
    const keys = pathStr.split('/').filter(Boolean);
    let current = this.data;

    for (const key of keys) {
      if (current && typeof current === 'object' && key in current) {
        current = current[key];
      } else {
        return undefined;
      }
    }
    return current;
  }
}

const db = new RealtimeDatabase();

// Servidor HTTP necessário para hospedagem em nuvem (Render, Railway, etc.)
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Realtime Database Server is running!\n');
});

const wss = new WebSocket.Server({ server });
const PORT = process.env.PORT || 8080;

server.listen(PORT, () => {
  console.log(`🚀 Servidor rodando na porta ${PORT}`);
});

wss.on('connection', (ws) => {
  console.log('📱 Novo cliente conectado!');
  ws.subscriptions = new Set();

  ws.on('message', (message) => {
    try {
      const msg = JSON.parse(message);

      if (msg.action === 'set') {
        db.set(msg.path, msg.value);
        console.log(`[SET] ${msg.path} =`, msg.value);

        wss.clients.forEach((client) => {
          if (client.readyState === WebSocket.OPEN && client.subscriptions.has(msg.path)) {
            client.send(JSON.stringify({
              event: 'update',
              path: msg.path,
              value: msg.value
            }));
          }
        });
      }
      else if (msg.action === 'get') {
        const value = db.get(msg.path);
        ws.send(JSON.stringify({ event: 'result', path: msg.path, value }));
      }
      else if (msg.action === 'subscribe') {
        ws.subscriptions.add(msg.path);
        const value = db.get(msg.path);
        ws.send(JSON.stringify({ event: 'update', path: msg.path, value }));
        console.log(`[SUBSCRIBE] Cliente inscrito em: ${msg.path}`);
      }
    } catch (e) {
      ws.send(JSON.stringify({ error: 'Mensagem inválida' }));
    }
  });

  ws.on('close', () => {
    console.log('📱 Cliente desconectado.');
  });
});
