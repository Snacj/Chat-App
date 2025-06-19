import express from 'express';
import { createServer } from 'node:http';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { Server } from 'socket.io';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import { availableParallelism } from 'node:os';
import cluster from 'node:cluster';
import { createAdapter, setupPrimary } from '@socket.io/cluster-adapter';

// open the database file
const db = await open({
    filename: 'chat.db',
    driver: sqlite3.Database
});

// create the messages table
await db.exec(`
    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_offset TEXT UNIQUE,
        content TEXT
    );
`);

if (cluster.isPrimary) {
    const numCPUs = availableParallelism();
    // create one worker per available core
    for (let i  = 0; i < numCPUs; i++) {
        cluster.fork({
            PORT:3000 + i
        });
    }

    setupPrimary();
} else {
    const app = express();
    const server = createServer(app);
    const io = new Server(server, {
        connectionStateRecovery: {},
        // set up the adapter on each worker thread
        adapter: createAdapter()
    });

    const port = process.env.PORT;

    const __dirname = dirname(fileURLToPath(import.meta.url));

    app.get('/', (req, res) => {
        res.sendFile(join(__dirname, 'index.html'));
    });

    app.get('/chat', (req, res) => {
        res.sendFile(join(__dirname, 'chat.html'));
    });

    io.on('connection', async (socket) => {
        console.log('a user connected');
        socket.on('chat message', async (msg, clientOffset, callback) => {
            let result;
            try {
                // store the message in the databse
                result = await db.run('INSERT INTO messages (content, client_offset) VALUES (?, ?)', msg, clientOffset);
            } catch (e) {
                if (e.errno == 19) { // SQLITE_CONSTRAINT
                    callback();
                } else {

                }
                return;
            }
            io.emit('chat message', msg, result.lastID);
            callback();
        });

         if (!socket.recovered) {
        // if the connection state recovery was not successful
        try {
          await db.each('SELECT id, content FROM messages WHERE id > ?',
            [socket.handshake.auth.serverOffset || 0],
            (_err, row) => {
              socket.emit('chat message', row.content, row.id);
            }
          )
        } catch (e) {
          // something went wrong
        }
      }
    });

    server.listen(port, () => {
        console.log(`server runnning at http://localhost:${port}`);
    })
}

