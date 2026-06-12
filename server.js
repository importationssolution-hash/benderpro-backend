const express = require('express');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

const users = {};
const trades = {};

app.get('/', (req, res) => {
  res.json({ status: 'Bender Pro Backend actif', users: Object.keys(users).length });
});

app.post('/connect', (req, res) => {
  const { email, apiKey, secret, exchangeName } = req.body;
  if (!email || !apiKey || !secret || !exchangeName) {
    return res.json({ success: false, error: 'Donnees manquantes' });
  }
  users[email] = { apiKey, secret, exchangeName, active: true };
  if (!trades[email]) trades[email] = [];
  res.json({ success: true, message: 'Connecte avec succes !' });
});

app.get('/status/:email', (req, res) => {
  const user = users[req.params.email];
  if (!user) return res.json({ connected: false });
  res.json({ connected: true, active: user.active, exchange: user.exchangeName });
});

app.get('/trades/:email', (req, res) => {
  res.json({ trades: trades[req.params.email] || [] });
});

app.post('/toggle', (req, res) => {
  const { email, active } = req.body;
  if (!users[email]) return res.json({ success: false, error: 'Utilisateur non connecte' });
  users[email].active = active;
  res.json({ success: true, active });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log('Bender Pro Backend demarre sur port ' + PORT);
});
