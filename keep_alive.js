const express = require('express');
const app = express();
const port = 3000;

// Simple ping endpoint to keep the bot alive
app.get('/', (req, res) => {
  res.send('K-pop Roleplay Bot is running! 🎵');
});

app.get('/ping', (req, res) => {
  res.json({ 
    status: 'alive', 
    timestamp: new Date().toISOString(),
    uptime: process.uptime()
  });
});

app.listen(port, () => {
  console.log(`Keep-alive server running on port ${port}`);
});

module.exports = app;