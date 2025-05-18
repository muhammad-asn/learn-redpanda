const express = require('express');
const bodyParser = require('body-parser');
const axios = require('axios');
const path = require('path');

const app = express();
const port = 3000;

app.use(bodyParser.json());
app.use(express.static('public'));

// Serve the HTML page
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Proxy endpoint to send messages to backend
app.post('/api/send-message', async (req, res) => {
    try {
        const response = await axios.post('http://localhost:8000/send-message', {
            content: req.body.content
        });
        res.json(response.data);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Proxy endpoint to get messages from backend
app.get('/api/messages', async (req, res) => {
    try {
        const response = await axios.get('http://localhost:8000/messages');
        res.json(response.data);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.listen(port, () => {
    console.log(`Frontend server running at http://localhost:${port}`);
});
