const express = require('express');
const bodyParser = require('body-parser');
const userRoutes = require('./routes/user.routes');

const app = express();
app.use(bodyParser.json());
app.use('/api', userRoutes);

app.get('/', (req, res) => {
  res.send('Kafka Learning API');
});

module.exports = app;
