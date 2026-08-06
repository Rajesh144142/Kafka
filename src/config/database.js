const { Pool } = require('pg');

const config = {
  host: process.env.DB_HOST,
  port: parseInt(process.env.DB_PORT, 10),
  database: process.env.DB_DATABASE,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
};

const pool = new Pool(config);
const userQueries = require('../queries/user.queries');

const initDb = async () => {
  await pool.query(userQueries.createUsersTable);
};

module.exports = {
  query: (text, params) => pool.query(text, params),
  initDb,
};
