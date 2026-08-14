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
const roleQueries = require('../queries/role.queries');
const permissionQueries = require('../queries/permission.queries');

const initDb = async () => {
  // 1. Create base tables
  await pool.query(userQueries.createUsersTable);
  await pool.query(roleQueries.createRolesTable);
  await pool.query(permissionQueries.createPermissionsTable);

  // 2. Create mapping tables with foreign keys
  await pool.query(roleQueries.createUserRolesTable);
  await pool.query(permissionQueries.createRolePermissionsTable);
};

module.exports = {
  query: (text, params) => pool.query(text, params),
  initDb,
};
