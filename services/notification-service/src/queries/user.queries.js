exports.createUsersTable = `
  CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE
  );
`;

exports.insertUser = 'INSERT INTO users (name, email) VALUES ($1, $2) RETURNING *';
exports.selectAllUsers = 'SELECT * FROM users';
