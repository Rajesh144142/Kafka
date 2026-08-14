exports.createRolesTable = `
  CREATE TABLE IF NOT EXISTS roles (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL,
    description TEXT
  );
`;

exports.createUserRolesTable = `
  CREATE TABLE IF NOT EXISTS user_roles (
    user_id INT REFERENCES users(id) ON DELETE CASCADE,
    role_id INT REFERENCES roles(id) ON DELETE CASCADE,
    PRIMARY KEY (user_id, role_id)
  );
`;

exports.insertRole = 'INSERT INTO roles (name, description) VALUES ($1, $2) RETURNING *';
exports.assignRoleToUser = 'INSERT INTO user_roles (user_id, role_id) VALUES ($1, $2) ON CONFLICT DO NOTHING RETURNING *';
