exports.createPermissionsTable = `
  CREATE TABLE IF NOT EXISTS permissions (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL,
    description TEXT
  );
`;

exports.createRolePermissionsTable = `
  CREATE TABLE IF NOT EXISTS role_permissions (
    role_id INT REFERENCES roles(id) ON DELETE CASCADE,
    permission_id INT REFERENCES permissions(id) ON DELETE CASCADE,
    PRIMARY KEY (role_id, permission_id)
  );
`;

exports.insertPermission = 'INSERT INTO permissions (name, description) VALUES ($1, $2) RETURNING *';
exports.assignPermissionToRole = 'INSERT INTO role_permissions (role_id, permission_id) VALUES ($1, $2) ON CONFLICT DO NOTHING RETURNING *';
