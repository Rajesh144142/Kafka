const db = require('../config/database');
const permissionQueries = require('../queries/permission.queries');
const roleQueries = require('../queries/role.queries');

exports.createPermissions = async (req, res) => {
  try {
    const { permissions } = req.body;
    if (!permissions || !Array.isArray(permissions)) {
      return res.status(400).json({ error: 'Request body must contain a "permissions" array.' });
    }

    const createdPermissions = [];
    for (const permission of permissions) {
      const result = await db.query(permissionQueries.insertPermission, [permission.name, permission.description || null]);
      createdPermissions.push(result.rows[0]);
    }

    res.status(201).json(createdPermissions);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

exports.assignPermissionsToRole = async (req, res) => {
  try {
    const { roleName, permissions } = req.body;
    if (!roleName || !permissions || !Array.isArray(permissions)) {
      return res.status(400).json({ error: 'Request body must contain "roleName" and a "permissions" array.' });
    }

    // Find role ID
    const roleResult = await db.query(roleQueries.findRoleByName, [roleName]);
    if (roleResult.rows.length === 0) {
      return res.status(404).json({ error: `Role "${roleName}" not found.` });
    }
    const roleId = roleResult.rows[0].id;

    // 1. Clear current permissions for this role
    await db.query(permissionQueries.deleteRolePermissions, [roleId]);

    // 2. Loop and assign new permissions
    const assigned = [];
    for (const permName of permissions) {
      // Find permission ID by name
      const permResult = await db.query(permissionQueries.findPermissionByName, [permName]);
      if (permResult.rows.length > 0) {
        const permId = permResult.rows[0].id;
        await db.query(permissionQueries.assignPermissionToRole, [roleId, permId]);
        assigned.push(permName);
      }
    }

    res.status(200).json({ message: 'Permissions successfully assigned to role.', role: roleName, assigned });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};
