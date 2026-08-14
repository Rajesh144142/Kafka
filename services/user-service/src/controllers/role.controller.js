const db = require('../config/database');
const roleQueries = require('../queries/role.queries');

exports.createRoles = async (req, res) => {
  try {
    const { roles } = req.body;
    if (!roles || !Array.isArray(roles)) {
      return res.status(400).json({ error: 'Request body must contain a "roles" array.' });
    }

    const createdRoles = [];
    for (const role of roles) {
      const result = await db.query(roleQueries.insertRole, [role.name, role.description || null]);
      createdRoles.push(result.rows[0]);
    }

    res.status(201).json(createdRoles);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

exports.assignRolesToUser = async (req, res) => {
  try {
    const { userId, roles } = req.body;
    if (!userId || !roles || !Array.isArray(roles)) {
      return res.status(400).json({ error: 'Request body must contain "userId" and a "roles" array.' });
    }

    // 1. Clear current roles for this user
    await db.query(roleQueries.deleteUserRoles, [userId]);

    // 2. Loop and assign new roles
    const assigned = [];
    for (const roleName of roles) {
      // Find role ID by name
      const roleResult = await db.query(roleQueries.findRoleByName, [roleName]);
      if (roleResult.rows.length > 0) {
        const roleId = roleResult.rows[0].id;
        await db.query(roleQueries.assignRoleToUser, [userId, roleId]);
        assigned.push(roleName);
      }
    }

    res.status(200).json({ message: 'Roles successfully assigned to user.', assigned });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};
