const db = require('../config/database');
const userQueries = require('../queries/user.queries');

exports.save = async (userData) => {
  const result = await db.query(userQueries.insertUser, [userData.name, userData.email]);
  return result.rows[0];
};

exports.findAll = async () => {
  const result = await db.query(userQueries.selectAllUsers);
  return result.rows;
};

exports.findAllWithRoles = async () => {
  const result = await db.query(userQueries.selectAllUsersWithRoles);
  return result.rows;
};
