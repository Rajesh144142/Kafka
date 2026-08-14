const express = require('express');
const userController = require('../controllers/user.controller');
const roleController = require('../controllers/role.controller');
const permissionController = require('../controllers/permission.controller');

const router = express.Router();

// User Routes
router.post('/users', userController.createUser);
router.get('/users', userController.getAllUsers);

// Role Routes
router.post('/roles', roleController.createRoles);
router.post('/users/assign-roles', roleController.assignRolesToUser);

// Permission Routes
router.post('/permissions', permissionController.createPermissions);
router.post('/roles/assign-permissions', permissionController.assignPermissionsToRole);

module.exports = router;
