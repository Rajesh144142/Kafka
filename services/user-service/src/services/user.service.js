const userRepository = require('../repositories/user.repository');
const { produceMessage } = require('../config/kafka');
const { USER_SIGNUPS } = require('../../../../shared/kafka/topic');

exports.createUser = async (userData) => {
  const user = await userRepository.save(userData);
  await produceMessage(USER_SIGNUPS, { event: 'USER_CREATED', payload: user }, user.id);
  return user;
};

exports.getAllUsersWithRoles = async () => {
  return await userRepository.findAllWithRoles();
};
