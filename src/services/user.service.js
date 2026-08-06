const userRepository = require('../repositories/user.repository');
const { produceMessage } = require('../kafka/producer');
const { USER_SIGNUPS } = require('../kafka/topic');

exports.createUser = async (userData) => {
  const user = await userRepository.save(userData);
  await produceMessage(USER_SIGNUPS, { event: 'USER_CREATED', payload: user });
  return user;
};
