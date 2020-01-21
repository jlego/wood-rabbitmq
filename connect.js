/**
 * Wood Plugin Module.
 * rabbitMq操作库
 * by guohualin on 2019-05-13
 */
const RabbitMq = require('./src/rabbit');

module.exports = async (app = {}, config = {}) => {
  await RabbitMq.connect(config);
  return app;
}
