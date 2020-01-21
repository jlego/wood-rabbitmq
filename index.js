/**
 * Wood Plugin Module.
 * rabbitmq操作库
 * by guohualin on 2019-05-13
 */
const RabbitMq = require('./src/rabbit');

module.exports = (app = {}, config = {}) => {
  app.RabbitMq = RabbitMq;
  if(app.addAppProp) app.addAppProp('RabbitMq', app.RabbitMq);
  return app;
}
