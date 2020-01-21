const amqp = require('amqplib');
const crypto = require('crypto');
const { Log, Redis } = WOOD;
let confirmChannels = []; //confirm模式channel连接池
let channels = []; //普通模式channel连接池
let connect = null;
const FINISHED = 2;
const UNFINISHED = 1;
const QOS = 10;

class RabbitMq {
  constructor() {
    this.redis = new Redis("RabbitMq");
  }

  async setCache(key, content, ttl = 60 * 60 * 24 * 7) {
    try {
      await this.redis.setValue(key, content, ttl);
    } catch (e) {
      Log.error(e);
    }
  }

  async waitForConfirms(channel) {
    try {
      await channel.waitForConfirms();
    } catch (e) {
      Log.error(`RabbitMq waitForConfirms error: ${e}`);
    }
  }

  // 配合redis记录是否消费
  async sendToQueueT(queue, content, options = { durable: true }) {
    const channel = await this.getConfirmChannel();
    let msg = JSON.stringify(content);
    let self = this;
    channel.sendToQueue(queue, Buffer.from(msg), options, async function (err) {
      if (err) {
        Log.error('RabbitMq sendToQueue failed: ' + err);
      } else {
        const id = crypto.createHash('md5').update(msg).digest('hex') + Date.now();
        await self.setCache(id, JSON.stringify({ status: UNFINISHED }))
      }
    })
    await this.waitForConfirms(channel);
    this.releaseConfirmChannel(channel);
  }

  // 无redis记录是否消费
  async sendToQueue(queue, content, options = { durable: true }) {
    const channel = await this.getConfirmChannel();
    let msg = JSON.stringify(content);
    channel.sendToQueue(queue, Buffer.from(msg), options, async function (err) {
      if (err) {
        Log.error('RabbitMq sendToQueue failed: ' + err);
      }
    })
    await this.waitForConfirms(channel);
    this.releaseConfirmChannel(channel);
  }

  // 根据redis判断是否已消费
  async consumeT(queue, fun) {
    const channel = await this.getChannel();
    channel.consume(queue, async function (msg) {
      const bodyStr = msg.content.toString();
      const body = JSON.parse(bodyStr);
      const value = await this.redis.getValue(body.id);
      const task = JSON.parse(value);
      if (task.status !== FINISHED) {
        try {
          await fun(msg);
        } catch (e) {
          channel.nack(msg);
          Log.error(`RabbitMq consume error: ${JSON.stringify(e)}, msg: ${JSON.stringify(msg)}`);
          await this.setCache(body.id, JSON.stringify({ status: UNFINISHED }));
        }
        channel.ack(msg);
        await this.setCache(body.id, JSON.stringify({ status: FINISHED }));
      }
      channel.ack(msg);
    }, {})
    this.releaseChannel(channel);
  }

  // 存在幂等性问题，可能重复消费
  async consusme(queue, fun) {
    const channel = await this.getChannel();
    channel.consume(queue, async function (msg) {
      await fun(msg);
      channel.ack(msg);
    }, {})
    this.releaseChannel(channel);
  }

  static async connect(config) {
    try {
      if (!connect) {
        connect = await amqp.connect(config);
      }
    } catch (e) {
      Log.error(e);
    }
  }

  async getChannel() {
    if (!channels.length) {
      const channel = await connect.createChannel();
      channel.prefetch(QOS);
      channels.push(channel);
    }
    return channels.pop();
  }

  releaseChannel(channel) {
    channels.push(channel);
  }

  async getConfirmChannel() {
    if (!confirmChannels.length) {
      const channel = await connect.createConfirmChannel();
      confirmChannels.push(channel);
    }
    return confirmChannels.pop();
  }

  releaseConfirmChannel(channel) {
    confirmChannels.push(channel);
  }

  async setExchangeBindQueue(ex, type, routeConfigs) {
    const ch = await this.getChannel();
    await ch.assertExchange(ex, type, { durable: true });
    let queueList = [];
    for (let config of routeConfigs) {
      let { routes, queues } = config;
      if (!Array.isArray(queues) || !queues.length) queues = [""];  
      if (!Array.isArray(routes) || !routes.length) throw new Error('need routes');
      let assertQueues = await Promise.all(queues.map(v => ch.assertQueue(v || "", { durable: true })));
      for (let i of assertQueues) {
        const { queue } = i;
        await Promise.all(routes.map(route => {
          ch.bindQueue(queue, ex, route);
        }))
        queueList.push(queue);
      }
    }
    this.releaseChannel(ch);
    return { routeConfigs, queueList };
  }

  async publishToExchange(ex, type, key, msg, options) {
    const ch = await this.getChannel();
    await ch.assertExchange(ex, type, {durable: true});
    ch.publish(ex, key, Buffer.from(msg), options);
    this.releaseChannel(ch);
  }
}

module.exports = RabbitMq;