const fp = require('fastify-plugin')
const amqpcm = require('amqp-connection-manager')

function fastifyAmqpConnectionManager (fastify, opts, next) {
  const {
    host = 'localhost',
    port = 5672,
    user = 'guest',
    pass = 'guest',
    defaultQueueName = 'test',
  } = opts;

  let channel;

  const amqpBaseUrl = `amqp://${user}:${pass}@${host}:${port}`;

  const connection = amqpcm.connect([amqpBaseUrl]);
  connection.on('connect', () => {
    return console.log(`AMQP server '${host}' connected`)
  });
  connection.on('disconnect', (err) => {
    return console.log(`AMQP server '${host}' disconnected`, err.err)
  });

  const channelWrapper = (queueName) => {
    return connection.createChannel({
      // json: true,
      setup: (channel) => {
        return channel.assertQueue(queueName, { durable: true });
      }
    })
  };

  const getBuffer = (message) => {
    switch (typeof message) {
      case 'string':
        return Buffer.from(message);
      case 'object':
        return Buffer.from(JSON.stringify(message));
      default:
        return Buffer.from('');
    }
  }

  const sendToQueue = (message, queue = null) => {
    if (!connection.isConnected()) {
      return console.log('AMQP server disconected -> Message not sent');
    }

    const queueName = queue || defaultQueueName;

    if (!channel) {
      channel =  channelWrapper(queueName);
    }

    const bufferedMsg = getBuffer(message);

    return channel.sendToQueue(queueName, bufferedMsg, { persistent: true })
      .then(() =>  {
        console.log('Message was sent');
      }).catch((err) => {
        channel.close();
        connection.close();
        console.log('Message was rejected');
      });
  }

  const consumeQueue = (onMessageCB, queue = null) => {
    const queueName = queue || defaultQueueName;

    const subscriberChannelWrapper = connection.createChannel({
      setup: (channel) =>  {
        return Promise.all([
          channel.assertQueue(queueName, { durable: true }),
          channel.prefetch(1),
          channel.consume(queueName, onMessage)
        ]);
      }
    });

    async function onMessage(data) {
      return onMessageCB(data)
        .then(() => {
          subscriberChannelWrapper.ack(data);
        })
        .catch((err) =>  {
          console.error(err)
        });
    }
  }

  fastify.decorate('amqpSendToQueue', sendToQueue)
  fastify.decorate('amqpConsumeQueue', consumeQueue)

  next()
}

module.exports = fp(fastifyAmqpConnectionManager, {
  fastify: '>=1.0.0',
  name: 'fastify-amqp-connection-manager'
})
