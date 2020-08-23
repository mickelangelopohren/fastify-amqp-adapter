const fp = require('fastify-plugin')
const amqpcm = require('amqp-connection-manager')

function fastifyAmqpConnectionManager (fastify, opts, next) {
  const {
    host = 'localhost',
    port = 5672,
    user = 'guest',
    pass = 'guest',
    queueName = 'test',
    memoryMsgPoolSize = '10',
  } = opts;

  const amqpBaseUrl = `amqp://${user}:${pass}@${host}:${port}`;

  const connection = amqpcm.connect([amqpBaseUrl]);

  // TODO queue receive options by parameters
  const channelWrapper = connection.createChannel({
    // json: true,
    setup: function(channel) {
      return channel.assertQueue(queueName, { durable: true });
    }
  });

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

  // TODO receive queue options by parameters
  const sendToQueue = (message) => {
    if (channelWrapper.queueLength() >= memoryMsgPoolSize) {
      // Trick to clear unsended messages stored in memory because
      // amqp-connection-manager actualy has no way to clear stored messages
      channelWrapper._messages = [];
      channelWrapper.close();
    }

    const bufferedMsg = getBuffer(message);

    // TODO Adjust logs
    return channelWrapper.sendToQueue(queueName, bufferedMsg, { persistent: true })
      .then(function() {
        return console.log("Message was sent ");
      }).catch(function(err) {
        return console.log("Message was rejected");
      });
  }

  fastify.decorate('amqpChannelWrapper', channelWrapper)
  fastify.decorate('amqpSendToQueue', sendToQueue)

  next()
}

module.exports = fp(fastifyAmqpConnectionManager, {
  fastify: '>=1.0.0',
  name: 'fastify-amqp-connection-manager'
})
