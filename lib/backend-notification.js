var NotificationEmitter = require('./notification-emitter'),
    Backend = require('./backend'),
    util = require('./util'),
    ot = require('./ot');

Backend.prototype.notificationSubscribe = function(agent, collection, id, options, callback){ console.log('Backend.notificationSubscribe')
  var channel = this.getDocChannel(collection, id),
      backend = this,
      emitter;

  var request = {
    backend: backend,
    collection: collection,
    id: id,
    channel: channel,
    db: backend.db
  };

  backend.pubsub.subscribe(channel, function(err, stream) {
    if (err) return callback(err);

    stream.agent = agent;

    if (options.emitter) {
      emitter = options.emitter;
      emitter.updateQuery(query);
    }
    else {
      emitter = new NotificationEmitter(request, agent, backend, stream);
    }

    callback(null, emitter, emitter.data);
  });
};

Backend.prototype.submitNop = function(agent, op, callback){ console.log('Backend.submitNop')
  var request = new SubmitNopRequest(this, agent, op);
  var backend = this;

  request.run(function(err) {
    callback(err);
  });
};

function SubmitNopRequest(backend, agent, op) {
  this.op = op;
  this.backend = backend;
  this.agent = agent;
  this.channels = [backend.getDocChannel(op.c, op.d)];
}

SubmitNopRequest.prototype.run = function(callback){ console.log('SubmitNopRequest.run')
  var op = this.op;

  if (!op) return callback();

  var request = this;
  var backend = this.backend;

  backend.pubsub.publish(request.channels, op, callback);
};
