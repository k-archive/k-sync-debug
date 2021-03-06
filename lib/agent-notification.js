var Agent = require('./agent'),
    util = require('./util'),
    QueryEmitter = require('./query-emitter');

Agent.prototype._notificationSubscribe = function(req, callback){ console.log('Agent._notificationSubscribe')
  var collection = req.c,
      id = req.q.$i,
      queryId = req.id,
      options = {},
      agent = this;

  if (req.o && req.o.deleteAfterOneSubmit) {
    return callback(null, {});
  }

  var previous = this.getExistingEmitter(queryId, options);
  if (previous) options.emitter = previous;

  this.backend.notificationSubscribe(this, collection, id, options, function(err, emitter, results) {
    if (err) return callback(err);

    agent._subscribeToNotification(emitter, queryId, collection, id);

    var message = {
      data: results
    };

    callback(null, message);
  });
};

Agent.prototype._notificationUnsubscribe = function(req, callback){ console.log('Agent._notificationUnsubscribe')
  var emitter = this.subscribedNotifications[req.id];
  if (emitter) {
    emitter.destroy();
    delete this.subscribedNotifications[req.id];
  }
  process.nextTick(callback);
};

Agent.prototype._notificationFetch = function(req, callback){ console.log('Agent._notificationFetch')
  var collection = req.c,
      id = req.q.$i,
      queryId = req.id;

  var message = {
    data: []
  };

  callback(null, message);
};

Agent.prototype._subscribeToNotification = function(emitter, queryId, collection, id){ console.log('Agent._subscribeToNotification')
  if (this.closed) return emitter.destroy();

  var previous = this.subscribedNotifications[queryId];
  if (previous) previous.destroy();
  var agent = this;

  emitter.onDiff = function(diff) {
    // Consider stripping the collection out of the data we send here
    // if it matches the query's collection.
    agent.send({a: 'nop', id: queryId, diff: diff});
  };

  emitter.onError = function(err) {
    // Log then silently ignore errors in a subscription stream, since these
    // may not be the client's fault, and they were not the result of a
    // direct request by the client
    console.error('Notification subscription stream error', collection, id, err);
  };

  this.subscribedNotifications[queryId] = emitter;
};

Agent.prototype._submitNop = function(req, callback) { console.log('Agent._submitNop')
  var op = new NotificationOp(req);
  var agent = this;
  this.backend.submitNop(this, op, function(err, ops) {
    // Message to acknowledge the op was successfully submitted
    var ack = { src: op.src, seq: op.seq };
    if (err) {
      // Occassional 'Op already submitted' errors are expected to happen as
      // part of normal operation, since inflight ops need to be resent after
      // disconnect. In this case, ack the op so the client can proceed
      if (err.code === 4001) return callback(null, ack);
      return callback(err);
    }

    callback(null, ack);
  });
};

function NotificationOp(req) {
  this.type = 'nop';
  this.c = req.c;
  this.d = req.index;
  this.data = req.data;
  this.seq = req.seq;
  this.src = req.src;
}
