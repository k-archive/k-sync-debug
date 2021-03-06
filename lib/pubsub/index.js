var OpStream = require('../op-stream');
var util = require('../util');

function PubSub(options) {
  this.prefix = options && options.prefix;
  this.nextStreamId = 1;
  this.streamsCount = 0;
  // Maps channel -> id -> stream
  this.streams = {};
  // State for tracking subscriptions. We track this.subscribed separately from
  // the streams, since the stream gets added synchronously, and the subscribe
  // isn't complete until the callback returns from Redis
  // Maps channel -> true
  this.subscribed = {};
}
module.exports = PubSub;

PubSub.prototype.close = function(callback) { console.log('PubSub.close')
  for (var channel in this.streams) {
    var map = this.streams[channel];
    for (var id in map) {
      map[id].destroy();
    }
  }
  if (callback) callback();
};

PubSub.prototype._subscribe = function(channel, callback) { console.log('PubSub._subscribe')
  callback(new Error('_subscribe PubSub method unimplemented'));
};

PubSub.prototype._unsubscribe = function(channel, callback) { console.log('PubSub._unsubscribe')
  callback(new Error('_unsubscribe PubSub method unimplemented'));
};

PubSub.prototype._publish = function(channels, data, callback) { console.log('PubSub._publish')
  callback(new Error('_publish PubSub method unimplemented'));
};

PubSub.prototype.subscribe = function(channel, callback) { console.log('PubSub.subscribe')
  if (this.prefix) {
    channel = this.prefix + ' ' + channel;
  }
  var pubsub = this;
  if (this.subscribed[channel]) {
    process.nextTick(function() {
      var stream = pubsub._createStream(channel);
      callback(null, stream);
    });
    return;
  }

  this._subscribe(channel, function(err) {
    if (err) return callback(err);
    pubsub.subscribed[channel] = true;
    var stream = pubsub._createStream(channel);
    callback(null, stream);
  });
};

PubSub.prototype.publish = function(channels, data, callback) { console.log('PubSub.publish')
  if (this.prefix) {
    for (var i = 0; i < channels.length; i++) {
      channels[i] = this.prefix + ' ' + channels[i];
    }
  }
  this._publish(channels, data, callback);
};

PubSub.prototype._emit = function(channel, data) { console.log('PubSub._emit')

  var channelStreams = this.streams[channel];
  if (channelStreams) {
    for (var id in channelStreams) {
      var copy = deepCopy(data);
      channelStreams[id].pushOp(copy.c, copy.d, copy);
    }
  }
};

PubSub.prototype._createStream = function(channel) { console.log('PubSub._createStream')
  var stream = new OpStream(channel);
  var pubsub = this;
  stream.once('close', function() {
    pubsub._removeStream(channel, stream);
  });

  this.streamsCount++;
  var map = this.streams[channel] || (this.streams[channel] = {});
  stream.id = this.nextStreamId++;
  map[stream.id] = stream;

  return stream;
};

PubSub.prototype._removeStream = function(channel, stream) { console.log('PubSub._removeStream')
  var map = this.streams[channel];
  if (!map) return;

  this.streamsCount--;
  delete map[stream.id];

  // Cleanup if this was the last subscribed stream for the channel
  if (util.hasKeys(map)) return;
  delete this.streams[channel];
  // Synchronously clear subscribed state. We won't actually be unsubscribed
  // until some unkown time in the future. If subscribe is called in this
  // period, we want to send a subscription message and wait for it to
  // complete before we can count on being subscribed again
  delete this.subscribed[channel];

  this._unsubscribe(channel, function(err) {
    if (err) throw err;
  });
};

function deepCopy(o) {
  var res;

  if (typeof o === 'string') {
    res = o;
  }
  // object
  else {
    res = Object.assign({}, o);

    for (var i in o) {
      if (Array.isArray(o[i])) {
        res[i] = o[i].slice(0);
      }
      else if (typeof o[i] === 'object') {
        res[i] = deepCopy(o[i]);
      }
    }
  }

  return res;
}
