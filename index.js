'use strict';
var util = require('util');
var Aggregator = require('node-aggregator');

util.inherits(RedisAggregator, Aggregator);

function RedisAggregator(aggregator, init, client, key) {

  if (!client) {
    throw new Error('RedisAggregator need a redis client');
  }

  if (typeof key !== 'string') {
    throw new Error('RedisAggregator need a redis key');
  }

  if (typeof init === 'function') {
    throw new Error('RedisAggregator do not support init function');
  }

  function redisWrapper(value, mem, cb) {
    var new_value = aggregator(value, mem);
    client.set(key, new_value, function() {
      cb(new_value);
    });
  }

  function initWrapper(cb) {
    client.get(key, function(err, value) {
      cb(value || init);
    });
  }

  Aggregator.call(this, redisWrapper, initWrapper);
}


module.exports = RedisAggregator;
