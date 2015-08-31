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

  this.client = client;
  this.key = key;
  this._defaultInitialValue = init;

  Aggregator.call(this, aggregator);
}

function init(cb) {
  this.client.get(this.key, function(err, value) {
    if (err) {
      throw new Error(err);
    } else if (value) {
      var val;
      try {
        val = JSON.parse(value);
      } catch (e) {
        val = value; //fallback to string
      }
      cb(val);
    } else {
      cb(this._defaultInitialValue);
    }
  }.bind(this));
}

function attachAggregatorFunction(aggregator) {
  if (typeof aggregator !== 'function') {
    this.aggregator(); //throw error: aggregator not implemented
  }

  if (aggregator.length === 2) {
    this.aggregator = function(value, mem, cb) {
      this.saveInRedis(aggregator(value, mem), cb);
    };
  } else {
    this.aggregator = function(value, mem, cb) {
      aggregator(mem, value, function(v) {
        this.saveInRedis(v, cb);
      }.bind(this));
    }.bind(this);
  }
}

function saveInRedis(value, callback) {
  var redis_value = typeof value === 'object' ? JSON.stringify(value) : value;
  this.client.set(this.key, redis_value, function(err) {
    if (err) {
      throw new Error(err);
    } else {
      callback(value);
    }
  });
}

RedisAggregator.prototype.init = init;
RedisAggregator.prototype.attachAggregatorFunction = attachAggregatorFunction;
RedisAggregator.prototype.saveInRedis = saveInRedis;

module.exports = RedisAggregator;
