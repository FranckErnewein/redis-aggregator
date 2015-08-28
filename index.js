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

  this._objectMode = false;

  function redisWrapper(value, mem, cb) {
    var new_value = aggregator(value, mem);
    var redis_value = new_value;
    if (typeof new_value === 'object') {
      redis_value = JSON.stringify(new_value);
    }
    client.set(key, redis_value, function() {
      cb(new_value);
    });
  }

  function initWrapper(cb) {
    client.get(key, function(err, value) {
      if(value){
        var val;
        try{
          val = JSON.parse(value);
        }catch(e){
          val = value; //fallback to string
        }
        cb(val);
      }else{
        cb(init);
      }
    });
  }

  Aggregator.call(this, redisWrapper, initWrapper);
}


module.exports = RedisAggregator;
