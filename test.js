var expect = require('chai').expect;
var RedisAggregator = require('./');
var redis = require('redis');

describe('Aggregator', function() {

  function lambda() {}

  function sum(value, mem) {
    return parseFloat(mem) + parseFloat(value);
  }

  var client = redis.createClient();
  var KEY = 'test-node-aggregator';

  function clean_redis(done) {
    client.del(KEY, done);
  }

  beforeEach(clean_redis);
  afterEach(clean_redis);

  it('should create an instance', function() {
    var agg = new RedisAggregator(lambda, 0, client, KEY);
    expect(agg).to.be.instanceof(RedisAggregator);
  });

  it('should throw an error if no client', function() {
    expect(function() {
      new RedisAggregator(lambda, 0, null, KEY);
    }).to.throw('RedisAggregator need a redis client');
  });

  it('should throw an error if no key', function() {
    expect(function() {
      new RedisAggregator(lambda, 0, client, null);
    }).to.throw('RedisAggregator need a redis key');
  });

  it('should throw an error if initial value is an async function', function() {
    expect(function() {
      new RedisAggregator(lambda, function(cb){
        cb(45);  
      }, client, KEY);
    }).to.throw('RedisAggregator do not support async init function');
  });

  it('should init with an sync function', function(done){
    var agg = new RedisAggregator(lambda, function(){
      return 16 + 1;
    }, client, KEY);
    agg.once('ready', function(){
      expect(agg.get()).to.be.equal(17);
      done();
    });
  });

  it('should init with redis value', function(done) {
    client.set(KEY, 42, function() {
      var agg = new RedisAggregator(lambda, 0, client, KEY);
      agg.once('ready', function() {
        expect(agg.value()).to.be.equal(42);
        done();
      });
    });
  });

  it('should init with default value because redis key was not set', function(done) {
    var agg = new RedisAggregator(lambda, 12, client, KEY);
    agg.once('ready', function() {
      expect(agg.value()).to.be.equal(12);
      done();
    });
  });

  it('should save in redis', function(done) {
    var agg = new RedisAggregator(sum, 5, client, KEY);
    agg.once('data', function() {
      expect(agg.get()).to.be.equal(6);
      client.get(KEY, function(err, val) {
        expect(val).to.be.equal('6');
        done();
      });
    });
    agg.add(1);
  });

  it('should multiple save in redis', function(done) {
    var agg = new RedisAggregator(sum, 5, client, KEY);
    var i = 0;
    agg.on('data', function() {
      i++;
      if (i === 5) {
        client.get(KEY, function(err, val) {
          expect(val).to.be.equal('10');
          done();
        });
      }
    });
    agg.add(1);
    agg.add(1);
    agg.add(1);
    agg.add(1);
    agg.add(1);
  });

  it('should aggregate a list', function(done) {
    var agg = new RedisAggregator(function(new_item, list) {
      list.push(new_item);
      return list;
    }, [], client, KEY);
    agg.add('a');
    agg.add('b');
    agg.add('c');
    var i = 0;
    agg.on('ready', function() {
      expect(agg._aggregated).to.be.deep.equal([]);
      agg.on('data', function() {
        i++;
        if (i === 3) {
          client.get(KEY, function(err, v) {
            expect(JSON.parse(v)).to.be.deep.equal(['a', 'b', 'c']);
            expect(agg.value()).to.be.deep.equal(['a', 'b', 'c']);
            done();
          });
        }
      });
    });
  });

  it('should initalize with a list from redis', function(done) {
    var agg = new RedisAggregator(function(new_item, list) {
      list.push(new_item);
      return list;
    }, [], client, KEY);
    agg.add('a');
    agg.add('b');
    agg.add('c');
    var i = 0;
    agg.on('data', function() {
      i++;
      if (i === 3) {
        var agg2 = new RedisAggregator(function(new_item, list) {
          list.push(new_item);
          return list;
        }, [], client, KEY);
        agg2.on('ready', function() {
          expect(agg2.value()).to.be.deep.equal(['a', 'b', 'c']);
          done();
        });
      }
    });
  });

});
