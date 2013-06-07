var assert = require("assert");
var harness = require("./harness");
var Qred = require("../index");

var redis = harness.getClient();
var tests = [{
        test: function reconciler_liveset(done) {
            var sredis = harness.getClient();
            var params = {
                redis: redis,
                subscriber: sredis,
                log: console.log.bind(console),
                name: "reconcile-liveset"
            };
            var q = new Qred.Manager(params); 
            redis.zadd(q.livesetkey, 444, 'stalejobid', function(err) {
                assert(!err, err);
                q.reconcile(function(err, result) {
                    assert(!err, err);
                    assert(result);
                    assert(result.liveSet instanceof Array);
                    assert(result.liveSet.length === 1);
                    assert(result.liveSet[0] === "stalejobid");
                    redis.zscore(q.livesetkey, 'stalejobid', function(err, score) {
                        assert(!err, err);
                        assert(score === null);
                        done();
                    });
                });
            });
        }
    },{
        test: function reconciler_delayprios(done) {
            var sredis = harness.getClient();
            var params = {
                redis: redis,
                subscriber: sredis,
                log: console.log.bind(console),
                name: "reconcile-delayprios"
            };
            var q = new Qred.Manager(params); 
            redis.hset(q.delayprioritieskey, 'stalejobid', '0', function(err) {
                assert(!err, err);
                q.reconcile(function(err, result) {
                    assert(!err, err);
                    assert(result);
                    assert(result.delayPriorities instanceof Array);
                    assert(result.delayPriorities.length === 1);
                    assert(result.delayPriorities[0] === "stalejobid");
                    redis.zscore(q.delayprioritieskey, 'stalejobid', function(err, score) {
                        assert(!err, err);
                        assert(score === null);
                        done();
                    });
                });
            });
        }
}];

function beforeeach(done) {
    redis.flushall(function(err) { assert(!err); done(); });
}

harness.go(tests, beforeeach, process.exit);

