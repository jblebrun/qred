
var assert = require("assert");
var harness = require("./harness");
var Qred = require("../index");
var checkerr = function(err) {
    assert(!err, err);
};

var tests = [
    function emitterCrossTalk(done) {
        var params1 = {
            redis: harness.getClient(),
            subscriber: harness.getClient(),
            log: console.log.bind(console),
            name: "crosstalkq1",
            handler: function(data, callback) {
                callback(null, JSON.stringify(data));
            }
        };
        var params2 = {
            redis: harness.getClient(),
            subscriber: harness.getClient(),
            log: console.log.bind(console),
            name: "crosstalkq2",
            handler: function(data, callback) {
                callback(null, JSON.stringify(data));
            }
        };
        var q1 = new Qred.Manager(params1);
        new Qred.Processor(params1);
        q1.submitJob('ajobq1', {}, {}, checkerr);
        var calledq1 = false;
        var checkdone = function() {
            if(calledq1 && calledq2) {
                done();
            }
        };
        q1.on('complete', function(data) {
            console.log("Called q1 complete for "+JSON.stringify(data));
            assert(!calledq1);
            calledq1 = true;
            setTimeout(checkdone, 10);
        });
        var q2 = new Qred.Manager(params2);
        var calledq2;
        q2.on('complete', function(data) {
            console.log("Called q2 complete for "+JSON.stringify(data));
            assert(!calledq2);
            calledq2 = true;
            setTimeout(checkdone, 10);
        });
        q2.submitJob('ajobq2', {}, {}, checkerr);
        new Qred.Processor(params2);
    }
];

function beforeeach(done) {
    harness.getClient().flushall(function(err) { assert(!err); done(); });
}

harness.go(tests, beforeeach, function(err) {
    process.exit(!!err);
});

