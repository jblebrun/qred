var assert = require("assert");
var Domain = require("domain");

var Qred = require("../index");

try {
    var redis = require("redis");
} catch(err) {
    console.log("npm install redis to run tests");
    process.exit(-1);
}

try {
var client = redis.createClient(6480);
var sclient = redis.createClient(6480);
} catch(err) {
    console.log("Run a redis server on port 6480");
    process.exit(-1);
}

var default_timeout = 5000;

var tests = [ 
    {
        test: function singleJob(done) {
            var params = {
                log: console.log.bind(console),
                name: "simpletest",
                handler: function(data, callback) {
                    callback(null, JSON.stringify(data));
                }
            };
            var q = new Qred.Manager(client, sclient, params);
            new Qred.Processor(client, sclient, params);

            var data = { data1: "a", data2: "b" };
            q.submitJob("ajobid", data , {}, function(err, result) {
                assert(!err, err);
                assert(result == JSON.stringify(data));
                done();
            });
        },
        timeout: 2000
    },
    function priorityJobs(done) {
        var params = {
            log: console.log.bind(console),
            name: "priotest",
            conurrency: 1,
            handler: function(data, callback) {
                setTimeout(function() {
                    callback(null, JSON.stringify(data));
                }, 500);
            }
        };
        var q = new Qred.Manager(client, sclient, params);
        var qp = new Qred.Processor(client, sclient, params);

        var adone = false;
        var bdone = false;
        var cdone = false;
        qp.pause();
        var adata = {info:"A"};
        q.submitJob("ajobid", adata, { priority: 1 }, function(err, result) {
            assert(!err, err);
            assert(cdone);
            adone = true;
            assert(result == JSON.stringify(adata));
            if(adone && bdone && cdone) done();
        });
        var bdata = {info:"B"};
        q.submitJob("bjobid", bdata , { priority: 1 }, function(err, result) {
            assert(!err, err);
            assert(cdone);
            bdone = true;
            assert(result == JSON.stringify(bdata));
            if(adone && bdone && cdone) done();
        });
        var cdata = {info:"C"};
        q.submitJob("cjobid", cdata , { priority: -1 }, function(err, result) {
            assert(!err, err);
            assert(!adone);
            assert(!bdone);
            cdone = true;
            assert(result == JSON.stringify(cdata));
            if(adone && bdone && cdone) done();
        });
        qp.unpause();
    },
    function concurrencyTest(done) {
        var active = 0;
        var started = {};
        var fin = {};
        var params = {
            log: console.log.bind(console),
            name: "concurrencytest",
            conurrency: 2,
            handler: function(data, callback) {
                started[data.info] = true;
                active++;
                assert(active <= 2, "active jobs: "+active);
                setTimeout(function() {
                    active--;
                    callback(null, JSON.stringify(data));
                }, 500);
            }
        };
        var q = new Qred.Manager(client, sclient, params);
        var qp = new Qred.Processor(client, sclient, params);
        qp.pause();
        var adata = {info:"A"};
        q.submitJob("ajobid", adata, { priority: 1 }, function(err, result) {
            fin.A = true;
            assert(!err, err);
            assert(started.C && started.E);
            assert(result == JSON.stringify(adata));
            if(fin.A && fin.B && fin.C && fin.D && fin.E) done();
        });
        var bdata = {info:"B"};
        q.submitJob("bjobid", bdata , { priority: 1 }, function(err, result) {
            assert(!err, err);
            fin.B = true;
            assert(started.C && started.E);
            assert(result == JSON.stringify(bdata));
            if(fin.A && fin.B && fin.C && fin.D && fin.E) done();
        });
        var cdata = {info:"C"};
        q.submitJob("cjobid", cdata , { priority: -1 }, function(err, result) {
            assert(!err, err);
            fin.C = true;
            assert(result == JSON.stringify(cdata));
        });
        var ddata = {info:"D"};
        q.submitJob("djobid", ddata , { priority: 1 }, function(err, result) {
            assert(!err, err);
            fin.D = true;
            assert(result == JSON.stringify(ddata));
            if(fin.A && fin.B && fin.C && fin.D && fin.E) done();
        });
        var edata = {info:"E"};
        q.submitJob("ejobid", edata , { priority: -1 }, function(err, result) {
            assert(!err, err);
            fin.E = true;
            assert(result == JSON.stringify(edata));
        });
        qp.unpause();
    },
    function delayJobs(done) {
        var params = {
            log: console.log.bind(console),
            name: "delaytest",
            conurrency: 1,
            handler: function(data, callback) {
                callback(null, JSON.stringify(data));
            }
        };
        var q = new Qred.Manager(client, sclient, params);
        new Qred.Processor(client, sclient, params);

        var start = Date.now();
        var data = {info:"delay"};
        q.submitJob("ajobid", data, { priority: 1, delay: 500 }, function(err, result) {
            assert(!err, err);
            assert(Date.now() > start + 500);
            assert(result == JSON.stringify(data));
            done();
        });
    },
    function attachToJob(done) {
        var handlerruns = 0;
        var params =  {
            log: console.log.bind(console),
            name: "delaytest",
            conurrency: 1,
            handler: function(data, callback) {
                handlerruns++;
                callback(null, JSON.stringify(data));
            }
        };
        var q = new Qred.Manager(client, sclient, params);
        var qp = new Qred.Processor(client, sclient, params);
        var data = {info:"attach"};
        var callbacks = 0;
        qp.pause();
        q.submitJob("ajobid", data, { }, function(err, result) {
            assert(!err, err);
            assert(handlerruns === 1);
            assert(result == JSON.stringify(data));
            callbacks++;
            if(callbacks >= 2) done();
        });
        q.submitJob("ajobid", data, { }, function(err, result) {
            assert(!err, err);
            assert(handlerruns === 1);
            assert(result == JSON.stringify(data));
            callbacks++;
            if(callbacks >= 2) done();
        });
        qp.unpause();
    }


];


//Run in domain to catch asserts
function runNextTest() {
    var tdomain = Domain.create();
    tdomain.add(client);
    tdomain.add(sclient);
    var test = tests.shift();
    if(!test) {
        console.log("Finished tests");
        process.exit(0);
    }
    tdomain.on('error', function(err) {
        console.log("*** Test "+test.name+" failed");
        console.log(err.message);
        console.log(err.stack);
        runNextTest();
    });
    tdomain.run(function() {
        var timeout = default_timeout;
        if(test instanceof Function) {
            test = test; 
        } else if (test instanceof Object && test.hasOwnProperty('test')) {
            if(test.hasOwnProperty("timeout")) {
                timeout = test.timeout; 
            }
            test = test.test;
        } else {
            assert('Test entries should be functions, or objects with a field called "test" that is a function');
        }
        console.log("--- Starting test "+test.name+(timeout?" (Times out in "+timeout+")":""));

        var timeout_id;
        test(function() {
            clearTimeout(timeout_id);
            console.log("    Test \""+test.name+"\" passed");
            process.nextTick(runNextTest);
        });
        timeout_id = setTimeout(function() {
            assert(false, test.name+" timed out!");
        }, timeout);
        tdomain.add(timeout_id);
    });
}

runNextTest();
