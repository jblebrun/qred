var assert = require("assert");
var Domain = require("domain");

var Qred = require("../index");

try {
    var redis = require("redis");
} catch(err) {
    console.log("npm install redis to run tests");
    process.exit(-1);
}

function getClient() {
    var client;
    try {
        client = redis.createClient(6479);
    } catch(err) {
        console.log("Run a redis server on port 6479");
        process.exit(-1);
    }
    return client;
}

var default_timeout = 5000;

function checkerr(err) {
    assert(!err, err);
}

var tests = [ 
    {
        test: function singleJob(done) {
            var params = {
                redis: getClient(),
                subscriber: getClient(),
                log: console.log.bind(console),
                name: "simpletest",
                handler: function(job, callback) {
                    callback(null, JSON.stringify(job.data));
                }
            };
            var q = new Qred.Manager(params);
            new Qred.Processor(params);
            var data = { data1: "a", data2: "b" };
            q.submitJob("ajobid", data , {}, checkerr, function(err, result) {
                assert(!err, err);
                assert(result == JSON.stringify(data));
                done();
            });
        },
        timeout: 5000
    },
    function priorityJobs(done) {
        var params = {
            redis: getClient(),
            subscriber: getClient(),
            log: console.log.bind(console),
            name: "priotest",
            conurrency: 1,
            handler: function(job, callback) {
                assert(job.priority = job.data.p);
                setTimeout(function() {
                    callback(null, JSON.stringify(job.data));
                }, 500);
            }
        };
        var q = new Qred.Manager(params);
        var qp = new Qred.Processor(params);

        var adone = false;
        var bdone = false;
        var cdone = false;
        qp.pause();
        var adata = {info:"1",p:1};
        q.submitJob("ajobid", adata, { priority: 1 }, checkerr, function(err, result) {
            assert(!err, err);
            assert(cdone);
            adone = true;
            assert(result == JSON.stringify(adata));
            if(adone && bdone && cdone) done();
        });
        var bdata = {info:"B",p:1};
        q.submitJob("bjobid", bdata , { priority: 1 }, checkerr, function(err, result) {
            assert(!err, err);
            assert(cdone);
            bdone = true;
            assert(result == JSON.stringify(bdata));
            if(adone && bdone && cdone) done();
        });
        var cdata = {info:"C",p:-1};
        q.submitJob("cjobid", cdata , { priority: -1 }, checkerr, function(err, result) {
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
            redis: getClient(),
            subscriber: getClient(),
            log: console.log.bind(console),
            name: "concurrencytest",
            conurrency: 2,
            handler: function(job, callback) {
                started[job.data.info] = true;
                active++;
                assert(active <= 2, "active jobs: "+active);
                setTimeout(function() {
                    active--;
                    callback(null, JSON.stringify(job.data));
                }, 500);
            }
        };
        var q = new Qred.Manager(params);
        var qp = new Qred.Processor(params);
        qp.pause();
        var adata = {info:"A"};
        q.submitJob("ajobid", adata, { priority: 1 }, checkerr, function(err, result) {
            fin.A = true;
            assert(!err, err);
            assert(started.C && started.E);
            assert(result == JSON.stringify(adata));
            if(fin.A && fin.B && fin.C && fin.D && fin.E) done();
        });
        var bdata = {info:"B"};
        q.submitJob("bjobid", bdata , { priority: 1 }, checkerr, function(err, result) {
            assert(!err, err);
            fin.B = true;
            assert(started.C && started.E);
            assert(result == JSON.stringify(bdata));
            if(fin.A && fin.B && fin.C && fin.D && fin.E) done();
        });
        var cdata = {info:"C"};
        q.submitJob("cjobid", cdata , { priority: -1 }, checkerr, function(err, result) {
            assert(!err, err);
            fin.C = true;
            assert(result == JSON.stringify(cdata));
        });
        var ddata = {info:"D"};
        q.submitJob("djobid", ddata , { priority: 1 }, checkerr, function(err, result) {
            assert(!err, err);
            fin.D = true;
            assert(result == JSON.stringify(ddata));
            if(fin.A && fin.B && fin.C && fin.D && fin.E) done();
        });
        var edata = {info:"E"};
        q.submitJob("ejobid", edata , { priority: -1 }, checkerr, function(err, result) {
            assert(!err, err);
            fin.E = true;
            assert(result == JSON.stringify(edata));
        });
        qp.unpause();
    },
    function delayJobs(done) {
        var params = {
            redis: getClient(),
            subscriber: getClient(),
            log: console.log.bind(console),
            name: "delaytest",
            conurrency: 1,
            handler: function(job, callback) {
                assert(job.data.d === job.delay);
                callback(null, JSON.stringify(job.data));
            }
        };
        var q = new Qred.Manager(params);
        new Qred.Processor(params);

        var d = 500;
        var start = Date.now();
        var data = {info:"delay",d:d};
        q.submitJob("ajobid", data, { priority: 1, delay: d}, checkerr, function(err, result) {
            assert(!err, err);
            assert(Date.now() > start + d);
            assert(result == JSON.stringify(data));
            done();
        });
    },
    function attachToJob(done) {
        var handlerruns = 0;
        var params =  {
            redis: getClient(),
            subscriber: getClient(),
            log: console.log.bind(console),
            name: "delaytest",
            conurrency: 1,
            handler: function(job, callback) {
                handlerruns++;
                callback(null, JSON.stringify(job.data));
            }
        };
        var q = new Qred.Manager(params);
        var qp = new Qred.Processor(params);
        var data = {info:"attach"};
        var callbacks = 0;
        qp.pause();
        q.submitJob("ajobid", data, { }, checkerr, function(err, result) {
            assert(!err, err);
            assert(handlerruns === 1);
            assert(result == JSON.stringify(data));
            callbacks++;
            if(callbacks >= 2) done();
        });
        q.submitJob("ajobid", data, { }, checkerr, function(err, result) {
            assert(!err, err);
            assert(handlerruns === 1);
            assert(result == JSON.stringify(data));
            callbacks++;
            if(callbacks >= 2) done();
        });
        qp.unpause();
    },
    function snoopJob(done) {
        var handlerruns = 0;
        var params =  {
            redis: getClient(),
            subscriber: getClient(),
            log: console.log.bind(console),
            name: "snooptest",
            conurrency: 1,
            handler: function(job, callback) {
                handlerruns++;
                callback(null, JSON.stringify(job.data));
            }
        };
        var q = new Qred.Manager(params);
        new Qred.Processor(params);
        var data = {info:"attach"};
        var callbacks = 0;
        q.snoopJobbyJob("ajobid", function(err, result) {
            assert(!err, err);
            assert(handlerruns === 1);
            assert(result == JSON.stringify(data));
            callbacks++;
            if(callbacks >= 2) done();
        });
        q.submitJob("ajobid", data, { }, checkerr, function(err, result) {
            assert(!err, err);
            assert(handlerruns === 1);
            assert(result == JSON.stringify(data));
            callbacks++;
            if(callbacks >= 2) done();
        });
    },
    function nx(done) {
        var params =  {
            redis: getClient(),
            subscriber: getClient(),
            log: console.log.bind(console),
            name: "nxtest",
            conurrency: 1,
            handler: function(job, callback) {
                callback(null, JSON.stringify(job.data));
            }
        };
        var q = new Qred.Manager(params);
        var qp = new Qred.Processor(params);
        var data = {info:"nx"};
        var callbacks = 0;
        qp.pause();
        q.submitJob("anxjobid", data, { note: "a", nx: 1 }, function(err, result) {
            assert(!err, err);
            assert(result === "1");
        }, function(err, result) {
            assert(!err, err);
            assert(result == JSON.stringify(data));
            if(++callbacks >= 2) done();
        });
        q.submitJob("anxjobid", data, { note: "b", nx: 1}, function(err, result) {
            assert(!err);
            assert(result === "0");
            if(++callbacks >= 2) done();
        }, function(err) { 
            assert(!err, err);
            assert(false, "shouldn't have run");
        });
        qp.unpause();
    },
    function autoremove(done) {
        var params =  {
            redis: getClient(),
            subscriber: getClient(),
            log: console.log.bind(console),
            name: "removetest",
            conurrency: 1,
            handler: function(job, callback) {
                callback(null, JSON.stringify(job.data));
            }
        };
        var verify = function verify() {
            q.findJob("akeptjobid", function(err, job) {
                assert(!err, err);
                assert(job.length === 2);
                q.findJob("aremovedjobid", function(err, job) {
                    assert(!err, err);
                    assert(job === "0", job);
                    done();
                });
            });
        };
        var q = new Qred.Manager(params);
        var qp = new Qred.Processor(params);
        var data = {info:"autorem"};
        var data2 = {info:"autorem2"};
        var callbacks = 0;
        qp.pause();
        q.submitJob("akeptjobid", data, { note: "a", autoremove: 0 }, function(err) {
            assert(!err, err);
        }, function(err, result) {
            assert(!err, err);
            assert(result == JSON.stringify(data));
            if(++callbacks >= 2) verify();
        });
        q.submitJob("aremovedjobid", data2, { note: "a", autoremove: 1 }, function(err) {
            assert(!err, err);
        }, function(err, result) {
            assert(!err, err);
            assert(result == JSON.stringify(data2));
            if(++callbacks >= 2) verify();
        });
        qp.unpause();
    }
];

function beforeeach(done) {
    getClient().flushall(function(err) { assert(!err); done(); });
}


//Run in domain to catch asserts
function runNextTest() {
    var tdomain = Domain.create();
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
        beforeeach(function() {
            test(function() {
                clearTimeout(timeout_id);
                console.log("    Test \""+test.name+"\" passed");
                process.nextTick(runNextTest);
            });
        });
        timeout_id = setTimeout(function() {
            assert(false, test.name+" timed out!");
        }, timeout);
        tdomain.add(timeout_id);
    });
}

runNextTest();
