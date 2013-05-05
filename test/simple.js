var assert = require("assert");
var Qred = require("../lib/qred").Qred;
try {
    var redis = require("redis");
} catch(err) {
    console.log("npm install redis to run tests");
    process.exit(-1);
}

var client = redis.createClient(6480);
var sclient = redis.createClient(6480);
var tests = [ 
    function singleJob(done) {
        var q = new Qred(client, sclient, {
            log: console.log.bind(console),
            name: "simpletest",
            handler: function(data, callback) {
                callback(null, JSON.stringify(data));
            }
        });
        var data = { data1: "a", data2: "b" };
        q.submitJob("ajobid", data , {}, function(err, result) {
            assert(!err, err);
            assert(result == JSON.stringify(data));
            done();
        });

    },
    function priorityJobs(done) {
        var q = new Qred(client, sclient, {
            log: console.log.bind(console),
            name: "priotest",
            conurrency: 1,
            handler: function(data, callback) {
                setTimeout(function() {
                    callback(null, JSON.stringify(data));
                }, 500);
            }
        });
        var adone = false;
        var bdone = false;
        var cdone = false;
        q.pause();
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
        q.unpause();
    },
    function concurrencyTest(done) {
        var active = 0;
        var started = {};
        var fin = {};
        var q = new Qred(client, sclient, {
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
        });
        q.pause();
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
        q.unpause();
    },
    function delayJobs(done) {
        var q = new Qred(client, sclient, {
            log: console.log.bind(console),
            name: "delaytest",
            conurrency: 1,
            handler: function(data, callback) {
                callback(null, JSON.stringify(data));
            }
        });
        var start = Date.now();
        var data = {info:"delay"};
        q.submitJob("ajobid", data, { priority: 1, delay: 500 }, function(err, result) {
            assert(!err, err);
            assert(Date.now() > start + 500);
            assert(result == JSON.stringify(data));
            done();
        });
    }

];


//Run in domain to catch asserts
function runNextTest() {
    var test = tests.shift();
    if(!test) {
        console.log("Finished tests");
        process.exit(0);
    }
    console.log("Starting test "+test.name);
    test(function() {
        console.log("Test \""+test.name+"\" passed");
        runNextTest();
    });
}

runNextTest();
