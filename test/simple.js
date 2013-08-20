var assert = require("assert");
var harness = require("./harness");
var Qred = require("../index");

var checkerr = function(err) {
    assert(!err, err);
};

var countresult = function(nums) {
    return function(err, result) {
        assert(!err);
        assert(result);
        assert(result.length === nums.length);
        for(var i = 0; i < result.length; i++) {
            assert(result[i] === nums[i]);
        }
    };
};

var tests = [ 
    {
        test: function singleJob(done) {
            var params = {
                redis: harness.getClient(),
                subscriber: harness.getClient(),
                log: console.log.bind(console),
                name: "simpletest",
                handler: function(data, callback) {
                    q.markProgress('ajobid', 'Set something for next test to clear', checkerr);
                    q.findJob('ajobid', function(err, job) {
                        assert(job.status === 'active');
                        callback(null, JSON.stringify(data));
                    });
                }
            };
            var q = new Qred.Manager(params);
            new Qred.Processor(params);
            var data = { data1: "a", data2: "b" };
            q.submitJob("ajobid", data , {}, checkerr, function(err, result) {
                assert(!err, err);
                assert(result == JSON.stringify(data));
                q.findJob('ajobid', function(err, job) {
                    assert(job.status === 'complete', job.status);
                    done();
                });
            });
        },
        timeout: 5000
    }, {
        test: function singleJobExpire(done) {
            var params = {
                redis: harness.getClient(),
                subscriber: harness.getClient(),
                log: console.log.bind(console),
                name: "simpletest",
                handler: function(data, callback) {
                    q.markProgress('ajobid', 'Set something for next test to clear', checkerr);
                    q.findJob('ajobid', function(err, job) {
                        assert(job.status === 'active');
                        callback(null, JSON.stringify(data));
                    });
                }
            };
            var q = new Qred.Manager(params);
            new Qred.Processor(params);
            var data = { data1: "a", data2: "b" };
            q.submitJob("ajobid", data , { autoremove: 2000 }, checkerr, function(err, result) {
                assert(!err, err);
                assert(result == JSON.stringify(data));
                q.findJob('ajobid', function(err, job) {
                    assert(job.status === 'complete', job.status);
                });
                setTimeout(function() {
                    q.findJob('ajobid', function(err, job) {
                        assert(!err);
                        assert(!job);
                        done();
                    });
                }, 3000);
            });
        },
        timeout: 5000
    }, {
        test: function progress(done) {
            var params = {
                redis: harness.getClient(),
                subscriber: harness.getClient(),
                log: console.log.bind(console),
                name: "simpletest",
                handler: function(data, callback) {
                    q.markProgress('ajobid', 'Looking up job', function(err) {
                        assert(!err);
                        q.findJob('ajobid', function(err, job) {
                            assert(!err);
                            assert(job.status === 'active');
                            q.markProgress('ajobid', 'Found job', function(err) {
                                assert(!err);
                                callback(null, JSON.stringify(data));
                            });
                        });
                    });
                }
            };
            var q = new Qred.Manager(params);
            new Qred.Processor(params);
            var data = { data1: "a", data2: "b" };
            q.submitJob("ajobid", data , {}, checkerr, function(err, result) {
                assert(!err, err);
                assert(result == JSON.stringify(data));
                q.findJob('ajobid', function(err, job) {
                    assert(job.status === 'complete', job.status);
                    q.getProgress('ajobid', function(err, progress) {
                        assert(!err);
                        assert(progress);
                        assert(progress.length === 2);
                        assert(progress[0].indexOf('Looking up job') >= 0, progress[0]);
                        assert(progress[1].indexOf('Found job') >= 0, progress[1]);
                        done();
                    });
                });
            });
        },
        timeout: 5000
    }, {
        test: function multiProc(done) {
            var called1 = false;
            var called2 = false;
            var params = {
                redis: harness.getClient(),
                subscriber: harness.getClient(),
                log: console.log.bind(console),
                name: "multitest",
                concurrency: 1,
                handler: function(data, callback) {
                    assert(!called1);
                    called1 = true;
                    setTimeout(function() {
                        callback(null, JSON.stringify(data));
                    }, 1000);
                }
            };
            var params2 = {
                redis: params.redis,
                subscriber: params.subscriber,
                log: params.log,
                name: params.name,
                concurrency: 1,
                handler: function(data, callback) {
                    assert(!called2);
                    called2 = true;
                    setTimeout(function() {
                        callback(null, JSON.stringify(data));
                    }, 1000);
                }
            };
           
            var finish = function() {
                assert(called1);
                assert(called2);
                done();
            };

            var q = new Qred.Manager(params);
            var qp1 = new Qred.Processor(params);
            var qp2 = new Qred.Processor(params2);
            var data = { data1: "a", data2: "b" };
            qp1.pause();
            qp2.pause();
            var cbs = 0;
            q.submitJob("ajobid", data , {}, checkerr, function(err, result) {
                assert(!err, err);
                assert(result == JSON.stringify(data));
                q.findJob('ajobid', function(err, job) {
                    assert(job.status === 'complete', job.status);
                    if(++cbs >= 2) finish();
                });
            });
            q.submitJob("a2ndjobid", data , {}, checkerr, function(err, result) {
                assert(!err, err);
                assert(result == JSON.stringify(data));
                q.findJob('a2ndjobid', function(err, job) {
                    assert(job.status === 'complete', job.status);
                    if(++cbs >= 2) finish();
                });
            });
            qp1.unpause();
            qp2.unpause();
        },
        timeout: 5000
    },
    function errors(done) {
        var params = {
            redis: harness.getClient(),
            subscriber: harness.getClient(),
            log: console.log.bind(console),
            name: "errorest",
            conurrency: 1,
            handler: function(data, callback) {
                setTimeout(function() {
                    callback(new Error());
                }, 500);
            }
        };
        var q = new Qred.Manager(params);
        new Qred.Processor(params);
        var data = { data1: "a", data2: "b" };
        q.submitJob("aerrjobid", data , {}, checkerr, function(err, result) {
            assert(err);
            assert(!result);
            q.findJob('aerrjobid', function(err, job) {
                assert(job.status === 'error', job.status);
                done();
            });
        });
    },

    function priorityJobs(done) {
        var params = {
            redis: harness.getClient(),
            subscriber: harness.getClient(),
            log: console.log.bind(console),
            name: "priotest",
            conurrency: 1,
            handler: function(data, callback) {
                setTimeout(function() {
                    callback(null, JSON.stringify(data));
                }, 500);
            }
        };
        var q = new Qred.Manager(params);
        var qp = new Qred.Processor(params);

        var adone = false;
        var bdone = false;
        var cdone = false;
        qp.pause();
        var adata = {info:"A"};
        q.submitJob("ajobid", adata, { priority: 1 }, countresult(["1",0,1,0,0]), function(err, result) {
            assert(!err, err);
            assert(cdone);
            adone = true;
            assert(result == JSON.stringify(adata));
            if(adone && bdone && cdone) done();
        });
        var bdata = {info:"B"};
        q.submitJob("bjobid", bdata , { priority: 1 }, countresult(["1",0,2,0,0]), function(err, result) {
            assert(!err, err);
            assert(cdone);
            bdone = true;
            assert(result == JSON.stringify(bdata));
            if(adone && bdone && cdone) done();
        });
        var cdata = {info:"C"};
        q.submitJob("cjobid", cdata , { priority: -1 }, countresult(["1",0,3,0,0]), function(err, result) {
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
            redis: harness.getClient(),
            subscriber: harness.getClient(),
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
            redis: harness.getClient(),
            subscriber: harness.getClient(),
            log: console.log.bind(console),
            name: "delaytest",
            conurrency: 1,
            handler: function(data, callback) {
                callback(null, JSON.stringify(data));
            }
        };
        var q = new Qred.Manager(params);
        var qp = new Qred.Processor(params);

        var start = Date.now();
        var data = {info:"delay"};
        var cb = 0;
        qp.pause();
        q.submitJob("ajobid", data, { priority: 1, delay: 10000 }, countresult(["1",1,0,0,0]), function(err, result) {
            assert(!err, err);
            assert(Date.now() > start + 500);
            assert(result == JSON.stringify(data));
            if(++cb >= 2) done();
        });
        q.submitJob("ajobid", data, { priority: 1, delay: 500 }, countresult(["1",1,0,0,0]), function(err, result) {
            assert(!err, err);
            assert(Date.now() > start + 500);
            assert(result == JSON.stringify(data));
            if(++cb >= 2) done();
        });
        qp.unpause();
    },
    function delayJobsNamedCallbacks(done) {
        var params = {
            redis: harness.getClient(),
            subscriber: harness.getClient(),
            log: console.log.bind(console),
            name: "delaytest",
            conurrency: 1,
            handler: function(data, callback) {
                callback(null, JSON.stringify(data));
            }
        };
        var q = new Qred.Manager(params);
        var qp = new Qred.Processor(params);

        var start = Date.now();
        var data = {info:"delay"};
        var cb = 0;
        qp.pause();
        q.submitJob("ajobid", data, { priority: 1, delay: 10000 }, countresult(["1",1,0,0,0]), function named() {
            assert(false);
        });
        q.submitJob("ajobid", data, { priority: 1, delay: 500 }, countresult(["1",1,0,0,0]), function named(err, result) {
            assert(!err, err);
            assert(Date.now() > start + 500);
            assert(result == JSON.stringify(data));
            if(++cb >= 1) done();
        });
        qp.unpause();
    },
    function attachToJob(done) {
        var handlerruns = 0;
        var params =  {
            redis: harness.getClient(),
            subscriber: harness.getClient(),
            log: console.log.bind(console),
            name: "delaytest",
            conurrency: 1,
            handler: function(data, callback) {
                handlerruns++;
                callback(null, JSON.stringify(data));
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
        q.submitJob("ajobid", data, { }, countresult(["1", 0, 1, 0, 0]), function(err, result) {
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
            redis: harness.getClient(),
            subscriber: harness.getClient(),
            log: console.log.bind(console),
            name: "snooptest",
            conurrency: 1,
            handler: function(data, callback) {
                handlerruns++;
                callback(null, JSON.stringify(data));
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
    function nxdq(done) {
        var params =  {
            redis: harness.getClient(),
            subscriber: harness.getClient(),
            log: console.log.bind(console),
            name: "nxtest",
            conurrency: 1,
            handler: function(data, callback) {
                callback(null, JSON.stringify(data));
            }
        };
        var q = new Qred.Manager(params);
        var qp = new Qred.Processor(params);
        var data = {info:"nx"};
        var callbacks = 0;
        qp.pause();
        q.submitJob("anxjobid", data, { note: "a" }, countresult(["1",0,1,0,0]), function(err, result) {
            assert(!err, err);
            assert(result == JSON.stringify(data));
            if(++callbacks >= 2) done();
        });
        q.submitJob("anxjobid", data, { note: "b", nx: "dq" }, function(err, result) {
            assert(!err);
            assert(result);
            assert(result[0] === "0");
            assert(result[1] === "queued");
            if(++callbacks >= 2) done();
        }, function(err) { 
            assert(!err, err);
            assert(false, "shouldn't have run");
        });
        qp.unpause();
    },
    function nxac(done) {
        var params =  {
            redis: harness.getClient(),
            subscriber: harness.getClient(),
            log: console.log.bind(console),
            name: "nxtest2",
            conurrency: 1,
            handler: function(data, callback) {
                callback(null, JSON.stringify(data));
            }
        };
        var finish = function() {
            q.submitJob("anx2jobid", data, { note: "b", nx: "ac" }, function(err, result) {
                assert(!err);
                assert(result);
                assert(result[0] === "0", result);
                assert(result[1] === "complete");
                done();
            }, function(err) {
                assert(!err, err);
                assert(false, "shouldn't have run");
            });
        };
        var q = new Qred.Manager(params);
        var qp = new Qred.Processor(params);
        var data = {info:"nx"};
        var callbacks = 0;
        qp.pause();
        q.submitJob("anx2jobid", data, { note: "a" }, countresult(["1",0,1,0,0]), function(err, result) {
            assert(!err, err);
            assert(result == JSON.stringify(data));
            if(++callbacks >= 2) finish();
        });
        q.submitJob("anx2jobid", data, { note: "a", nx: "ac" }, countresult(["1",0,1,0,0]), function(err, result) {
            assert(!err, err);
            assert(result == JSON.stringify(data));
            if(++callbacks >= 2) finish();
        });
        qp.unpause();
    },
    function remove(done) {
        var params =  {
            redis: harness.getClient(),
            subscriber: harness.getClient(),
            log: console.log.bind(console),
            name: "removetest",
            conurrency: 1,
            handler: function(data, callback) {
                callback(null, JSON.stringify(data));
            }
        };
        var q = new Qred.Manager(params);
        new Qred.Processor(params);
        var data = {info:"nx"};
        var callbacks = 0;
        q.submitJob("aremovejob", data, { note: "a" }, function(err, result) {
            assert(!err, err);
            assert(result);
            assert(result[0] === "1");
            assert(result[1] === 0);
            assert(result[2] === 1);
            assert(result[3] === 0);
            assert(result[4] === 0);
            if(++callbacks >= 2) done();
        }, function(err) {
            assert(!err);
            assert(false, "Shouldn't get here");
        });
        q.removeJob("aremovejob", function(err, result) {
            assert(!err, err);
            assert(result instanceof Array);
            assert(result.length === 5);
            assert(result[0] === 1);
            assert(result[1] === 1);
            assert(result[2] === 0);
            assert(result[3] === 0);
            assert(result[4] === 0);
            if(++callbacks >= 2) done();
        });
    },
    function autoremove(done) {
        var params =  {
            redis: harness.getClient(),
            subscriber: harness.getClient(),
            log: console.log.bind(console),
            name: "removetest",
            conurrency: 1,
            handler: function(data, callback) {
                callback(null, JSON.stringify(data));
            }
        };
        var verify = function verify() {
            q.findJob("akeptjobid", function(err, job) {
                assert(!err, err);
                assert(job);
                assert(job.id === "akeptjobid");
                q.findJob("aremovedjobid", function(err, job) {
                    assert(!err, err);
                    assert(!job, JSON.stringify(job));
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
        q.submitJob("akeptjobid", data, { note: "a", autoremove: -1 }, function(err) {
            assert(!err, err);
        }, function(err, result) {
            assert(!err, err);
            assert(result == JSON.stringify(data));
            if(++callbacks >= 2) setTimeout(verify, 10);
        });
        q.submitJob("aremovedjobid", data2, { note: "a", autoremove: 10 }, function(err) {
            assert(!err, err);
        }, function(err, result) {
            assert(!err, err);
            assert(result == JSON.stringify(data2));
            if(++callbacks >= 2) setTimeout(verify, 10);
        });
        qp.unpause();
    }
];


function beforeeach(done) {
    harness.getClient().flushall(function(err) { assert(!err); done(); });
}

harness.go(tests, beforeeach, function(err) {
    process.exit(!!err);
});

