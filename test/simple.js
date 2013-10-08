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
            q.submitJob("ajobid", data , {}, checkerr);
            var verify = function(message) {
                assert(message.jobid === "ajobid");
                assert(message);
                assert(!message.error, message.error);
                assert(message.result == JSON.stringify(data));
                q.findJob('ajobid', function(err, job) {
                    assert(job.status === 'complete', job.status);
                    done();
                });
            };
            q.once('complete', verify);
        },
        timeout: 5000
    }, {
        test: function singleJobWeirdAutoremove(done) {
            var params = {
                redis: harness.getClient(),
                subscriber: harness.getClient(),
                log: console.log.bind(console),
                name: "weirdautoremove",
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
            q.submitJob("ajobid", data , { autoremove: true }, checkerr);
            var verify = function(message) {
                assert(message.jobid === "ajobid");
                assert(message);
                assert(!message.error, message.error);
                assert(message.result == JSON.stringify(data));
                q.findJob('ajobid', function(err, job) {
                    assert(job.status === 'complete', job.status);
                    done();
                });
            };
            q.once('complete', verify);
        },
        timeout: 5000
    }, {
        test: function specificEmits(done) {
            var params = {
                redis: harness.getClient(),
                subscriber: harness.getClient(),
                log: console.log.bind(console),
                name: "simpletest",
                handler: function(data, callback) {
                    q.markProgress('ajobid', 'Set something for next test to clear', checkerr);
                    callback(null, JSON.stringify(data));
                }
            };
            var q = new Qred.Manager(params);
            new Qred.Processor(params);
            var data = { data1: "a", data2: "b" };
            var data2 = { data1: "a2", data2: "ab" };
            q.submitJob("ajobid", data , {}, checkerr);
            q.submitJob("bjobid", data2 , {}, checkerr);
            var cb = 0;
            var verify = function(message) {
                assert(message.jobid === "ajobid");
                assert(message);
                assert(!message.error, message.error);
                assert(message.result == JSON.stringify(data));
                q.findJob('ajobid', function(err, job) {
                    assert(job.status === 'complete', job.status);
                    if(++cb === 2) done();
                });
            };
            var verify2 = function(message) {
                assert(message.jobid === "bjobid");
                assert(message);
                assert(!message.error, message.error);
                assert(message.result == JSON.stringify(data2));
                q.findJob('bjobid', function(err, job) {
                    assert(job.status === 'complete', job.status);
                    if(++cb === 2) done();
                });
            };
            q.once('complete:ajobid', verify);
            q.once('complete:bjobid', verify2);
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
            q.submitJob("ajobid", data , { autoremove: 2000 }, checkerr);
            var verify = function(message) {
                assert(message.jobid === "ajobid");
                assert(!message.error, message.error);
                assert(message.result == JSON.stringify(data));
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
            };
            q.once("complete", verify);
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
            q.submitJob("ajobid", data , {}, checkerr);
            var verify = function(message) {
                assert(message.jobid === "ajobid");
                assert(!message.error, message.error);
                assert(message.result == JSON.stringify(data));
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
            };
            q.once("complete", verify);
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
                q.removeListener('complete', verify);
                done();
            };

            var q = new Qred.Manager(params);
            var qp1 = new Qred.Processor(params);
            var qp2 = new Qred.Processor(params2);
            var data = { data1: "a", data2: "b" };
            qp1.pause();
            qp2.pause();
            var cbs = 0;
            q.submitJob("ajobid", data , {}, checkerr);
            q.submitJob("a2ndjobid", data , {}, checkerr);
            
            var verify = function(message) {
                assert(!message.error, message.error);
                assert(message.result == JSON.stringify(data));
                if(message.jobid === "ajobid") {
                    q.findJob('ajobid', function(err, job) {
                        assert(job.status === 'complete', job.status);
                        if(++cbs >= 2) finish();
                    });
                } else if(message.jobid === "a2ndjobid") {
                    q.findJob('a2ndjobid', function(err, job) {
                        assert(job.status === 'complete', job.status);
                        if(++cbs >= 2) finish();
                    });
                }
            };
            q.on('complete', verify);
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
        q.submitJob("aerrjobid", data , {}, checkerr);
        var verify = function(message) {
            assert(message);
            assert(message.jobid === "aerrjobid");
            assert(message.error);
            assert(!message.result);
            q.findJob('aerrjobid', function(err, job) {
                assert(job.status === 'error', job.status);
                done();
            });
        };
        q.once('complete', verify);
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
        var verify = function(message) {
            assert(message);
            assert(!message.error, message.error);
            if(message.jobid === "ajobid") {
                assert(cdone);
                adone = true;
                assert(message.result == JSON.stringify(adata));
            } else if(message.jobid === "bjobid") {
                assert(cdone);
                assert(message.result == JSON.stringify(bdata));
                bdone = true;
            } else if(message.jobid === "cjobid") {
                assert(!adone);
                assert(!bdone);
                assert(message.result == JSON.stringify(cdata));
                cdone = true;
            }
            if(adone && bdone && cdone) {
                q.removeListener('complete', verify);
                done();
            }
        };
        var adata = {info:"A"};
        q.submitJob("ajobid", adata, { priority: 1 }, countresult(["1",0,1,0,0]));
        var bdata = {info:"B"};
        q.submitJob("bjobid", bdata , { priority: 1 }, countresult(["1",0,2,0,0]));
        var cdata = {info:"C"};
        q.submitJob("cjobid", cdata , { priority: -1 }, countresult(["1",0,3,0,0]));
        q.on('complete', verify);
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
        var verify = function(message) {
            assert(!message.error, message.error);
            fin[message.jobid] = true;
            if(message.jobid === "ajobid") {
                assert(started.cjobid && started.ejobid);
                assert(message.result == JSON.stringify(adata));
            } else if(message.jobid === "bjobid") {
                assert(started.cjobid && started.ejobid);
                assert(message.result == JSON.stringify(bdata));
            } else if(message.jobid === "cjobid") {
                assert(message.result == JSON.stringify(cdata));
            } else if(message.jobid === "djobid") {
                assert(message.result == JSON.stringify(ddata));
            } else if(message.jobid === "ejobid") {
                assert(message.result == JSON.stringify(edata));
            }
            if(fin.ajobid && fin.bjobid && fin.cjobid && fin.djobid && fin.ejobid) {
                q.removeListener('complete', verify);
                done();
            }
        };
        var adata = {info:"ajobid"};
        q.submitJob("ajobid", adata, { priority: 1 }, checkerr);
        var bdata = {info:"bjobid"};
        q.submitJob("bjobid", bdata , { priority: 1 }, checkerr);
        var cdata = {info:"cjobid"};
        q.submitJob("cjobid", cdata , { priority: -1 }, checkerr);
        var ddata = {info:"djobid"};
        q.submitJob("djobid", ddata , { priority: 1 }, checkerr);
        var edata = {info:"ejobid"};
        q.submitJob("ejobid", edata , { priority: -1 }, checkerr);
        q.on('complete', verify);
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
        qp.pause();
        var verify = function(message) {
            assert(!message.error);
            assert(message.result == JSON.stringify(data));
            assert(Date.now() > start + 500);
            done();
        };
        q.submitJob("ajobid", data, { priority: 1, delay: 10000 }, countresult(["1",1,0,0,0]));
        q.submitJob("ajobid", data, { priority: 1, delay: 500 }, countresult(["1",1,0,0,0]));
        q.once('complete', verify);
        qp.unpause();
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
        var data2 = {info:"nx2"};
        qp.pause();
        var verify = function(message) {
            assert(!message.error, message.error);
            assert(message.result == JSON.stringify(data));
            q.removeAllListeners('complete');
            done();
        };
        q.submitJob("anxjobid", data, { note: "a" }, countresult(["1",0,1,0,0]));
        q.submitJob("anxjobid", data2, { note: "b", nx: "dq" }, function(err, result) {
            assert(!err);
            assert(result);
            assert(result[0] === "0");
            assert(result[1] === "queued");
        });
        q.on('complete', verify);
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
        var verify = function(message) {
            assert(!message.error, message.error);
            assert(message.result === JSON.stringify(data2));
            q.submitJob("anx2jobid", data, { note: "b", nx: "ac" }, function(err, result) {
                assert(!err);
                assert(result);
                assert(result[0] === "0", result);
                assert(result[1] === "complete");
                q.removeAllListeners('complete');
                done();
            });
        };
        var q = new Qred.Manager(params);
        var qp = new Qred.Processor(params);
        var data = {info:"ac"};
        var data2 = {info:"ac2"};
        qp.pause();
        q.submitJob("anx2jobid", data, { note: "a" }, countresult(["1",0,1,0,0]));
        q.submitJob("anx2jobid", data2, { note: "a", nx: "ac" }, countresult(["1",0,1,0,0]));
        q.on('complete', verify);
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
            name: "autoremovetest",
            conurrency: 1,
            handler: function(data, callback) {
                callback(null, JSON.stringify(data));
            }
        };
        var callbacks = 0;
        var verify = function verify(message) {
            assert(!message.error, message.error);
            assert(message.jobid === "akeptjobid" || message.jobid === "aremovedjobid");
            callbacks++;
            assert(callbacks <= 2);
            if(callbacks == 2) {
                setTimeout(function() {
                    q.findJob("akeptjobid", function(err, job) {
                        assert(!err, err);
                        assert(job);
                        assert(job.id === "akeptjobid");
                        q.findJob("aremovedjobid", function(err, job) {
                            assert(!err, err);
                            assert(!job, JSON.stringify(job));
                            q.removeAllListeners('complete');
                            done();
                        });
                    });
                }, 10);
            }
        };
        var q = new Qred.Manager(params);
        var qp = new Qred.Processor(params);
        var data = {info:"autorem"};
        var data2 = {info:"autorem2"};
        qp.pause();
        q.submitJob("akeptjobid", data, { note: "a", autoremove: -1 }, checkerr);
        q.submitJob("aremovedjobid", data2, { note: "a", autoremove: 10 }, checkerr);
        q.on('complete', verify);
        qp.unpause();
    }
];


function beforeeach(done) {
    harness.getClient().flushall(function(err) { assert(!err); done(); });
}

harness.go(tests, beforeeach, function(err) {
    process.exit(!!err);
});

