var fs = require("fs");
var path = require("path");
var Qred = require("./qred");
var async = require("async");

/*jshint es5:true*/
function Manager(opts) {
    Qred.call(this, opts);

    //Global concurrency settings
    //TODO - set these in redis, but only if explicitly passed in
    this.globalconcurrency = opts.globalconcurrency || 0;
    this.maxperperiod = opts.maxperperiod || 0;
    this.period = opts.period || 0;

    //Set up subsriptions
    opts.subscriber.subscribe(this.completion_channel_name, function(err) {
        if(err) throw new Error("Couldn't subscribe to redis completion channel");
    });
    opts.subscriber.on('message', this._handleMessage.bind(this));
}
Manager.prototype = new Qred({abstract:1});

//Handle job completion messages from redis
Manager.prototype._handleMessage = function(channel, message) {
    if(channel === this.completion_channel_name) {
        try {
            message = JSON.parse(message);
        } catch (err) {
            this.log("Unparseable msg "+message);
            return;
        }
        if(!message || !message.jobid) {
            this.log("Unknown message received on queue channel: "+JSON.stringify(message));
            return;
        }
        this.emit("complete", message);
        this.emit("complete:"+message.jobid, message);
    }
};

//Submit a job to the queue
//jobid - a name that uniquely defines the job. If a job exists in the queue with the same jobid, this job will replace it!
//data - an object with data to pass to the job. Will be stored in redis as a JSON blob
//opts - optional parameters for the job: priority, delay
//callback - called when job submission completes, or if an error occurs during submission or execution
Manager.prototype.submitJob = function submitJob(jobid, data, opts, submitted_cb) {
    var self = this;
    opts = opts || {};

    /*jshint evil:true*/
    self.redis.eval(scripts.addJob, 7, 
                    this.queuekey, this.delayqueuekey, this.delayprioritieskey, this.livesetkey, this.activesetkey, this.completesetkey, this.jobKey(jobid),
                    jobid, JSON.stringify(data), opts.priority || 0, Date.now(), opts.delay || 0, opts.nx || "", opts.autoremove === undefined ? -1 : opts.autoremove,
                    function(err, result) {
        if(err) {
            return submitted_cb(err, result);
        } else {
            if(result[0] === 1) {
                self.redis.publish(self.submission_channel_name, jobid, function(err) {
                    return submitted_cb(err, result);
                });
            } else {
                return submitted_cb(err, result);
            }
        }
    }); 
};

//Find a job in this queue with the given ID
Manager.prototype.findJob = function findJob(jobid, callback) {
    var self = this;
    this.redis.hgetall(this.jobKey(jobid),
                    function(err, job) {
                        if(err) return callback(err);
                        if(job) {
                            try {
                                job.data = JSON.parse(job.data);
                            } catch(err) { }
                            job.id = jobid;
                        }
                        if(job && job.expires && job.expires < Date.now()) {
                            self.removeJob(jobid, {}, function(err) {
                                callback(err, null);
                            });
                        } else {
                            callback(null, job);
                        }
                    }); 
};




//Tidy up auxialiary data structures that may contain bad data due to crashes 
//or manual redis twiddling
Manager.prototype.reconcile = function(callback) {
    var self = this;

    var getAll = function(type, key, callback) {
        switch(type) {
            case 'z':
                return self.redis.zrange(key, 0, -1, callback);
            case 's':
                return self.redis.smembers(key, callback);
            case 'h':
                return self.redis.hkeys(key, callback);
        }
    };

    var removers = {
        z: 'zrem',
        s: 'srem',
        h: 'hdel'
    };

    var removeInvalid = function(type, setkey, callback) {
        getAll(type, setkey, function(err, ids) {
            if(err) callback(err);
            var removed = [];
            async.each(ids, function(id, done) {
                self.redis.exists(self.jobKey(id), function(err, exists) {
                    if(err) return done(err);
                    if(exists) {
                        done();
                    } else {
                        self.redis[removers[type]](setkey, id, function(err) {
                            if(err) return done(err);
                            removed.push(id);
                            done();
                        });
                    }
                });
            }, function(err) {
                callback(err, removed);
            }); 
        });
    };

    async.parallel({
        liveSet: function(done) {
            removeInvalid('z', self.livesetkey, done);
        },
        delayQueue: function(done) {
            removeInvalid('z', self.delayqueuekey, done);
        },
        queue: function(done) {
            removeInvalid('z', self.queuekey, done);
        },
        activeSet: function(done) {
            removeInvalid('s', self.activesetkey, done);
        },
        completeSet: function(done) {
            removeInvalid('s', self.completesetkey, done);
        },
        delayPriorities: function(done) {
            removeInvalid('h', self.delayprioritieskey, done);
        }
    }, callback);
};

//Job is hash with fields:
//  group - the concurrency group of the job (i.e…. the named queue)
//  priority - relative priority of the job. lower priorities will all be run before higher priorities, respecting delay
//  runtime - the time after which the job may be run
//  data - a generic JSON payload passed back to the worker


//EXAMPLE KEYS
//Queue Name - sample
//Job Names: jobA (priority -1), jobB (priority 0), jobC (priority -1)
//
//A job has a few properties, each stored in a different hash keyed on jobid
//We store this way so that we can get various job properties without dynamically generating
//keys in scripts. This keeps scripts compatible with Redis Cluster.
//
//hash qred:sample::data
//hash qred:sample:priority
//hash qred:sample:runtime
//hash qred:sample:created
//
//zset qred:sample:delayed <set of job ids scored by runtime>
//zset qred:samples:queued <set of job ids scored by priority>
//

//Later functionality: 
//Store the max number of jobs globally this queue should run at once (global concurrency)
//value qred:sample:maxjobs 5
//Store the max number of jobs globally this queue should start per second (global ratelimit)
//value qred:sample:perjobs 5
//Number of jobs currently running
//value qred:sample:current <n>
//Maintain list of active jobs
//set qred:samples:active <set of job ids>

var scripts = {
    addJob: fs.readFileSync(path.join(__dirname, "..", "lua", "addJob.lua"))
};

module.exports = Manager;
