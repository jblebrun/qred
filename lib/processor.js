var fs = require("fs");
var path = require("path");
var Qred = require("./qred.js");

/*jshint es5:true*/
function Processor(opts) {
    Qred.call(this, opts);
    
    //Local job handler settings
    this.handler = opts.handler || null;
    this.concurrency = opts.concurrency || 1;

    //Initialize internal state
    this.active = 0;
    this.paused = false;

    //Set up subsriptions
    opts.subscriber.subscribe(this.completion_channel_name, this.submission_channel_name, function(err) {
        if(err) throw new Error("Couldn't subscribe to redis completion channel");
    });
    opts.subscriber.on('message', this._handleMessage.bind(this));
    opts.subscriber.on('subscribe', this._handleMessage.bind(this));
}
Processor.prototype = new Qred({abstract:1});

//Internal method which handles a channel message publication and dispatches
//to the appropriate handler (it's either a job submission or a job completion)
Processor.prototype._handleMessage = Processor.prototype.handleSubscribe = function(channel) {
    if(channel === this.completion_channel_name) {
        this._process();
        this._clean();
    } else if(channel === this.submission_channel_name) {
        this._process();
    }
};

Processor.prototype.pause = function() {
    this.paused = true;
};

Processor.prototype.unpause = function() {
    this.paused = false;
    this._process();
};


Processor.prototype._clean = function clean() {
    var self = this;
    if(self.cleaning) return;
    self.cleaning = true;
    this.redis.zrange(this.livesetkey, 0, 0, 'withscores', function(err, items) {
        self.cleaning = false;
        if(err) {
            self.log(err);
            return;
        }
        if(!items || items.length === 0) return;
        if(items.length !== 2) {
            self.log("Unexpected number of items from zrange for "+this.livesetkey);
            self.log(JSON.stringify(items));
            return;
        }
        var jobid = items[0];
        var expires = items[1];
        if(expires === "inf" || expires === "Infinity") return;
        expires = parseInt(expires, 10);
        if(isNaN(expires)) {
            self.log("Unknown expire time: "+items[1]);
            return;
        }
        var now = Date.now();
        if(now > expires) {
            self.removeJob(jobid, {}, self.logerr);
            process.nextTick(self._clean.bind(self));
        } else {
            if(expires - now > 0) {
                setTimeout(self._clean.bind(self), expires- now);
            }
        }
    });
};

//Attempt to fetch and handle a job. If we get a job, try to get another one right away
Processor.prototype._process = function process() {
    var self = this;
    if(!this.handler) return;
    if(this.paused) return;
    if(self.active >= self.concurrency) return;
    self.active++;
    /*jshint evil:true*/
    this.redis.eval(scripts.getNextJob, 4,
                    this.queuekey, this.delayqueuekey, this.activesetkey, this.delayprioritieskey,
                    Date.now(),
                    function(err, result) {
        self.active--;
        if(err) {
            self.log("Error getting next job");
            self.log(err);
            return;
        }
        if(result && result[0] === 1) {
            self._handleJob(result[1]);
        } else if(result && result[0] === 0) {
            var delay = parseInt(result[1], 10) - Date.now();
            if(delay) {
                setTimeout(self._process.bind(self), delay);
            }
        } else {
            self.log("Error: unknown result "+result);
        } 
    });
};


Processor.prototype.checkerr = function(err, msg) {
    if(err) {
        this.log(msg);
        this.log(err);
        return true;
    }
    return false;
};

Processor.prototype._handleJob = function handleJob(jobid) {
    var self = this;
    var jobKey = self.jobKey(jobid);
    self.redis.hgetall(jobKey, function(err, job) {
        if(self.checkerr(err, "Error retreiving details of next job")) return;
        if (!job) {
            self.log("Error retreiving details of next job: " + jobKey);
            return;
        }
        var data = job.data;
        
        try { data = JSON.parse(data); } catch(err) { }

        self.active++;
        
        self.redis.hset(jobKey, 'status', 'active', function(err) {
            if(self.checkerr(err, "Error updating job status to active")) return;
            self.clearProgress(jobid, function(err) {
                if(self.checkerr(err, "Error clearing job progress for "+jobid)) return;
                self.handler(data, function(job_err, result) {
                    /*jshint evil:true*/
                    self.redis.eval(scripts.finishJob, 5,
                                    self.activesetkey, self.completesetkey, self.livesetkey, self.delayqueuekey, jobKey, 
                                    jobid, !!job_err, Date.now(),
                                    function(err) {
                                        self.active--;
                                        if(self.checkerr(err, "Error updating job status to complete")) return;
                                        self.redis.publish(self.completion_channel_name, JSON.stringify({
                                            jobid: jobid,
                                            error: job_err,
                                            result: result
                                        }));
                                        self.emit("complete", jobid, job_err, result);
                                        self.emit("complete:"+jobid, jobid, job_err, result);
                                    });
                });
            });
            //Since we got a job, we might be able to run more, so try getting another.
            self._process();
        });
    });
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
    getNextJob: fs.readFileSync(path.join(__dirname, "..", "lua", "getNextJob.lua")),
    finishJob: fs.readFileSync(path.join(__dirname, "..", "lua", "finishJob.lua"))
};

module.exports = Processor;
