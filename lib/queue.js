
function Queue(redis, redis_subscribe, opts) {
    opts = opts || {};
    this.logger = opts.logger || {};
    this.handler = opts.handler || null;
    this.concurrency = opts.concurrency || 1;
    this.priorities = opts.priorties || 2;
    this.redis = redis;
    this.prefix = opts.prefix || "qred";
    this.name = opts.name || "queue";
    this.processors = [];
    this.loadqueue = [];
    this.completion_callbacks = {};
    this.completion_channel_name = "qred:"+opts.name+":finished";
    this.submission_channel_name = "qred:"+opts.name+":submit";
    redis_subscribe.subscribe(this.completion_channel_name, function(err) {
        if(err) throw new Error("Couldn't subscribe to redis completion channel");
    });
    redis_subscribe.subscribe(this.submission_channel_name, function(err) {
        if(err) throw new Error("Couldn't subscribe to redis submission channel");
    });
    redis_subscribe.on('message', this.handleMessage.bind(this));
    this.submitScripts();
}

//Initialization function
//Submit all of the necessary Lua scripts to the redis backing
//for this queue instance
Queue.prototype.submitScripts = function() {
    var self = this;
    (function loadAddJobScript() {
        self.redis['load script'](addJobScript, function(err, sha) {
            self.addJobScriptSHA = sha;
            loadRemJobScript();
        });
    })();
    function loadRemJobScript() {
        self.redis['load script'](remJobScript, function(err, sha) {
            self.remJobScriptSHA = sha;
            loadGetNextJobScript();
        });
    }
    function loadGetNextJobScript() {
        self.redis['load script'](getNextJobScript, function(err, sha) {
            self.getNextJobScriptSHA = sha;
            finish();
        });
    }
    function finish() {
        self.scriptsReady = true;
        if(self.loadqueue.length > 0) {
            for(var i = 0; i < self.loadqueue.length; i++) {
                self.loadqueue[i]();
            }
        }
    }
};

//Internal method which handles a channel message publication and dispatches
//to the appropriate handler (it's either a job submission or a job completion)
Queue.prototype.handleMessage = function(channel, message) {
    if(channel === this.completion_channel_name) {
        if(!message || !message.jobid || (!message.result && !message.error)) {
            this.logger.warn("Unknown message received on queue channel: "+JSON.stringify(message));
            return;
        }
        this.handleCompletion(message.jobid, message.error, message.result);
        this.process();
    } else if(channel === this.submission_channel_name) {
        this.process();
    }
};

//Internal method to handle completion publication messages
//This checks to see if any local callbacks are pending for the job ID
//if so, they are called with the published error & results
Queue.prototype.handleCompletion = function handleCompletion(jobid, err, result) {
    var callbacks = this.completion_callbacks[jobid];
    delete this.completion_callbacks[jobid];
    if(!callbacks) {
        return;
    }
    for(var i = 0; i < callbacks.length; i++) {
        callbacks[i](err, result);
    }
};

//Submit a job to the queue
//jobid - a name that uniquely defines the job. If a job exists in the queue with the same jobid, this job will replace it!
//data - an object with data to pass to the job. Will be stored in redis as a JSON blob
//opts - optional parameters for the job: priority, delay
//callback - called when job submission completes, or if an error occurs during submission or execution
Queue.prototype.submitJob = function submitJob(jobid, data, opts, callback) {
    var self = this;
    if(!this.scriptsReady) {
        this.loadqueue.push(function() {
            self.submitJob(jobid, data, opts, callback);
        }); 
    }
    opts = opts || {};
    var callbacks = this.completion_callbacks[jobid];
    if(!callbacks) {
        callbacks = this.completion_callbacks[jobid] = [];
    }
    this.redis.evalsha(this.addJobScriptSHA, 0, jobid, data, opts.priority || 0, function(err) {
        if(err) {
            return callback(err);
        } else {
            self.redis.publish(self.submission_channel_name, jobid);
            callbacks.push(callback);
        }
    }); 
};

//Find a job in this queue with the given ID
Queue.prototype.findJob = function findJob(jobid, callback) {
    this.redis.get(this.prefix+":"+this.name+":"+jobid, callback);
};


//Remove a job in this queue with the given ID
//All callbacks for the job will be removed as well.
Queue.prototype.removeJob = function removeJob(jobid, callback) {
    delete this.completion_callbacks[jobid];
    var self = this;
    if(!this.scriptsReady) {
        this.loadqueue.push(function() {
            self.removeJob(jobid, callback);
        }); 
    }
    this.redis.evalsha(this.removeJobScriptSHA, 0, jobid, callback); 
};

//Attempt to fetch and handle a job. If we get a job, try to get another one right away
Queue.prototype.process = function process() {
    var self = this;
    if(!this.handler) return;
    this.redis.evalsha(this.getNextJobScriptSHA, 0, function(err, job) {
        if(err) {
            self.log("Error getting next job");
            self.log(err);
            return;
        }
        if(job) {
            self.handler(job, function(err, result) {
                self.redis.publish(self.completion_channel_name, {
                    jobid: job.uuid,
                    error: err,
                    result: result
                });
            });
            //Since we got a job, we might be able to run more, so try getting another.
            self.process();
        }
    });
};

//Job is hash with fields:
//  group - the concurrency group of the job (i.e…. the named queue)
//  priority - relative priority of the job. lower priorities will all be run before higher priorities, respecting delay
//  runtime - the time after which the job may be run
//  data - a generic JSON payload passed back to the worker
//  created_at - first added to queue since not being present
//  updated_at - last changed after add, before run

//EXAMPLE KEYS
//Queue Name - sample
//Job Names: jobA (priority 1), jobB (priority 0), jobC (priority 1)
//Priorites: 2
//
//info: qred:sample:maxjobs 5
//hashes: qred:sample:jobA, qred:sample:jobB, qred:sample:jobC
//queues: zset qred:samples:0 (jobB, score: 133456789)
//queues: zset qred:samples:1 (jobA, score: 133456788, jobC: score: 133456792)
//active queue: qred:samples:active ()

//keys[0] = jobkey
//argv[1] = now_ms
//Pseudocode
//
/*jshint laxbreak:true*/
var addJobScript = ""
+"priority = redis.call('hget', ARGV[0], 'priority')"
+""
+"}"
+"";

//keys[0] jobkey
//keys[1] queuekey
//For priority 0 -> maxprio
//    zrem qred:samples:priority jobid
var remJobScript = ""
+"";

//argv[0] = now
//Pseudocode:
//if scard qred:sample:active >= maxjobs return null
//For priority 0 -> maxprio
//   job = get minimum scored job greater than now
//   if job delete the job and remove from queue and return job
var getNextJobScript = ""
+"";


