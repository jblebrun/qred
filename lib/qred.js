/*jshint es5:true*/
function Queue(redis, redis_subscribe, opts) {
    opts = opts || {};
    this.logger = opts.logger || {};
    this.handler = opts.handler || null;
    this.concurrency = opts.concurrency || 1;
    this.active = 0;
    this.redis = redis;
    this.prefix = opts.prefix || "qred";
    this.name = opts.name || "queue";
    this.processors = [];
    this.loadqueue = [];
    this.completion_callbacks = {};
    this.completion_channel_name = [this.prefix,this.name,'finished'].join(':');
    this.submission_channel_name = [this.prefix,this.name,'submitted'].join(':');
    this.queuekey = [this.prefix,this.name,'queued'].join(':');
    this.delayqueuekey = [this.prefix,this.name,'delayed'].join(':');
    this.datahashkey = [this.prefix,this.name,'data'].join(':');
    this.priorityhashkey = [this.prefix,this.name,'priority'].join(':');
    this.runtimehashkey = [this.prefix,this.name,'runtime'].join(':');
    this.cratedhashkey = [this.prefix,this.name,'created'].join(':');
    redis_subscribe.subscribe(this.completion_channel_name, function(err) {
        if(err) throw new Error("Couldn't subscribe to redis completion channel");
    });
    redis_subscribe.subscribe(this.submission_channel_name, function(err) {
        if(err) throw new Error("Couldn't subscribe to redis submission channel");
    });
    redis_subscribe.on('message', this.handleMessage.bind(this));
    this.submitScripts();
}

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

    /*jshint evil:true*/

    self.redis.eval(scripts.addJob, 6, 
                    this.queuekey, this.delayqueuekey, 
                    this.datahashkey, this.priorityhashkey, this.runtimehashkey, this.createdhashkey,
                    jobid, data, opts.priority || 0, Date.now(), opts.delay || 0, 
                    function(err) {
        var callbacks = self.completion_callbacks[jobid];
        if(!callbacks) {
            callbacks = self.completion_callbacks[jobid] = [];
        }
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
    /*jshint evil:true*/
    this.redis.eval(scripts.removeJob, 4,
                    this.datahashkey, this.priorityhashkey, this.runtimehashkey, this.createdhashkey,
                    jobid, 
                    callback); 
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
    /*jshint evil:true*/
    this.redis.eval(scripts.removeJob, 6,
                    this.queuekey, this.delayqueuekey, 
                    this.datahashkey, this.priorityhashkey, this.runtimehashkey, this.createdhashkey,
                    jobid, 
                    callback); 
};

//Attempt to fetch and handle a job. If we get a job, try to get another one right away
Queue.prototype.process = function process() {
    var self = this;
    if(!this.handler) return;
    /*jshint evil:true*/
    this.redis.eval(scripts.getNextJob, 6, 
                    this.queuekey, this.delayqueuekey, 
                    this.datahashkey, this.priorityhashkey, this.runtimehashkey, this.createdhashkey,
                    Date.now(),
                    function(err, job) {
        if(err) {
            self.log("Error getting next job");
            self.log(err);
            return;
        }
        if(job && self.active < self.concurrency) {
            self.active++;
            self.handler(job, function(err, result) {
                self.active--;
                //Remove job?
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
    /*jshint laxbreak:true*/
    //Pseudocode
    //If job exists: remove from queues,
    //Set job key hash fields to input data
    //If a delay is specified, add job to delay queue, and record its priority in a hash
    //Else, add job to main queue with specified priority as score
    addJob: ""
        +"local kQueue = KEYS[1] "
        +"local kDelayQueue = KEYS[2] "
        +"local kDataHash = KEYS[3] "
        +"local kPriorityHash = KEYS[4] "
        +"local kRuntimeHash = KEYS[5] "
        +"local kCreatedHash = KEYS[6] "
        +"local jobid = ARGV[1] "
        +"local data = ARGV[2] "
        +"local priority = ARGV[3] "
        +"local now_ms = ARGV[4] "
        +"local delay = ARGV[5] "
        +"local run_at = now_ms + delay "
        +"redis.call('hset', kDataHash, jobid, data) "
        +"redis.call('hset', kPriorityHash, jobid, priority) "
        +"redis.call('hset', kRuntimeHash, jobid, run_at) "
        +"redis.call('hset', kCreatedHash, jobid, now_ms) "
        +"if delay then "
        +"  redis.call('zadd', kDelayQueue, run_at, jobid) "
        +"  redis.call('zrem', kQueue, jobid) "
        +"else "
        +"  redis.call('zadd', kQueue, priority) "
        +"  redis.call('zrem', kDelayQueue, jobid) "
        +"endif "
        +"",

    //Pseudocode
    //Remove from delay queue
    //Remove from queue
    //Get all job fields
    //Delete the job
    //Return job fields
    removeJob: ""
        +"local kQueue = KEYS[1] "
        +"local kDelayQueue = KEYS[2] "
        +"local kDataHash = KEYS[3] "
        +"local kPriorityHash = KEYS[4] "
        +"local kRuntimeHash = KEYS[5] "
        +"local kCreatedHash = KEYS[6] "
        +"local jobid = ARGV[1] "
        +"redis.call('hdel', kDataHash, jobid) "
        +"redis.call('hdel', kPriorityHash, jobid) "
        +"redis.call('hdel', kRuntimeHash, jobid) "
        +"redis.call('hdel', kCreatedHash, jobid) "
        +"redis.call('zrem', kQueue, jobid) "
        +"redis.call('zrem', kDelayQueue, jobid) "
        +"return result; "
        +"",

    //Pseudocode
    //Get all job fields
    //Return job fields
    getJob: ""
        +"local kDataHash = KEYS[1] "
        +"local kPriorityHash = KEYS[2] "
        +"local kRuntimeHash = KEYS[3] "
        +"local kCreatedHash = KEYS[4] "
        +"local jobid = ARGV[1] "
        +"local priority = redis.call('hget', kPriorityHash, jobid) "
        +"local data = redis.call('hget', kPriorityHash, jobid) "
        +"local created_at = redis.call('hget', kCreatedHash, jobid) "
        +"local runtime = redis.call('hget', kRuntimeHash, jobid) "
        +"return {jobid=jobid, priority=priority, data=data, created_at=created_at, runtime=runtime} "
        +"",

    //Return the next queued job
    //Each time this function is called, 
    //The delay queue is checked for promotable delayed jobs
    //Pseudocode:
    //get all keys in delay queue that are now runnable
    //move those keys into the main queue
    //record the lowest time in the delay queue
    //get the first job in the queued jobs list
    //add that key to the active list
    //remove that key from the queue list
    //return [jobid, next_delay_check]
    getNextJob:""
        +"local kQueue = KEYS[1] "
        +"local kDelayQueue = KEYS[2] "
        +"local kDataHash = KEYS[3] "
        +"local kPriorityHash = KEYS[4] "
        +"local kRuntimeHash = KEYS[5] "
        +"local kCreatedHash = KEYS[6] "
        +"local now_ms = ARGV[1] "
        +"local promoting = redis.call('zrangebyscore', 0, now_ms) "
        +"redis.call('zremrangebyscore', 0, now_ms) "
        +"for jobid in promoting do "
        +"  local priority = redis.call('hget', kPriorityHash, jobid) "
        //TODO - can we turn this into a bulk zadd?
        +"  redis.call('zadd', kQueue, priority, jobid) "
        +"end "
        +"local jobid = redis.call('zrange', 0, 1) "
        //Return the next queued job
        +"if jobid then "
        +"  local priority = redis.call('hget', kPriorityHash, jobid) "
        +"  local data = redis.call('hget', kPriorityHash, jobid) "
        +"  local created_at = redis.call('hget', kCreatedHash, jobid) "
        +"  local runtime = redis.call('hget', kRuntimeHash, jobid) "
        +"  return {jobid=jobid, priority=priority, data=data, created_at=created_at, runtime=runtime} "
        +"else " 
        //If no jobs are ready, return the time until the next delayed job is ready
        +"    return redis.call('zrange', 0, 1, 'withscores') "
        +"endif "
};
