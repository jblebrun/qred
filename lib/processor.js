var fs = require("fs");
var path = require("path");

/*jshint es5:true*/
function Processor(opts) {
    if(!opts) {
        throw new Error("Must provide options name, rclient, rsclient");
    }
    if(!opts.name) throw new Error("Must provide a queue name");
    /*jshint evil:true*/
    if(!opts.redis || !opts.redis.publish || !opts.redis.eval) throw new Error("Must provide option 'redis' which is a redis client that offers the commands: publish, eval"); 
    if(!opts.subscriber || !opts.subscriber.subscribe || !opts.subscriber.on) throw new Error("Must provide an options 'subscriber' which is a redis client that offers the commands: subscribe, on('message')"); 

    //Log helper
    this.log= opts.log|| function() {};

    //Local job handler settings
    this.handler = opts.handler || null;
    this.concurrency = opts.concurrency || 1;

    //Global concurrency settings
    this.globalconcurrency = opts.globalconcurrency || 0;
    this.maxperperiod = opts.maxperperiod || 0;
    this.period = opts.period || 0;

    //Initialize internal state
    this.active = 0;
    this.paused = false;

    this.completion_callbacks = {};

    this.redis = opts.redis;

    //Redis key names
    this.prefix = opts.prefix || "qred";
    this.name =  opts.name;
    
    this.completion_channel_name = [this.prefix,this.name,'finished'].join(':');
    this.submission_channel_name = [this.prefix,this.name,'submitted'].join(':');
    this.queuekey = [this.prefix,this.name,'queued'].join(':');
    this.delayqueuekey = [this.prefix,this.name,'delayed'].join(':');
    this.datahashkey = [this.prefix,this.name,'data'].join(':');
    this.optshashkey = [this.prefix,this.name,'opts'].join(':');

    //Set up subsriptions
    opts.subscriber.subscribe(this.completion_channel_name, this.submission_channel_name, function(err) {
        if(err) throw new Error("Couldn't subscribe to redis completion or submission channel").cause(err);
    });
    opts.subscriber.on('message', this._handleMessage.bind(this));
    opts.subscriber.on('subscribe', this._handleMessage.bind(this));
}

//Internal method which handles a channel message publication and dispatches
//to the appropriate handler (it's either a job submission or a job completion)
Processor.prototype._handleMessage = function(channel) {
    if(channel === this.completion_channel_name) {
        this._process();
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
//Attempt to fetch and handle a job. If we get a job, try to get another one right away
Processor.prototype._process = function process() {
    var self = this;
    if(!this.handler) return;
    if(this.paused) return;
    if(self.active >= self.concurrency) return;
    self.active++;
    /*jshint evil:true*/
    this.redis.eval(scripts.getNextJob, 4, 
                    this.queuekey, this.delayqueuekey, 
                    this.datahashkey, this.optshashkey,
                    Date.now(),
                    function(err, result) {
        self.active--;
        if(err) {
            self.log("Error getting next job");
            self.log(err);
            return;
        }
        if(result) {
            if(result instanceof Array && result.length === 3) {
                var jobid = result[0];
                var data = result[1];
                var jobopts = result[2];
                try {
                    data = JSON.parse(result[1]);
                } catch(err) {
                }
                var job = {
                    data: data,
                    priority: jobopts[0],
                    delay: jobopts[1],
                    nx: jobopts[2],
                    autoremove: jobopts[3]
                };

                self.active++;
                self.handler(job, function(err, result) {
                    self.active--;
                    //Remove job?
                    self.redis.publish(self.completion_channel_name, JSON.stringify({
                        jobid: jobid,
                        error: err,
                        result: result
                    }));
                });
                //Since we got a job, we might be able to run more, so try getting another.
                self._process();
            } else if(typeof result === "string") {
                var delay = parseInt(result, 10) - Date.now();
                setTimeout(self._process.bind(self), delay);
            } else {
                console.log("Error: unknown result "+result);
            }
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
    getNextJob: fs.readFileSync(path.join(__dirname, "..", "lua", "getNextJob.lua"))
};

module.exports = Processor;
