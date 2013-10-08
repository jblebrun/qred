var fs = require("fs");
var path = require("path");
var events = require('events');
var util = require('util');

function Qred(opts) {
    if(opts.abstract) return;
    if(!opts) {
        throw new Error("Must provide options name, rclient, rsclient");
    }

    events.EventEmitter.call(this);

    if(!opts.name) throw new Error("Must provide a queue name");
    /*jshint es5:true evil:true*/
    if(!opts.redis || !opts.redis.publish || !opts.redis.eval) throw new Error("Must provide option 'redis' which is a redis client that offers the commands: publish, eval"); 
    if(!opts.subscriber || !opts.subscriber.subscribe || !opts.subscriber.on) throw new Error("Must provide an options 'subscriber' which is a redis client that offers the commands: subscribe, on('message')"); 

    //Log helper
    this.log= opts.log|| function() {};

    this.redis = opts.redis;

    //Redis key names
    this.prefix = opts.prefix || "qred";
    this.name = opts.name;

    this.completion_channel_name = this.rkey('f');
    this.submission_channel_name = this.rkey('s');
    this.queuekey = this.rkey('jq');
    this.delayqueuekey = this.rkey('dq');
    this.delayprioritieskey = this.rkey('dp');
    this.livesetkey = this.rkey('ls');
    this.activesetkey = this.rkey('as');
    this.completesetkey = this.rkey('cs');

    this.logerr = (function(err) {
        if(err) this.log(err);
    }).bind(this);
    this.logerr(null);
}

module.exports = Qred;

util.inherits(Qred, events.EventEmitter);

Qred.prototype.jobKey = function(jobid) {
    return this.rkey('j'+":"+jobid);
};

Qred.prototype.progressKey = function(jobid) {
    return this.rkey('jp'+":"+jobid);
};

Qred.prototype.rkey = function(suffix) {
    return this.prefix+":"+this.name+":"+suffix;
};

Qred.prototype.markProgress = function(jobid, info, callback) {
    this.redis.rpush(this.progressKey(jobid), new Date()+" "+info, callback);
};

Qred.prototype.clearProgress = function(jobid, callback) {
    this.redis.del(this.progressKey(jobid), callback);
};

Qred.prototype.getProgress = function(jobid, callback) {
    this.redis.lrange(this.progressKey(jobid), 0, -1, callback); 
};

//Remove a job in this queue with the given ID
//All callbacks for the job will be removed as well.
Qred.prototype.removeJob = function removeJob(jobid, callback) {
    /*jshint es5:true evil:true*/
    this.redis.eval(scripts.removeJob, 7,
                    this.queuekey, this.delayqueuekey, this.activesetkey, this.completesetkey, this.livesetkey, this.delayprioritieskey, this.jobKey(jobid),
                    jobid,
                    callback); 
};
var scripts = {
    removeJob: fs.readFileSync(path.join(__dirname, "..", "lua", "removeJob.lua"))
};
