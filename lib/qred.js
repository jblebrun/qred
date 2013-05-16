function Qred(opts) {
    if(opts.abstract) return;
    if(!opts) {
        throw new Error("Must provide options name, rclient, rsclient");
    }

    if(!opts.name) throw new Error("Must provide a queue name");
    /*jshint evil:true*/
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
    this.datahashkey = this.rkey('j');
    this.optshashkey = this.rkey('o');

}

module.exports = Qred;

Qred.prototype.job = function(jobid) {
    return this.rkey('j'+":"+jobid);
};

Qred.prototype.rkey = function(suffix) {
    return this.prefix+":"+this.name+":"+suffix;
};

