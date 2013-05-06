A redis-backed job queue system that doesn't suck

To run tests: 
```
    npm install redis
    node test/simple.js
```


A specific queue is identified by the tuple 
```(redis_client, redis_subcscription_client, queue_prefix, queue_name)```

It will be important to remember to use the same parameter values when instantiating a queue with the same identifying tuple. Any time different parameters are provided, they will override the old ones. Each queue has one handler that processes jobs locally. If you do not specify a handler for the queue, the created queue object will not process jobs, but can be used to submit them. We may consider splitting queue managers and queue processors into two separate objects for clarity. 

Queue commands:
````
qred.submitJob(jobid, data, opts, callback)
```
* jobid - a string that uniquely identifies a job. If a job with the specified id exists, it will be *replaced*.
* data - a generic blob that represents the data that the job handler will receive when the job is ready to be run.
* opts - job specific settings. Currently 'priority', a relative value that determines the order in which jobs are run relative to one another, and 'delay', which specifies a delay in ms that the job should be queued before being run. 
* callback - the callback that will be fired when a handler has finished executing this job. 


```
qred.findJob(jobid, callback) 
```
Find a job in the queue with the specified ID. 

```
qred.removeJob(jobid, callback) 
```
Remove a job from the queue. Active jobs will not be cancelled, but local callbacks for the job ID will not be fired. Remote callbacks currently *will* still execute, although this may change in the future

```
qred.pause() 
```
Stops the handler from searching for and accepting jobs


```
qred.unpause()
``` 
Start listening for and processing jobs again 


Internal methods:
```
_process()
```
Pull the next job from the queue and run it if one exists. If no job exists, but delayed jobs exist, the time until the next job can be run will be returned. The processor will then sleep for that amount of time.

```
_handleMessage(channel, message)
```
Handle a message from the redis subscription client. The two redis message types are "submitted" and "completed". Receive a "submitted" message triggers a _process() run, while receiving a "completed" message triggers _handleCompletion and then a _process run.

```
_handleCompletion(jobid, err, result) 
```
Handle a message that a job processor on this queue has finished processing a job. All local callbacks for the job id will be executed with the specified err, result values, and then removed from the local callback cache.
