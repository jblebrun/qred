A redis-backed job queue system that doesn't suck

To run tests: 
```
    npm install redis
    node test/simple.js
```


A specific queue is identified by the tuple 
```(redis_client, redis_subcscription_client, queue_prefix, queue_name)```

It will be important to remember to use the same parameter values when instantiating a queue with the same identifying tuple. Any time different parameters are provided, they will override the old ones. Each queue has one handler that processes jobs locally. If you do not specify a handler for the queue, the created queue object will not process jobs, but can be used to submit them. 

There are two interfaces into the queue: Processor, and Manager. A Processor receives messages about new job submissions, and pulls jobs from the queue to handle them. The Manager is an interface to submit jobs to the queue and query it.

Queue Manager
=============

```
new qred.Manager(opts)
````
Create a queue manager object that can be used to submit jobs to the queue.
Required Options:
* name - A string identifying the queue
* redis - A redis client that provides "eval" and "publish"
* subscriber - A redis client that provides "subscribe" and "on"

Optional Options:
* log - a function that will be used to log debug messages

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

Internal Methods
----------------

```
_handleMessage(channel, message)
```
Handle a message from the redis subscription client. The two redis message types are "submitted" and "completed". Managers only listen for the "completed" messages, and when received, trigger a _handleCompletion call, if the message is valid.

```
_handleCompletion(jobid, err, result) 
```
Handle a message that a job processor on this queue has finished processing a job. All local callbacks for the job id will be executed with the specified err, result values, and then removed from the local callback cache.


Queue Processor
===============

```
new qred.Processor(opts)
````
Create a queue process object that can be used to listen for and handle jobs 

Required Options:
* name - A string identifying the queue
* redis - A redis client that provides "eval" and "publish"
* subscriber - A redis client that provides "subscribe" and "on"
* handler - A function that accepts (err, data) and performs the job of tasks in this queue

Optional Options:
* log - a function that will be used to log debug messages
* concurrency - the number of jobs to run locally at once

```
qred.pause() 
```
Stops the handler from searching for and accepting jobs


```
qred.unpause()
``` 
Start listening for and processing jobs again 


Internal Methods
----------------
```
_process()
```
Pull the next job from the queue and run it if one exists. If no job exists, but delayed jobs exist, the time until the next job can be run will be returned. The processor will then sleep for that amount of time.

```
_handleMessage(channel, message)
```
Handle a message from the redis subscription client. The two redis message types are "submitted" and "completed". Receiving either kind triggers a _process() run.
