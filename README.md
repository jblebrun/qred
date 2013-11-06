A redis-backed job queue system 

To run tests: 
```
    npm install redis
    npm test
```


A specific queue is identified by the tuple 
```(redis_client, redis_subcscription_client, queue_prefix, queue_name)```

It will be important to remember to use the same parameter values when instantiating a queue with the same identifying tuple. Any time different parameters are provided, they will override the old ones. Each queue has one handler that processes jobs locally. If you do not specify a handler for the queue, the created queue object will not process jobs, but can be used to submit them. 

There are two interfaces into the queue: Processor, and Manager. A Processor receives messages about new job submissions, and pulls jobs from the queue to handle them. The Manager is an interface to submit jobs to the queue and query it.

# Queue Manager

TODO - add support for "requeue" if a job is submitted while a job is already in the active state. This would result in the submitting job getting put in a separate "rerun" queue, and when the processor finishes, it will look in this queue and pop a job back onto the queue if it's there.

```
new qred.Manager(opts)
```
Create a queue manager object that can be used to submit jobs to the queue.
Required Options:
* name - A string identifying the queue
* redis - A redis client that provides "eval" and "publish"
* subscriber - A redis client that provides "subscribe" and "on"

Optional Options:
* log - a function that will be used to log debug messages generated in the qred code
* prefix - the prefix for the keys in redis. Defaults to "qred"

```
qred.submitJob(jobid, data, opts, submitted, completed)
```
Submit a new job to the queue, identified by the string `jobid`. Only one job for a particular job ID can exist in the queue. Submitting a new job with the same ID will result in either replacing the job or discarding it, depending on the value of the `nx` option. If the job was added (or replaced), the function returns an array [1,delayed,queued,active,complete], where the named values are the number of jobs in that state. If the new job was discarded, [0,status] is returned, where status is the status of the existing job.

* jobid - a string that uniquely identifies a job. If a job with the specified id exists, it will be *replaced*.
* data - a generic blob that represents the data that the job handler will receive when the job is ready to be run.
* opts - job specific settings. Possible options:
  * 'priority', a relative value that determines the order in which jobs are run relative to one another
  * 'delay', specifies a delay in ms that the job should be queued before being run. 
  * 'nx', a value indicating when to replace an existing job with the same ID. This value is a string of characters representing the first character of possible job statuses. So for example, if you pass in nx='dq', any job with the provided ID that is in the "delayed" or "queued" state will be replaced with the provided job. If the job with the given ID is in the active or complete state, the provided job will simply be ignored.
  * 'autoremove', The time after which to autoclean job data from the queue data structures
* callback - called back when the job has been successfully submitted to the redis queue, or with an error if one occurred

```
qred.findJob(jobid, callback) 
```
Find a job in the queue with the specified ID. Returns an object with the job options and data that were passed in. The `id` field is populated with the job id, for convenience.

```
qred.removeJob(jobid, opts, callback) 
```
Remove a job from the queue. Active jobs will not be cancelled, but local callbacks for the job ID will not be fired. Remote callbacks currently *will* still execute, although this may change in the future

* jobid - a string that uniquely identifies a job. If a job with the specified id exists, it will be *replaced*.
* opts - job specific settings. Possible options:
  * 'nx', a value indicating when to remove the job. This value is a string of characters representing the first character of possible job statuses. So for example, if you pass in nx='dq', any job with the provided ID that is in the "delayed" or "queued" state will not be removed. 

```
qred.reconcile(callback)
```
A helper function to clean out stale queue data. If a process using qred is quit suddenly, or the redis database is modified manually, the structures that maintain qred data could be corrupted. This function goes through and removes partial data elements, restoring the queue to a clean state.

## Internal Methods

```
_handleMessage(channel, message)
```
Handle a message from the redis subscription client. The two redis message types are "submitted" and "completed". Managers only listen for the "completed" messages, and when received, trigger a _handleCompletion call, if the message is valid.

```
_handleCompletion(jobid, err, result) 
```
Handle a message that a job processor on this queue has finished processing a job. All local callbacks for the job id will be executed with the specified err, result values, and then removed from the local callback cache.

## Events
```
complete (jobid, err, result)
```
This is fired every time a job is completed. Any registered event handlers receive the message that redis published on completion. The event is fired for all managers any time any processor in the same queue space finishes a job. 

```
complete:<jobid> (jobid, err, result)
```
This is fire when a job named jobid is completed. This makes it easy to register an event handler for a particular named job without having to register a handler that hears every job. The event is fired for all managers any time a processor in the same queue space completes a job with the given name.

# Queue Processor

TODO - add support for detecting incomplete active jobs, where the processor died while performing the job, so that the job can be requeued, or clean up can occur.

TODO - add support for "requeue" if a job is submitted while a job is already in the active state. This would result in the submitting job getting put in a separate "rerun" queue, and when the processor finishes, it will look in this queue and pop a job back onto the queue if it's there.


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

## Events
```
complete
```
This is fired every time a job is completed by this processor.

```
complete:<jobid>
```
This is fired when a job named jobid is completed by this processor. This makes it easy to register an event handler for a particular named job without having to register a handler that hears every job. 

## Internal Methods
```
_process()
```
Pull the next job from the queue and run it if one exists. If no job exists, but delayed jobs exist, the time until the next job can be run will be returned. The processor will then sleep for that amount of time.

```
_clean()
```
Go through completed jobs and check if any of them have expired (based on their autoremove values), and if so, remove the data from the queue data structures.

```
_handleMessage(channel, message)
```
Handle a message from the redis subscription client. The two redis message types are "submitted" and "completed". Receiving either kind triggers a _process() run.

```
_process()
```
Pluck the next jobid to be run. 

```
_handleJob(jobid)
```
Run the job, and then update its status according and publish the message.

