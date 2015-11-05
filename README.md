Dynamiq
=========

A simple implementation of a queue on top of Riak 2.0

Disclaimer
==========

Dynamiq is currently in use in production at Tapjoy. However, the product is still considered to be a beta offering, which comes with no guarantees around the interfaces or behaviors provided in whatever the "current" version is.

We encourage interested parties to use Dynamiq, understanding that prior to an official v1 release, things could change.

There is currently a Version 2 update underway, which you can check out [here](https://github.com/Tapjoy/dynamiq/tree/feature/httpapi_v2)

Caveat Emptor

What is Dynamiq?
==========

Dynamiq is a distributed databag, acting as a queue. It's written in golang, and run in a cluster on top of Riak 2.0.

It exposes a simple REST API for publishing to topics and directly enqueueing to queues, as well as receiving batches of messages at a time.

Dynamiq acts as both a simple queueing application, as well as a topic-fanout system. Simply, you can create topics and queues, subscribe queues to topics, and publish messages either to topics (which will fan out to all of their queues) or to queues directly (which will only enqueue to that specific queue).

It provides at-least once delivery semantics, which are governed by "partitions" - slices of the overall total range of all possible keys, broken up by the number of nodes in the cluster, each node holding the configured number of partitions for that queue.

When a batch of messages are received from Dynamiq, the partition they came from is considered locked-out for delivery, which will expire once the Visibility Timeout on that queue expires. When that timeout expires, any un-acknowledged messages are available to be served again.

At-Least-Once and De-Duplication
===========

Dynamiq has the possibility of sending duplicate messages. This possibility increases during certain significant events, such as changing the number of partitions in a given queue.

You should account for this in your design by either managing your own de-dupe solution (such as using Memcache to hold the keys you've seen from a given queue, expiring with the visibility timeout on the queue) or design a system which self-defends against duplicate messages.

The former is likely a more realistic approach than the latter

Why Dynamiq?
==========

The main design ideal of Dynamiq is to act as a drop-in replacement for the Amazon's SNS / SQS services, which can become expensive at scale, both in terms of price as well as latency.

Getting Started
=========

Prerequisites
-------------
To get Dynamiq up and running, you need to have a few pre-requisites installed.

First, you'll need golang installed. You can find the instructions [here](https://golang.org/doc/install).

If you've never worked with golang before, you should take some time to familiarize yourself with how go expects to work from a development standpoint, with regard to the GOROOT / GOPATH environment variables and where your go code is actually located on disk.

Second, you'll need the code for Dynamiq itself. Because of golang's highly opinionated nature, you should avoid cloning the Dynamiq repo directly, and instead use go's built in cloning mechanism, "go get".

```
go get github.com/Tapjoy/dynamiq
```

Third, you need an installation of Riak 2.0 up and running somewhere (preferably local, for testing / development). You can find guides on how to install Riak 2.0 for your particular operating system [here](http://docs.basho.com/riak/latest/quickstart/)

Finally, you need to create and enable certain bucket types in Riak 2.0. This is taken care of for you in the setup.sh script provided by Dynamiq.

```
sh ./setup.sh
```

Configuring Dynamiq
---------------

Dynamiq comes with a sample config in lib/config.gcfg. This is considered "good enough" for local testing, but may require tweaks for use in production or test environments. Here is a description of each setting, and an example of valid values

Core
------
* name - The name of the current node. It's important that this name be in the same format as the names in the "seedserver" option, which is a hostname or ip_address
* port - The port it will listen on for incoming membership traffic
* seedserver - A comma-delimited list of additional nodes in the cluster. This uses [hashicorp/memberlist](http://github.com/hashicorp/memberlist) which utilizes a modified SWIM protocol for node discovery. These should be hostnames or IP addresses that can be discovered over the network. You can include the current server in this list - Dynamiq will filter it out if found.
* seedport - The port to talk to other memberlist nodes over
* httpport - The port to server HTTP traffic over
* riaknodes - A comma-delimited list of Riak nodes to speak to
* backendconnectionpool - How many riak connections to open and keep in waiting
* syncconfiginterval - The period of time in seconds in which Dynamiq waits before attempting to update it's internal config based on changes in the configuration stored in Riak. A lower settings means dynamiq will be more frequently refresh it's internal config
* loglevelstring -  Any value of debug | info | warn | error. Sets the logging level internally

Stats
-------

* type - Any value of statsd | none. Set to none to disable stats tracking
* flushinterval - Number of seconds to hold data in memory before flushing to disk
* address - Address + Port of the Statsd compatible endpoint you wish to talk to
* prefix - A prefix to apply to all of your metrics to better cluster them. This is passed through to the statsd client itself, and is not applied directly in Dynamiq code

Running Dynamiq Locally
---------------

Now that you have the pre-reqs installed, it's time to run Dynamiq. You can compile and run Dynamiq easiest by invoking it from go directly each time. Navigate into the Dynamiq directory ($GOPATH/src/github.com/Tapjoy/dynamiq) and run the following:

```
go run dynamiq.go
```

If all is well, you should see log messages about booting up, and "syncing" with Riak. This means you're ready to begin using Dynamiq! Please continue on to the API section to learn how to create and utilize topics and queues in Dynamiq.

Note: you will likely also see a WARN log entry about failing to resolve a host. For local, single node testing this is expected.

Running Dynamiq in a Cluster
----------------

Eventually, you'll want to run Dynamiq in a cluster in a production or testing environment.

You'll likely want to front your cluster with something like HAProxy or Nginx. How to do such a thing is out of scope for this document.

You'll need to provide Dynamiq with the address of at least one other node in the "seedserver" field. Keep the following recommendations in mind when providing these values:

* It is better to provide the entire cluster, as opposed to a single node, so that Dynamiq can attempt to talk to additional nodes if the first in the list is unavailable initially
* You can, for the sake of convenience, put the current node in this list so long as you have provided the same value in the "name" value. This is because Dynamiq will filter itself from the list, and prioritize the node immediately following ours in the list, alphabetically speaking. This is to minimize the impact of all nodes trying to talk to the same node at once as a single point of failure
* You can bring nodes up individually, and so long as the 2nd and beyond nodes are able to talk to any of the ones before them, they will all successfully get information about the state of the entire cluster in (very short) time.

You will likely want to limit each Dynamiq node to it's own Riak node, instead of providing them a whole list to try. Due to performance reasons, it's often reasonable to run both Dynamiq and Riak on the same node. You may also consider running Nginx or HAProxy between Dynamiq and Riak, allowing for the former to fail over to a remote instance of the latter in the worst possible case.

Under log level "debug" you will likely see a lot of spam from Martini, the web framework we use. Consider setting it to info or error once you're comfortable with the data you see in debug.

Dynamiq natively provides no security, authentication, or authorization services. Take care to ensure that only trusted servers are able to access your Dynamiq cluster.

REST API
============

Dynamiq supports a REST API for communicating between clients and Dynamiq. The current API (v1) is not perfectly RESTful in its nature, due to it's evolving naturally and quickly during the initial development of Dynamiq. Some endpoints return a simple string, others return a JSON object.

It can, however, be considered "Stable", in that we will only continue to add new routes which behave like old ones, and will not modify or remove existing v1 routes or behaviors.

An overhauled v2 of this API, containing more RESTful routes and a consistent response object is planned.

## Basic Topic / Queue Operations

### GET /topics

* Response Code: 200
* Response: a JSON object containing the key "topics" and the value as a list of strings containing the topic names
* Result: Successfully retrieved a list of all known topics

### GET /topics/:topic_name

* Response Code: 200
* Response: a JSON array with the list of subscribed Queues as strings
* Result: Successfully retrieved a list of Queues mapped to this Topic

### PUT /topics/:topic_name

* Response Code: 201
* Response: a string containing the phrase "created"
* Result: The topic was created successfully

--------------------

* Response Code: 422
* Response: a JSON object containing the error "Topic already exists."
* Result: The topic already existed, was not modified

### DELETE /topics/:topic_name

* Response Code: 200
* Response: a JSON object containing the key "Deleted" and a value of true
* Result: The topic has been deleted. Queues that were subscribed to this topic will no longer receive data from it.

--------------------

* Response Code: 404
* Response: a JSON object containing the error "Topic did not exist."
* Result: The topic was not deleted as it did not exist with the provided name

### GET /queues

* Response Code: 200
* Response: a JSON object containing the key "queues" and the value as a list of strings containing the topic names
* Result: Successfully retrieved a list of all known queues

### GET /queues/:queue

* Response Code: 200
* Response: a JSON object representing the current configuration settings for that queue
* Result: Successfully retrieve information about the queue

----------------------

* Response Code: 404
* Response: a string with a message indicating there was no queue with the provided name
* Result: No queue was located

### PUT /queues/:queue_name

* Response Code: 201
* Response: a string containing the phrase "created"
* Result: The queue was created successfully

------------------------

* Response Code: 422
* Response: a JSON object containing the error "Queue already exists."
* Result: The queue already existed, was not modified

### DELETE /queue/:queue_name

* Response Code: 200
* Response: a JSON object containing the key "Deleted" and a value of true
* Result: The queue has been deleted. Topics will no longer send data to this queue.

-------------------------

* Response Code: 404
* Response: a JSON object containing the error "Queue did not exist."
* Result: The queue was not deleted as it did not exist with the provided name

## Publishing and Consuming

### PUT /queues/:queue_name/message

* Response Code: 200
* Response: a JSON string containing the ID of the message that enqueued. If no ID is returned, no message was enqueued
* Result: A message is enqueued (if an ID is returned) or not (if no ID is returned)

### PUT /topics/:topic_name/message

* Response Code: 200
* Response: a JSON object containing keys for every queue name subscribed to it, where the values are the IDs of the messages enqueued. If a queue is missing or contains an empty string, it did not receive the message
* Result: The message was broadcast to the queues subscribed to the topic

### GET /queues/:queue_name/messages/:batch_size

* Response Code: 200
* Response: a JSON array where each element is one message body, up to the amount specified in the request as the batch_size
* Result: A series of messages are returned to you, and the partition which governed their ID range is now considered locked for the duration of that queues visibility timeout

-----------------------

* Response Code: 204
* Response: a JSON string indicating that there were no available partitions to serve messages from
* Result: No messages are sent

------------------------

* Response Code: 404
* Response: a JSON string indicating that there was no queue with the provided name
* Result: No messages are sent

------------------------

* Response Code: 422
* Response: A string indicating there was a problem with the batchSize you attempted to provide
* Result: No messages are sent

------------------------

* Response Code: 500
* Response: A string indicating what the server error was. 500s are only explicitly thrown when there was an un-expected error in trying to retrieve the messages
* Result: No messages are sent, but there is potential for a partition to be locked.

### DELETE /queues/:queue_name/message/:ID

A note about deletes:

Because it is possible to try to delete a message which was already deleted, we do not throw any errors on an incorrect ID.

* Response Code: 200
* Response: true or false, depending on the existence of the message to be deleted
* Result: The message is either deleted (true) or did not exist (false)

## Configuration

### PUT /topics/:topic_name/queues/:queue_name

* Response Code: 200
* Response: a JSON object containing the key "Queues" and housing a list of all queues, including the one provided, subscribed to the provided topic.
* Result: The provided queue is subscribed to the provided topic, and will now receive any messages sent to the topic

--------------

* Response Code: 422
* Response: a JSON object containing an error that either the topic or queue did not exist
* Result: Nothing was created or subscribed together

### DELETE /topics/:topic_name/queues/:queue_name

* Response Code: 200
* Response: a JSON object containing the key "Queues" and housing a list of all queues, minus the provided one, subscribed to the provided topic
* Result: The provided queue was removed from the provided topics description list

### PATCH /queues/:queue_name/

A note about the configuration endpoint for queues:

Golang is very particular about its data types, specifically parsing them from JSON. Providing an integer value as a string will result in a nil value. Be very careful about providing the right datatypes in the right places.

#### Example Request Body

```json
{
  "visibility_timeout" : 2,
  "max_partitions" : 10,
  "min_partitions" : 1,
  "max_partition_age" : 426000,
  "compressed_messages" : false
}
```

* Response Code: 200
* Response a JSON string with the word "ok"
* Result: The provided values, where applicable, were successfully applied to the queue

#### Parameters

Here is a list of params that you can optionally include in a configuration update

* Visibility Timeout
 * Controls how long messages recently sent are considered "out" before becoming available to be re-sent. This is the primary timeout on the "at-least-once" aspect of Dynamiq
* Max Partitions
 * Controls the upper bound on the number of partitions used to divide and lock the messages being sent. A higher amount means more granularity in making messages available
* Min Partitions
  * Controls the lower bound on the number of partitions.
* Max Partition Age
 * Controls how long the system will let an "un-touched" (empty) partition exist before it considers it a waste of resources and lowers the partition count
* Compressed Messages
 * Dynamiq has the option of compressing messages on the way in, and on the way out, of buckets in Riak. This helps if you think space on disk or network traffic between Riak nodes is an issue. The current compression strategy is golangs ZLib implementation.


Changing any of these values will result in an immediate write to Riak ensuring the data is persisted, however the individual Dynamiq nodes (including the node you issued the request to) will not have their in memory configuration updated until the next "Sync" with Riak.

Dynamiq and Statistics
======================

Dynamiq has the ability to publish to any StatsD equivalent service a number of metrics around the useage of your queues. Currently, some of the metrics are not 100% accurate, but can still be used to get a relative baseline for your queues health. We are continuing to work on and improve how these counters are handled in Dynamiq

* Fill Rate: fill.count
 * For a given batch B, Fill Rate represents the % of B that was fulfilled by the request. For example, if B is 200, and the actual messages returned number 50, then Fill Rate is 25%
* Direct Depth : depth.count
 * Counts the number of messages in / out of Dynamiq with a direct counter
* Approximate Depth : approximate_depth.count
 * Estimates the relative depth by examining the fill rate of the last partition accessed
* Sent : sent.count
 * The number of messages sent into Dynamiq
* Received : received.count
 * The number of messages received by a consuming client of Dynamiq
* Deleted : deleted.count
 * The number of messages acknowledged by a consuming client of Dynamiq

Client Libraries
================

* Ruby - https://github.com/Tapjoy/dynamiq-ruby-client

Tuning
==========

When it comes to tuning Dynamiq, there are primarily 3 things to think about (which interplay with each other tightly).

* Visibility Timeout
* Max Partitions
* Batch Size (for the clients request messages)

As mentioned above, Visibility Timeout is how long a given partiton is considered "locked" or "in-use" once it has served some or all of its messages. This prevents duplicates from that partition being served, *but also prevents non-duplicate messages potentially still within that partition from being served as well*.

This is a very important aspect of tuning Dynamiq to be aware of - You could inadvertently deny yourself the ability to receive messages for the period of your timeout if you aren't careful.

One line of thinking says you could make your partition size and your batch size to be 1:1. This means you'd need to tune your max partitions such that each partition could theoretically only hold the number of messages you wish to pull with a single request. Your visibility timeout in this case would be the time it takes to complete one message times the batch size.

The downside to this approach is that you'll have a very large number of partitions, which will take longer for you to cycle through all un-served partitions and leave more messages in-waiting for longer.

An alternative approach is to tune for the least amount of time making non-duplicate messages unavailable. This can be accomplished with fewer numbers of overall partitions. Considering the time it takes to complete a single message, and having determined the correct batch size, your visibility timeout should again be the time for a single message times the batch size.

How does Dynamiq work?
=========

Internally, Dynamiq assigns an id to each message it receives. Using this id as a key, Dynamiq calculates the following:

Given my number of nodes N, the upperbound of my keyspace K (which is the maximum value of an int64, or roughly 9.223 X 10^18), each individual node is responsible for a theoretical maximum of K / N messages.

Let's say I'm Node N of 5. We're responsible for roughly 1.8 X 10^18 messages total (per queue), of which I am aware of the lower and upper bound of my range of the keyspace (LB and UB, respectively).

From here, I'm able to serve any of those messages. Each of the queues that I'm holding has a configured number of Partitions, globally.

Given the same number of nodes N, and the total partitions P, each node is responsible for N / P partitions

Let's say a given queue has 1000 partitions. As node N of 5, I'm responsible for 200 of those partitions. Each partition will be the size of K / P, or 9.233 X 10^15 messages. My first partition will start at LB, while my last will end at UB. Each partition in between will hold an even slice of my subset of the keyspace.

Partitions will be served to clients such that Partitions which have not recently been used have a direct priority over ones that have been used. In effect, each partition will be served exactly once before any will be served a second time.

Implementation Details
=========
This is a simple proof of concept demonstrating the effectiveness of implementing a
Dynamiq is a distributed data bag implementation on top of Riak, which anchors on the following properties:

1. Provides a distributed key-value store to support fast put/enqueue times ( put will be limited by the speed of the underlying storage ).
2. Provides a distributed method of range scanning the key-value store.
3. Each key-value pair supports a consistent SET/MAP object

In theory, any backing storage technology which adheres to this two properties is capable of being used with Dynamiq, but at this time we've chosen to focus on support for Riak.

Properties 1 and 2 do not necessarily need to be provided in the same store, as long as a mechanism is present to ensure that messages do not get written to only 1 store.

Given the first two properties, we can generally support the following complexity for operations:

Operation | Complexity
--- | ---
PUT | O(1)*
GET | O(log n)*
DELETE | O(1)

* This assumes append only insert into store 2, should the insert not be append only enque will become O(log n), where n is based on the total number of objects in the store

Batching
========
Given properties 1 and 2, it is possible to gather a range of objects from our datastore, and iterate across the resulting objects. If m is the number of messages in a requested batch, then the operations would be the following.

Operation | Complexity
--- | ---
PUT | O(m)
GET | O(m log n)
DELETE | O(m)

Alternatively if our data store supports property 3 from above, we can achieve the following operation complexities as long as the requested batch is < the batch size in a SET/MAP

Operation | Complexity
--- | ---
PUT | O(1)
GET | O( log n)
DELETE | O(1)

GET Times
=========
The assumption of a log n get is based on a standard index scan, as we only need to read the beginning or end of the index it is possible to achieve O(1) GET operations

Implementation
==========
Currently this implementation uses riak and properties 1 & 2 from above, with the following heuristic:

PUT)

  1. Generate a uuid for the message
  2. Put the object into riak with a 2i index on the uuid
  3. Serve the uuid of the object back to the client

GET)

  1. Return the least recently used, unlocked partition for the requested queue
  2. 2i query for X messages in the bucket, where X is your provided batch size, within the range of the retrieved partition
  3. Serve the messages

DELETE)

  1. Delete the message matching the inbound uuid
