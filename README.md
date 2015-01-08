DynamiQ
=========

A simple implimentation of a queue on top of riak

What is Dynamiq?
==========

Dynamiq is a distributed databag, acting as a queue. It's written in golang for performance and easy of concurrency, and run in a cluster ontop of a Riak 2.0 cluster. It exposes a simple REST API for publishing to topics and directly enqueueing to queues, as well as receiving batches of messages at a time.

How does Dynamiq work?
=========

Internally, Dynamiq assigns an id to each message it receives. Using this id as a key, Dynamiq calculates the following:

Given my number of nodes N, the upperbound of my keyspace K (which is the maximum value of an int64, or roughly 9.223 X 10^18), each individual node is responsible for a theoretical maximum of K / N messages.

Lets say I'm Node N of 5. We're responsible for roughly 1.8 X 10^18 messages total (per queue), of which I am aware of the lower and upper bound of my range of the keyspace (LB and UB, respectively). 

From here, I'm able to serve any of those messages. Each of the queues that I'm holding has a configured number of Partitions, globally.

Given the same number of nodes N, and the total partitions P, each node is responsible for N / P partitions

Lets say a given queue has 1000 partitions. As node N of 5, I'm responsible for 200 of those partitions. Each partition will be the size of K / P, or 9.233 X 10^15 messages. My first partition will start at LB, while my last will end at UB. Each partition in between will hold an even slice of my subset of the keyspace.

Partitions will be served to clients such that Partitions which have not recently been used have a direct priority over ones that have been used. In effect, each partition will be served exactly once before any will be served a second time.

For more information on the importance of tuning partitions, see Tuning, below. 

Why Dynamiq?
==========

The main design ideal of Dynamiq is to be a drop-in replacement for Amazons SQS service, which can become expensive to run at a very large scale (150m+ per day).

De-Duplication
===========

Like SQS, Dynamiq has the possibility of sending duplicate messages. This possibility increases during certain significant events, such as making certain configuration changes to the running system.

You should account for this in your design by either managing your own de-dupe solution (such as using Memcache to hold the keys you've seen from a given queue, expiring with the visibility timeout on the queue) or design a system which self-defends against duplicate messages.

The former is likely a more realistic approach than the latter

Getting Started
=========

Prerequisites
-------------
To get Dynamiq up and running, you need to have a few pre-requisites installed. 

First, you need an installation of Riak 2.0 up and running somewhere (preferably local for testing / development). You can find guides on how to install Riak 2.0 for your particular operating system [here](http://docs.basho.com/riak/latest/quickstart/)

Second, you will need go installed. You can find the instructions [here](https://golang.org/doc/install). Go has incredibly opinionated ideas about where to place code so it can resolve dependencies and perform it's linking, so you'll need to do it the Go way or you're gonna have a bad time. Since you can't effectively develop and compile Go code outside of the primary go directory, this means the best place to install the go directory is within your existing directory for holding source code, in it's own directory "go". 

One thing to note is to make sure you have an environment variable GOPATH set to the location of this directory. For example, if your source code is normally in ~/src, and you installed go to ~/src/go, then GOPATH should be set to ~/src/go

Navigate into your go directory, and run the following

```
go get github.com/Tapjoy/dynamiq
mkdir lib
cp ./src/github.com/Tapjoy/dynamiq/lib/config.gcfg ./lib/config.gcfg
./bin/dynamiq
```

This will not only fetch Dynamiq, but also all of it's dependencies and their dependencies as well. Additionally this will create a config file where the compiled binary can locate it for local testing.

Next, you need to enable certain bucket types in Riak 2.0. This is taken care of for you in the setup.sh script provided by Dynamiq. Simply run that script, and Riak 2.0 should be ready to use.

Running Dynamiq
---------------

Now that you have the pre-reqs installed, it's time to run Dynamiq. You can compile and run Dynamiq easiest by invoking it from go directly each time. Navigate into the Dynamiq directory ($GOPATH/src/github.com/Tapjoy/dynamiq) and run the following:

```
go run dynamiq.go
```

If all is well, you should see log messages about booting up, and "syncing" with Riak. This means you're ready to begin using Dynamiq! Please continue on to the API section to learn how to create and utilize topics and queues in Dynamiq

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
  
  1. 2i query for all inflight messages ( limit 10000 )
  2. 2i query for 10 messages in the bucket
  3. Outer join the lists in 1 & 2 to find messages that are not inflight
  4. Add a 2i index to the message with the present timestamp for "inflight"
  5. Serve the message
  
DELETE)

  1. delete the message matching the inbound uuid

Client Libraries
================

* Ruby - https://github.com/Tapjoy/dynamiq-ruby-client
* Scala - Coming Soon

REST API
============

Dynamiq supports a REST API for communicating between clients and Dynamiq. The following is a list of routes, and the verbs they accept, for you to refer to in developing against Dynamiq. You should strive to use one of the official clients, in lieu of direct HTTP access in your applications.

Verb | Route | Description
--- | --- | ---
PUT | /topics/{topic_name} | Creates a topic with the given name
DELETE | /topics/{topic_name} | Deletes a topic with the given name
PUT | /queues/{queue_name} | Creates a queue with the given name
DELETE | /queues/{queue_name} | Deletes a queue with the given name
PUT | /topics/{topic_name}/queues/{queue_name} | Subscribed the given queue to the given topic
PUT | /topics/{topic_name}/message | Publish the data in the request body as a message to all subscribed queues
PUT | /queues/{queue_name}/message | Enqueue the data in the request body as a message to the given queue
GET | /queues/{queue_name}/messages/{batch_size} | Retrieves up to the given batch_size in messages from the given queue
DELETE | /queues/{queue_name}/message/{message_id} | Deletes the message specified by the message_id from the given queue
GET | /queues/{queue_name} | Retrieves configuration and metadata about the given queue
PATCH | /queues/{queue_name} | Update the configuration of the given queue (see below).

PATCH /queues/{queue_name} currently accepts the following fields

* visibility_timeout : The amount of time a partition should be considered "locked" to prevent it from serving duplicate messages too quickly
* min_partitions : The minimum number of slices of the keyspace this queue should have, spread across all nodes in the cluster. For high-throughput queues, you'll want to keep this number high, and always below the maximum
* max_partitions : The maximum number of slices of keyspace this queue should have, spread across all nodes in the cluster. For high-throughput queues, you'll want to keep this number high, and always above the minimum

Changing any of these values will result in an immediate write to Riak ensuring the data is persisted, however the individual Dynamiq nodes (including the node you issued the request to) will not have their in memory configuration updated until the next "Sync" with Riak.

Tuning
==========

When it comes to tuning Dynamiq, there are primarily 3 things to think about (which interplay with eachother tightly).

* Visibility Timeout
* Max Partitions
* Batch Size (for the clients request messages)

As mentioned above, Visibility Timeout is how long a given partiton is considered "locked" or "in-use" once it has served some or all of its messages. This prevents duplicates from that partition being served, *but also prevents non-duplicate messages potentially still within that partition from being served as well*.

This is a very important aspect of tuning Dynamiq to be aware of - You could inadvertently deny yourself the ability to receive messages for the period of your timeout if you aren't careful.

One line of thinking says you could make your partition size and your batch size to be 1:1. This means you'd need to tune your max partitions such that each partition could theoretically only hold the number of messages you wish to pull with a single request. Your visibility timeout in this case would be the time it takes to complete one message times the batch size.

The downside to this approach is that you'll have a very large number of partitions, which will take longer for you to cycle through all un-served partitions and leave more messages in-waiting for longer.

An alternative approach is to tune for the least amount of time making non-duplicate messages unavailable. This can be accomplished with fewer numbers of overall partitions. Considering the time it takes to complete a single message, and having determined the correct batch size, your visibility timeout should again be the time for a single message times the batch size.
