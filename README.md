DynamiQ
=========

A simple implimentation of a queue on top of riak

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

Summary
=========
This is a simple proof of concept demonstrating the effectiveness of implementing a Distributed data bag implementation on top of Riak or an alternative store which has the following properties.

1) provides a distributed key-value store to support fast put/enqueue times ( put will be limited by the speed of the underlying storage ).
2) provides a distributed method of range scanning the key-value store.


item 1, and item 2 do not necessarily need to be provided in the same store, as long as a mechanism is present to ensure that messages do not get written to only 1 store.

Given these two properties we can generally support the following complexity for operations.

PUT) O(1)*

GET) O(log n )*

DELETE) O(1)

*  assumes append only insert into store 2, should the insert not be append only enque will become O(log n )

** where n is based on the total number of objects in the store

Batching
========
given properties 1 and 2 it is possible to gather a range of objects from our datastore, and iterate across the resulting objects.  if m is the number of messages in a requested batch, then the operations would be the following.

PUT)    O(m)

GET)    O(m log n )

DELETE) O(m)

Alternatively if our data store supports the following property:

3) each key-value pair supports a consistent SET/MAP object

We can achieve the following operation complexities as long as the requested batch is < the batch size in a SET/MAP

PUT)    O(1)

GET)    O( log n )

DELETE)  O(1)

GET Times
=========
The assumption of a log n get is based on a standard index scan, as we only need to read the beginning or end of the index it is possible to achieve O(1) GET operations

Implementation
==========
Currently this implementation uses riak and properties 1&2. with the following algorithm

PUT) 

  1) generate a uuid for the message

  2) put the object into riak with a 2i index on the uuid
  
  3) serve the uuid of the object back to the client

GET)
  
  1) 2i query for all inflight messages ( limit 10000 )
  
  2) 2i query for 10 messages in the bucket
  
  3) outer join the lists in 1&2 to find messages that are not inflight
  
  4) add a 2i index to the message with the present timestamp for "inflight"
  
  5) serve the message
  
DELETE)

  1) delete the message matching the inbound uuid

Client Libraries
================

* Ruby - https://github.com/Tapjoy/dynamiq-ruby-client
* Scala - Coming Soon

REST API
============

Dynamiq supports a REST API for communicating between clients and Dynamiq. The following is a list of routes, and the verbs they accept, for you to refer to in developing against Dynamiq. You should strive to use one of the official clients, in lieu of direct HTTP access in your applications.

* PUT /topics/{topic_name} : Creates a topic with the given name
* DELETE /topics/{topic_name} : Deletes a topic with the given name
* PUT /queues/{queue_name} : Creates a queue with the given name
* DELETE /queues/{queue_name} : Deletes a queue with the given name
* PUT /topics/{topic_name}/queues/{queue_name} : Subscribed the given queue to the given topic
* PUT /topics/{topic_name}/message : Publish the data in the request body as a message to all subscribed queues
* PUT /queues/{queue_name}/message : Enqueue the data in the request body as a message to the given queue
* GET /queues/{queue_name}/messages/{batch_size} : Retrieves up to the given batch_size in messages from the given queue
* DELETE /queues/{queue_name}/message/{message_id} : Deletes the message specified by the message_id from the given queue
* GET /queues/{queue_name} : Retrieves configuration and metadata about the given queue
* PATCH /queues/{queue_name} : Update the configuration of the given queue.

PATCH /queues/{queue_name} currently accepts the following fields

* visibility_timeout : The amount of time a partition should be considered "locked" to prevent it from serving duplicate messages too quickly
* min_partitions : The minimum number of slices of the keyspace this queue should have, spread across all nodes in the cluster. For high-throughput queues, you'll want to keep this number high, and always below the maximum
* max_partitions : The maximum number of slices of keyspace this queue should have, spread across all nodes in the cluster. For high-throughput queues, you'll want to keep this number high, and always above the minimum

Changing any of these values will result in an immediate write to Riak ensuring the data is persisted, however the individual Dynamiq nodes (including the node you issued the request to) will not have their in memory configuration updated until the next "Sync" with Riak.
