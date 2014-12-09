dynamiq
=========

A simple implimentation of a queue on top of riak

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

the assumption of a log n get is based on a standard index scan, as we only need to read the beginning or end of the index it is possible to achieve O(1) GET operations


getting started
=========
```
export GOPATH=`pwd`
go get github.com/Tapjoy/dynamiq
mkdir lib
cp ./src/github.com/Tapjoy/dynamiq/lib/config.gcfg ./lib/config.gcfg
./bin/dynamiq
```

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



