# Key-Value Store
This repository contains the implementation for a Distributed Key-Value Store that uses sharding to improve on fault tolerance, capacity, and throughput. Additionally, it attempts to maintain causal consistency and be fault tolerant. This project was the result of a cumulative course project that built on each assignment over the course of 10 weeks. This was made by the team consisting of myself alongside classmates [Dalton Nham](https://github.com/dalton-nham), and [Mohit Agrawal](https://github.com/MohitAgrawal404). 

# Directory Description
This directory contains the implementation of an HTTP Web Service for `GET`, `PUT`, and `DELETE` requests that utilizes a key-value store, with proxies, using the endpoint, `/kvs/<key>`. It allows for a multiple Docker containers, called "replicas", to communicate with each other in a network. The key-value store should enforce causal consistency and be fault tolerant, keeping the key-value store up-to-date and available even when one replica in the network goes down. This version of the key-value store has been extended using sharding to provide improved throughput, fault-tolerance, and latency. "Shards" are partitions in the network of replicas in which keys and nodes are separated into, albeit all nodes can still communicate. It is written in **Python** using the **Flask** framework and is designed to be ran within multiple Docker containers.

## Mechanism Descriptions
1. **Causal Consistency**: We used a Vector Clock tracking PUT/DELETE operations in order to enforce causal consistency. This came in the form of a Dict where the key is the replica address and the value is the current value of that replica's Vector Clock position. The VC is verified using a function and then incremented once verified. The VC is included in broadcasts to other replicas who also must verify the VC at their own replica. If the VC verification fails, then a `503` error is returned, indicating to the client that the causal dependency for their request is not satisfied, but they should try again later. We chose this mechanism as in class, we have the most experience working with Vector Clocks in different delivery protocols.
2. **Down Detection**: We originally wanted to use a heartbeat mechanism, but found that it was not necessary for this assignment as we noticed it only mattered during PUT/DELETE requests. Instead, when a replica PUTs/DELETEs a key-value pair, it broadcasts the message. If it attempts three retries with 0.5s timeouts before adding it to a list of dead replicas. It then removes that replica from its View and broadcasts that removal. The View is stored as a set in order to make sure that replica addresses are only added once. The timeouts are fairly short, so in the event that a replica is very, very slow, it may lead to a false-positive. On the other hand, we don't believe a replica will ever be a false-negative at the moment of detection as it must reply within short windows of time.
3. **Sharding Keys**: Keys are sharded across different nodes using the `ConsistentRing` class found in `consistent_hash.py`. The hashing algorithm used is sha256, and is also used to give shards a place on the ring. Once the ring has been built, when a key is provided for a `PUT` request, it is hashed and the value has a modulo operation applied in order to assign it to the correct node. This provides a consistent assignment as the same key will result in the same hash value, while also attempting to evenly distribute keys by using the design of consistent hashing.
4. **Reshard Mechanism**: During a reshard, there are two events that must take place: The new shards must be created and the existing key-value pairs must be remapped to give the new shard some of the load. To accomplish this goal the following steps are taken:
    * The original replica the client requests at builds the new shard assignments and broadcasts it.
    * Upon reception, all replicas then remap the keys in their Store, clear their local Store/shard-key maps, and broadcast the new mappings they have.
    * Finally, they then receive updates from the other replicas and rebuild their Store and shard-key mapping.

### Files Included
#### Documentation
* `README.md` - Markdown formatted, and is the file that you are currently reading. It contains a short description of this directory's files as well as other important information.
* `Dockerfile` - A simple Dockerfile used to build a Docker image to run the implemented HTTP Web Service in a container. 
* `requirements.txt` - Contains dependencies for the program implemented in `app.py` (only the Flask dependency for this program).
#### Program Files
* `app.py` - Contains the implementation of an HTTP Web Service that takes requests `GET`/`PUT`/`DELETE` for the endpoint `/kvs/<key>` that supports a collection of communicating instances. It now additionally has endpoints at `/shard` to represent functions related to sharding. This is implemented in **Python** using the **Flask** framework.
* `consistent_hash.py` - Contains the implementation of the ConsistentRing class that provides the implementation for consistent hashing of shards and keys. It also contains the hashing function `sha256_hasher`.
### Other
* `container_build.sh` - A bash script that executes the creation of a 6 replica version of the key-value store. It builds the image based off `app.py`, generates the subnet, and starts all the containers up, ranging from addresses 8082-8087. 
* `cleanup.sh` - A bash script that executes the destruction and removal of the image, subnet, and containers.
 
### Program Usage
#### Building the Program
This repository's `app.py` assumes that the user has **Docker** installed and running.
With Docker running, the Docker image can be built and then a multi-replica network can be made. In this repo, a script can be called as `./container_build.sh`, which is provided to start a network with 6 replicas. Calls to the APIs can then be made using the correct url for a curl command. The image, network, and containers can also be destroyed by calling the cleanup script, `./cleanup.sh`.

### Errors
There are no errors to report at this time.