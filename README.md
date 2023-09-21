# Introduction

This document describes the experiments to measure the speed of XDP-Rocks-to-XDP-Rocks replication over the network.

We benchmark the copy of a typical Kiwi shard (10GB to 100GB), populated with KV pairs of identical size. Namely, the key size is 16B, 
whereas the value size varies (128B and above). *The target copy speed is 100MB/s.*

The hardware platform is the target Kiwi hardware Xeon(R) Gold 6354 CPU (2 sockets, 36 cores, hyperthreading on), 128GB DRAM, 
3.84 TB SSD (Samsung MZQL23T8HCLS-00A07), 25 Gbps ETH network. 

**Executive summary**. The copy speed exceeds 110 MB/s for all value sizes except 128B (in the latter case, it is 60MB/s to 80MB/s).

# System Architecture

## Configuration
The KV separation threshold is 128B in both the source and the target XDP-Rocks databases. The data compression ratio is 1 (i.e., 
the data is uncompressible). 

## Setup 
On the source side, a db is pre-loaded with db_bench, then its read-only checkpoint (*data source*) is created.
On the target side, an empty db (referred to as *data target*) is created. 

## Data Copy
To maximize the end-to-end throughput, the network transfer is decoupled from the XDP-Rocks reads and writes. 
The diagram below depicts the system architecture. 

![High-level Replication Architecture.](https://github.com/michaelpanpliops/replicator/assets/143769632/6d2fa25a-9147-4cdc-aa8c-2b9e865b0d22)

The target node initiates the transfer. It connects to the source node thru a socket and pulls the data through it. 
The source node produces a stream of KV pairs by running in parallel *N* iterators through the data source. 
Each iterator traverses a key range of approximately the same size. All these iterators queue up their KV pairs 
to a concurrent queue that feeds the socket. 

The number of concurrent iterators *N* is selected by the application. We experimented with *N=4..12* (a bigger db enables a bigger *N*). 
Every iterator has an *internal* parallelism of 16 threads (set through *ReadOptions::iterator_internal_parallelism_enabled* and 
*ReadOptions::iterator_internal_parallelism_factor* - see the XDP-Rocks Programmer Manual). Altogether, the number of threads 
that fetch KV pairs in parallel is *16N* (e.g, 192 threads for *N=12*). 

At the target node, all the incoming KV pairs are retrieved from the socket and streamed to a concurrent queue. 
A single writer thread retrieves the data from there and writes to the data target. To speed up the import, the 
target db WAL is switched off during the copy. (In case of crash, the process restarts from scratch). 

# Numerical results

![Value size = 256B.](https://github.com/michaelpanpliops/replicator/assets/143769632/5e793e97-ef6c-4b96-ac9f-fd47458ba4f8)

![Value size = 128B.](https://github.com/michaelpanpliops/replicator/assets/143769632/11978c17-fba0-4c89-bec6-56e42586a716)


The following plots demonstrate the copy rate (over time) for two value sizes - 128B and 256B. The rate is very stable throughout the copy process (at the end, it decelerates because some threads finish earlier). The 100GB source db size is the most typical. We see that the rate is 110 MB/s for value size of 256B, and 80 MB/s for value size of 128B. For value sizes above 256B (the common case), the rate is bigger than 110 MB/s (we omitted those results since the system already surpasses the performance goal). 

The overall speed depends on the value size. The smaller the value, the bigger the read amplification (more values to be retrieved for the same source db size), 
and the slower the overall read. For example, if the value size is 128B, then the read amplification is approximately *4KB/128B = 32*. In other words, 
reading at the rate of 80MB/s at the application level requires at least 2.56 GB/s at the SSD level. If with the 256B value, the read amplification is 16, 
and therefore, the application level throughput of 110MB/s translates to 1.76 GB/s at the SSD level. 

The CPU overhead is negligible (less than 10% in all the experiments). 
