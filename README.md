# Introduction

This document describes the experiments to measure the speed of XDP-Rocks-to-XDP-Rocks replication over the network, with multiple shard and value sizes. 

# Data

The source is an read-only checkpoint of the kiwi shard (= XDP-Rocks db). The target is an XDP-Rocks db that is created prior to the copy. 
The source db size varies from 10GB to 100GB. The key size is fixed (16B). The value size varies (128B and up).
The target copy speed is 100MB/s. 

# Hardware

1. CPU Intel(R) Xeon(R) Gold 6354 CPU @ 3.00GHz, 2 sockets, 18 cores/socket, hyperthreading=on.
1. DRAM 128GB.
1. SSD SAMSUNG MZQL23T8HCLS-00A07, 3.84TB.
1. NETWORK 25Gbps ETH.

# Software

The code has two parts:

1. *Producer* (client). Iterate thru the source db and stream the rows (KV pairs) thru the socket, in the key order. 
1. *Consumer* (server). Read the stream of KV pairs from the socket, and insert them into the target db. 

## Details

1. *Producer side*. To speed up the source db iteration, the code employs the *parallel iterator* optimization. The iterator fetches multiple KV pairs in parallel, with no API change. The KV pairs are placed by the iterator in a non-blocking queue which feeds the socket-streamer thread. To speed up the iteration even further, the producer splits the key range into a small number of sub-ranges (1-5, depending on the source db size), and iterates them in parallel. Overall, the iteration is performed by 8 to 40 threads in parallel. 

1. *Consumer side*. To speed up the insertion into the target db, its WAL is switched off during the copy. (In case of crash, the copying restarts from scratch). 

# Results

The following plots demonstrate the copy rate (over time) for two value sizes - 128B and 256B. The rate is very stable throughout the copy process (at the end, it decelerates because some threads finish earlier). The 100GB source db size is the most typical. We see that the rate is 110 MB/s for value size of 256B, and 80 MB/s for value size of 128B. For value  sizes above 256B (the common case), the rate is bigger than 110 MB/s (we omitted those results since the system already surpasses the performance goal). 

![Value size = 256B](https://github.com/michaelpanpliops/replicator/assets/143769632/56dd2b6c-1576-410e-a2b8-ab33a15f6a18)

![Value size = 128B](https://github.com/michaelpanpliops/replicator/assets/143769632/087267ce-f942-4b8d-8406-495ee4605c3d)
