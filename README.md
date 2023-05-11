# krake
krake is like kafka but easier to scale for smaller businesses.

the problem with Kafka is that it is difficult to use for smaller organisations. 
maintaining a set of kafka brokers is tricky especially when using something like kubernetes which is not designed for stateful applications. 
it is expensive to use existing solutions such as MSK or Confluents managed Kafka.
it is difficult to setup your own broker instances and manage them.

krake aims to provide _similar_ guarantees, semantics, and a familiar api with the ease of a modern & low-footprint design which can be hosted 
on a smaller kubernetes cluster. no zookeeper, no 2gb minimum per broker, easier to host on kubernetes or even spin up on small digitalocean droplets.

## goals

1. runs on kubernetes (custom operator?)
2. compatible with a subset of the Kafka apis - to be decided _how_ compatible (not everything will be supported)
3. written in Go = lower overheads than the jvm
4. tiered storage from the get-go

### on the kafka api
Krake does _not_ support the Kafka API on the wire, i.e. the exact protocol. Why?

- it complicates implementation details
- it adds a tight coupling to the kafka api itself

## high-level architecture

the high level architecture is broadly the same. topics, partitions, in-sync-replicas, message consuming and producing via offsets.

//TODO diagram

to achieve more flexibility in scaling krake uses a snapshotting mechanism* to make it easier to re-load brokers onto
new pods.
*snapshotting system to be designed.

### technology
1. 

## non-guarantees/goals
1. broker afinity - partitions will be expected to be on all hosts.

## license
apache?
