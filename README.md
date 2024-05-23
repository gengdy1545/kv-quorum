# KV-Quorum

A distributed key-value store that implements quorum consensus replication.

## Overall design

This system comprises two main components: the coordinator and the store. Coordinators handle client requests and stores handle routed requests from coordinators. Each node is both a coordinator and a store.

## ToDo
Implement the logic to handle the success responses from the store node. This includes `put` and `get` requests. You can see the `ToDo` in the `Coordinator.java` file.

## Test
First, run `Runner.java`.
 
Second, run `CorrectnessTest.java` to test the correctness of the system.

There are currently no server down tests, you can test personally.

## Important Notice
Placing multiple replicas on a single node is obviously a stupid thing to do and would not be done in practice.
