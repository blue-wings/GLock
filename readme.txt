BUG
1. can't interrupt when zookeeper is creating node , if do , zookeeper can't return node id, and lock is bad.   --------FIXED

STORY
1. this future may banding wasted node delete event (w w w w r r r W). optimal point.
2. LockSwitch, when it is off, client cat get lock always.
3. zookeeper connection closed disaster recovery
4. lock condition try time
5. lock condition unit test
6. lock Interruptibly
7. lock can't be interrupted
8. exception manual unlock

TASK
1. session time out, connection closed,  EPHEMERAL_SEQUENTIAL node delete, lock failed?
2. client closed and start watch

---------------------------------------------------------------------------------------------------------------------------------------

RELEASE
1. write read lock trylock
2. condition await signal signalAll

ONGOING
1. lock Fault tolerance, zookeeper error result to some error