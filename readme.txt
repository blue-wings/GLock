TRICK ERROR
1. can't interrupt when zookeeper is creating node , if do , zookeeper can't return node id, and lock is bad.

TODO
1. this future may banding wasted node delete event (w w w w r r r W). optimal point.
2. LockSwitch, when it is off, client can't get lock anymore.
3. zookeeper connection closed disaster recovery

QUESTION
1. session time out, connection closed,  EPHEMERAL_SEQUENTIAL node delete, lock failed?
2. client closed and start watch