module example.com/qengine-cluster-demo

go 1.21

require github.com/qengine/tsdb-go v0.0.0

// Dev path — point at the in-tree SDK.  Replace once tsdb-go is published.
replace github.com/qengine/tsdb-go => ../../sdk/go
