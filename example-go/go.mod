module example.com/qengine-router

go 1.21

require github.com/qengine/tsdb-go v0.0.0

// Local development: point at the in-tree SDK.  When you clone this
// project out, replace this with `go get github.com/qengine/tsdb-go@latest`
// once the SDK is published, or keep a local path.
replace github.com/qengine/tsdb-go => ../sdk/go
