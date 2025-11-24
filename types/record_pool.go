package types

import "sync"

var recordPool = sync.Pool{
	New: func() interface{} {
		return &Record{
			Data: make(map[string]*Value, 16),
		}
	},
}

func AcquireRecord() *Record {
	r := recordPool.Get().(*Record)
	r.ID = ""
	r.Table = ""
	return r
}

func ReleaseRecord(r *Record) {
	for k := range r.Data {
		delete(r.Data, k)
	}
	recordPool.Put(r)
}
