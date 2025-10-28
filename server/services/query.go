package services

import (
	"github.com/auula/urnadb/vfs"
)

type QueryService interface {
	GetSegment(name string) (version uint64, seg *vfs.Segment, err error)
}

type QueryServiceImpl struct {
	storage *vfs.LogStructuredFS
}

func NewQueryServiceImpl(storage *vfs.LogStructuredFS) QueryService {
	return &QueryServiceImpl{
		storage: storage,
	}
}

func (q *QueryServiceImpl) GetSegment(name string) (version uint64, seg *vfs.Segment, err error) {
	return q.storage.FetchSegment(name)
}
