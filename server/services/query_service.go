package services

import "github.com/auula/urnadb/vfs"

type QueryService interface {
	GetSegment(name string) (version uint64, seg *vfs.Segment, err error)
}
