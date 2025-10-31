// Copyright 2022 Leon Ding <ding_ms@outlook.com> https://urnadb.github.io

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vfs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/auula/urnadb/clog"
	"github.com/auula/urnadb/utils"
	"github.com/robfig/cron/v3"
	"github.com/spaolacci/murmur3"
)

const (
	_             = 1 << (10 * iota) // skip iota = 0
	kb                               // 2^10 = 1024
	mb                               // 2^20 = 1048576
	gb                               // 2^30 = 1073741824
	appendOnlyLog = os.O_RDWR | os.O_CREATE | os.O_APPEND
)

type _GC_STATE = uint8 // Region garbage collection state

const (
	_GC_INIT _GC_STATE = iota // gc 第一次执行就是这个状态
	_GC_ACTIVE
	_GC_INACTIVE
	_SEGMENT_PADDING    = 26
	_INDEX_SEGMENT_SIZE = 49
)

var (
	shard            = 10
	transformer      = NewTransformer()
	fileExtension    = ".db"
	indexFileName    = "index.db"
	dataFileMetadata = []byte{0xDB, 0x00, 0x01, 0x01}
)

type Options struct {
	Path      string
	FSPerm    os.FileMode
	Threshold uint8
}

// inode represents a file system node with metadata.
type inode struct {
	RegionID  int64  // Unique identifier for the region
	Position  int64  // Position within the file
	ExpiredAt int64  // Expiration time of the inode (UNIX timestamp in nano seconds)
	CreatedAt int64  // Creation time of the inode (UNIX timestamp in nano seconds)
	mvcc      uint64 // Multi-version concurrency ID
	Length    int32  // Data record length
	Type      kind   // Data record type
}

type indexMap struct {
	mu    sync.RWMutex
	index map[uint64]*inode
}

// LogStructuredFS represents the virtual file storage system.
type LogStructuredFS struct {
	mu               sync.RWMutex
	offset           int64
	regionID         int64
	directory        string
	fsPerm           os.FileMode
	indexs           []*indexMap
	active           *os.File
	regions          map[int64]*os.File
	gcstate          _GC_STATE
	compactTask      *cron.Cron
	dirtyRegions     []*os.File
	regionThreshold  int64
	checkpointWorker *time.Ticker
	expireLoopWorker *time.Ticker
}

// PutSegment inserts a Segment record into the LogStructuredFS virtual file system.
func (lfs *LogStructuredFS) PutSegment(key string, seg *Segment) error {
	inum := inodeNum(key)

	bytes, err := serializedSegment(seg)
	if err != nil {
		return err
	}

	lfs.mu.Lock()
	defer lfs.mu.Unlock()

	// Append data to the active region with a lock.
	err = appendToActiveRegion(lfs.active, bytes)
	if err != nil {
		return err
	}

	// Select an index shard based on the hash function and update it.
	// To avoid locking the entire index, only the relevant shard is locked.
	imap := lfs.indexs[inum%uint64(shard)]
	imap.mu.Lock()
	// Update the inode metadata within a critical section.
	imap.index[inum] = &inode{
		RegionID:  lfs.regionID,
		Position:  lfs.offset,
		Length:    seg.Size(),
		CreatedAt: seg.CreatedAt,
		ExpiredAt: seg.ExpiredAt,
		mvcc:      0,
		Type:      seg.Type,
	}
	imap.mu.Unlock()

	lfs.offset += int64(seg.Size()) // uint32 to uint64 is always safe

	if lfs.offset >= lfs.regionThreshold {
		return lfs.createActiveRegion()
	}

	return nil
}

func (lfs *LogStructuredFS) BatchFetchSegments(keys ...string) ([]*Segment, error) {
	var segs []*Segment
	for _, key := range keys {
		_, seg, err := lfs.FetchSegment(key)
		if err != nil {
			return nil, err
		}
		segs = append(segs, seg)
	}
	return segs, nil
}

func (lfs *LogStructuredFS) DeleteSegment(key string) error {
	seg := NewTombstoneSegment(key)

	bytes, err := serializedSegment(seg)
	if err != nil {
		return err
	}

	// 写入和更新 offset 应该是一个整体操作
	lfs.mu.Lock()
	err = appendToActiveRegion(lfs.active, bytes)
	if err != nil {
		lfs.mu.Unlock()
		return err
	}

	lfs.offset += int64(seg.Size())
	lfs.mu.Unlock()

	inum := inodeNum(key)
	imap := lfs.indexs[inum%uint64(shard)]
	if imap == nil {
		return fmt.Errorf("inode index shard for %d not found", inum)
	}

	imap.mu.Lock()
	delete(imap.index, inum)
	imap.mu.Unlock()

	return nil
}

func (lfs *LogStructuredFS) HasSegment(key string) bool {
	inum := inodeNum(key)
	imap := lfs.indexs[inum%uint64(shard)]
	if imap == nil {
		return false
	}

	imap.mu.RLock()
	defer imap.mu.RUnlock()
	inode, ok := imap.index[inum]
	if !ok {
		return false
	}

	return inode != nil && time.Now().UnixMicro() < inode.ExpiredAt
}

func (lfs *LogStructuredFS) FetchSegment(key string) (uint64, *Segment, error) {
	inum := inodeNum(key)
	imap := lfs.indexs[inum%uint64(shard)]
	if imap == nil {
		return 0, nil, fmt.Errorf("inode index shard for %d not found", inum)
	}

	imap.mu.RLock()
	inode, ok := imap.index[inum]
	imap.mu.RUnlock()
	if !ok {
		return 0, nil, fmt.Errorf("inode index for %d not found", inum)
	}

	if atomic.LoadInt64(&inode.ExpiredAt) <= time.Now().UnixMicro() &&
		atomic.LoadInt64(&inode.ExpiredAt) > 0 {
		imap.mu.Lock()
		delete(imap.index, inum)
		imap.mu.Unlock()
		return 0, nil, fmt.Errorf("inode index for %d has expired", inum)
	}

	fd, ok := lfs.regions[atomic.LoadInt64(&inode.RegionID)]
	if !ok {
		return 0, nil, fmt.Errorf("data region with ID %d not found", inode.RegionID)
	}

	_, segment, err := readSegment(fd, atomic.LoadInt64(&inode.Position), _SEGMENT_PADDING)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read segment: %w", err)
	}

	// Return the fetched segment and multi-version concurrency ID
	return atomic.LoadUint64(&inode.mvcc), segment, nil
}

// GetTotalSpaceUsed 获取当前 NoSQL 文件存储系统使用的总空间
func (lfs *LogStructuredFS) GetTotalSpaceUsed() uint64 {
	var total uint64
	for _, imap := range lfs.indexs {
		imap.mu.RLock()
		for _, inode := range imap.index {
			total += uint64(inode.Length)
		}
		imap.mu.RUnlock()
	}
	return total
}

// RefreshInodeCount iterate over each index in lfs.indexs.
func (lfs *LogStructuredFS) RefreshInodeCount() uint64 {
	inodes := uint64(0)
	for _, imap := range lfs.indexs {
		for key, inode := range imap.index {
			// Clean expired inode
			imap.mu.Lock()
			if inode.ExpiredAt <= time.Now().UnixMicro() && inode.ExpiredAt > 0 {
				delete(imap.index, key)
			} else {
				inodes += 1
			}
			imap.mu.Unlock()
		}
	}
	return inodes
}

func (lfs *LogStructuredFS) StopExpireLoop() {
	lfs.mu.Lock()
	defer lfs.mu.Unlock()

	if lfs.expireLoopWorker != nil {
		lfs.expireLoopWorker.Stop()
	}
}

func expireLoop(indexs []*indexMap, ticker *time.Ticker) {
	for range ticker.C {
		for _, imap := range indexs {
			imap.mu.Lock()
			for key, inode := range imap.index {
				if inode.ExpiredAt > 0 && inode.ExpiredAt <= time.Now().UnixMicro() {
					delete(imap.index, key)
				}
			}
			imap.mu.Unlock()
		}
	}
}

func inodeNum(key string) uint64 {
	return murmur3.Sum64([]byte(key))
}

// UpdateSegmentWithCAS 通过类似于 MVCC 来实现更新操作数据一致性
func (lfs *LogStructuredFS) UpdateSegmentWithCAS(key string, expected uint64, newseg *Segment) error {

	// 在基于已有的 segment 更新时，检查是否过期。
	// 如果在更新过程中过期就直接拒绝基于原有的更新请求。
	if _, ok := newseg.ExpiresIn(); !ok {
		return errors.New("cannot insert expired segment")
	}

	inum := inodeNum(key)
	imap := lfs.indexs[inum%uint64(shard)]
	if imap == nil {
		return fmt.Errorf("inode index shard for %d not found", inum)
	}

	// 加 inode 写锁，保护 MVCC 检查 + inode 更新的一致性
	imap.mu.Lock()
	defer imap.mu.Unlock()

	inode, ok := imap.index[inum]
	if !ok {
		return fmt.Errorf("inode index for %d not found", inum)
	}

	// 快速检测 MVCC 版本号，被修改则快速失败
	if atomic.LoadUint64(&inode.mvcc) != expected {
		return errors.New("failed to update data due to version conflict")
	}

	// 序列化新数据
	bytes, err := serializedSegment(newseg)
	if err != nil {
		return err
	}

	// 写 active region 时用全局锁，写前就锁防止 offset 不一致
	lfs.mu.Lock()
	err = appendToActiveRegion(lfs.active, bytes)
	if err != nil {
		lfs.mu.Unlock()
		return fmt.Errorf("failed to update CAS region data: %w", err)
	}
	lfs.mu.Unlock()

	// 更新 inode 字段在 imap.mu 锁 和 atomic 保护下进行原子操作，
	// 不使用 &inode{...} 来替代是因为降低垃圾回收器负载。
	// imap.index[inum] = &inde{...}
	// 新 inode 的 CreatedAt 这个时间应该是使用原始的 inode 的 CreatedAt，
	// 理论上应该添加一个 UpdatedAt 字段来适用于 CAS 操作。
	atomic.StoreInt64(&inode.CreatedAt, newseg.CreatedAt)
	atomic.StoreInt64(&inode.ExpiredAt, newseg.ExpiredAt)
	atomic.StoreInt64(&inode.RegionID, lfs.regionID)
	atomic.StoreInt32(&inode.Length, newseg.Size())
	atomic.StoreInt64(&inode.Position, lfs.offset)

	// 我的设计是没有问题的，问题是很多客户端不支持 long 或者 uint64 类型的版本号。
	// 长时间运行可能会出现 MVCC 版本号溢出的问题，对溢出进行检查。
	if atomic.LoadUint64(&inode.mvcc) == math.MaxUint64 {
		return errors.New("failed to CAS number version overflow")
	}

	// 更新 MVCC 版本号，如果使用的 atomic.StoreUint64 只能保证原子地写入内存，不能保证算数运算过程也是原子。
	_ = atomic.AddUint64(&inode.mvcc, 1)

	// 更新全局 offset 原子操作保证并发安全
	_ = atomic.AddInt64(&lfs.offset, int64(newseg.Size()))

	return nil
}

func (lfs *LogStructuredFS) changeRegions() error {
	lfs.mu.Lock()
	defer lfs.mu.Unlock()

	err := lfs.active.Sync()
	if err != nil {
		return fmt.Errorf("failed to change active regions: %w", err)
	}

	lfs.regions[lfs.regionID] = lfs.active

	err = lfs.createActiveRegion()
	if err != nil {
		return fmt.Errorf("failed to chanage active regions: %w", err)
	}

	return nil
}

func (lfs *LogStructuredFS) createActiveRegion() error {
	lfs.regionID += 1
	fileName, err := generateFileName(lfs.regionID)
	if err != nil {
		return fmt.Errorf("failed to new active region name: %w", err)
	}

	active, err := os.OpenFile(filepath.Join(lfs.directory, fileName), appendOnlyLog, lfs.fsPerm)
	if err != nil {
		return fmt.Errorf("failed to create active region: %w", err)
	}

	n, err := active.Write(dataFileMetadata)
	if err != nil {
		return fmt.Errorf("failed to write active region metadata: %w", err)
	}

	if n != len(dataFileMetadata) {
		return errors.New("failed to active region metadata write")
	}

	lfs.active = active
	lfs.offset = int64(len(dataFileMetadata))
	lfs.regions[lfs.regionID] = lfs.active

	return nil
}

func (lfs *LogStructuredFS) scanAndRecoverRegions() error {
	// Single-thread recovery does not require locking
	files, err := os.ReadDir(lfs.directory)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), fileExtension) {
			if strings.HasPrefix(file.Name(), "0") {
				fd, err := os.OpenFile(filepath.Join(lfs.directory, file.Name()), os.O_RDWR, lfs.fsPerm)
				if err != nil {
					return fmt.Errorf("failed to open data file: %w", err)
				}

				regionID, err := parseDataFileName(file.Name())
				if err != nil {
					return fmt.Errorf("failed to get region id: %w", err)
				}
				lfs.regions[regionID] = fd
			}
		}
	}

	// Only find the largest file if there are more than one data files
	if len(lfs.regions) > 0 {
		var regionIds []int64
		for v := range lfs.regions {
			regionIds = append(regionIds, v)
		}
		// Sort the regionIds slice in ascending order
		sort.Slice(regionIds, func(i, j int) bool {
			return regionIds[i] < regionIds[j]
		})

		// Find the latest version of the data file
		lfs.regionID = regionIds[len(regionIds)-1]

		// Create a new file if the largest region file exceeds the threshold, otherwise, no need to create a new file
		active, ok := lfs.regions[lfs.regionID]
		if !ok {
			return fmt.Errorf("region file not found for region id: %d", lfs.regionID)
		}
		stat, err := active.Stat()
		if err != nil {
			return fmt.Errorf("failed to get region file info: %w", err)
		}

		if stat.Size() >= lfs.regionThreshold {
			return lfs.createActiveRegion()
		} else {
			offset, err := active.Seek(0, io.SeekEnd)
			if err != nil {
				return fmt.Errorf("failed to get region file offset: %w", err)
			}
			lfs.active = active
			lfs.offset = offset
		}
	} else {
		// If it is an empty directory, create a writable data file
		return lfs.createActiveRegion()
	}

	return nil
}

// recoveryIndex performs index recovery operations on data files stored on disk.
// Steps:
//  1. Read the index snapshot file to restore the index.
//  2. Unlike bitcask, where hint files are generated during the compressor process,
//     in bitcask, hint files are created during compression but do not represent
//     the full state of the in-memory index.
//  3. UrnaDB adopts a completely different design. If the system was closed normally,
//     an index file is generated upon closure.
//  4. If the data file has an associated index file, the index is restored directly
//     from the index file.
//  5. If no index file exists, a global scan of the data files is performed at startup
//     to reconstruct the index file.
func (lfs *LogStructuredFS) scanAndRecoverIndexs() error {
	// Construct the full file path
	filePath := filepath.Join(lfs.directory, indexFileName)
	if utils.IsExist(filePath) {
		// If the index file exists, restore it
		file, err := os.Open(filePath)
		if err != nil {
			return fmt.Errorf("failed to open index file: %w", err)
		}
		defer file.Close()

		err = recoveryIndex(file, lfs.indexs)
		if err != nil {
			return fmt.Errorf("failed to recover index mapping: %w", err)
		}

		return nil
	}

	// 只有数据文件大于 2 并且有检查点文件才加快启动恢复
	ckpts, _ := filepath.Glob(filepath.Join(lfs.directory, "*.ids"))
	if len(lfs.regions) >= 2 && len(ckpts) > 0 {
		return scanAndRecoverCheckpoint(ckpts, lfs.regions, lfs.indexs)
	}

	// If the index file does not exist, recover by globally scanning the regions files
	// If the data files are very large and numerous, recovery time increases significantly.
	// Frequent garbage collection reduces the size of data files and speeds up startup time.
	// However, frequent garbage collection may negatively impact overall read/write performance.
	return crashRecoveryAllIndex(lfs.regions, lfs.indexs)
}

func (*LogStructuredFS) SetCompressor(compressor Compressor) {
	transformer.SetCompressor(compressor)
}

func (*LogStructuredFS) SetEncryptor(encryptor Encryptor, secret []byte) error {
	return transformer.SetEncryptor(encryptor, secret)
}

func (lfs *LogStructuredFS) RunCheckpoint(second uint32) {
	lfs.mu.Lock()
	if lfs.checkpointWorker != nil {
		lfs.mu.Unlock()
		return
	}

	// 设置 checkpoint 异步生成周期
	lfs.checkpointWorker = time.NewTicker(time.Duration(second) * time.Second)
	lfs.mu.Unlock()

	var chkptState bool = false

	go func() {
		for range lfs.checkpointWorker.C {
			// 上一个检查点还在生成就跳过本次的
			if chkptState {
				continue
			}

			// Toggle checkpoint state
			chkptState = !chkptState

			// 只有数据文件大于 2 个，才生成快速恢复的检查点
			if len(lfs.regions) >= 2 {
				ckpt := checkpointFileName(lfs.regionID)
				path := filepath.Join(lfs.directory, ckpt)

				fd, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, lfs.fsPerm)
				if err != nil {
					clog.Errorf("failed to generate index checkpoint file: %v", err)
					chkptState = !chkptState
					continue
				}

				// 先写入 metadata
				n, err := fd.Write(dataFileMetadata)
				if err != nil {
					clog.Errorf("failed to write checkpoint file metadata: %v", err)
					chkptState = !chkptState
					_ = utils.FlushToDisk(fd)
					continue
				}
				if n != len(dataFileMetadata) {
					clog.Warnf("checkpoint file metadata write incomplete")
					chkptState = !chkptState
					_ = utils.FlushToDisk(fd)
					continue
				}

				// 创建一个 buf 缓冲区方便服用内存
				buf := new(bytes.Buffer)

				// 遍历 indexs 确保锁的粒度更小
				for _, imap := range lfs.indexs {
					imap.mu.RLock()
					// 遍历复制的数据，进行序列化写入
					for inum, inode := range imap.index {
						bytes, err := serializedIndex(buf, inum, inode)
						if err != nil {
							clog.Warnf("failed to serialize index (inum: %d): %v", inum, err)
							continue
						}

						_, err = fd.Write(bytes)
						if err != nil {
							clog.Errorf("failed to write serialized index (inum: %d): %v", inum, err)
							continue
						}
					}
					imap.mu.RUnlock()
				}

				// 确保文件在当前循环结束时正确刷盘关闭
				err = utils.FlushToDisk(fd)
				if err != nil {
					clog.Errorf("failed to generated checkpoint file: %v", err)
					chkptState = !chkptState
					continue
				}

				// 使用 strings.TrimSuffix 去掉 .tmp 后缀，然后加上 .ids 后缀
				newckpt := strings.TrimSuffix(ckpt, ".tmp") + ".ids"
				err = os.Rename(filepath.Join(lfs.directory, ckpt), filepath.Join(lfs.directory, newckpt))
				if err != nil {
					clog.Errorf("failed to rename checkpoint temp file: %v", err)
					chkptState = !chkptState
					_ = utils.FlushToDisk(fd)
					continue
				}

				clog.Infof("generated checkpoint file (%s) successfully", newckpt)

				// 滚动 checkpoint 文件确保只保留 1 份快照
				err = cleanupDirtyCheckpoint(lfs.directory, newckpt)
				if err != nil {
					clog.Warnf("failed to cleanup old checkpoint file: %v", err)
				}

				// Toggle checkpoint state
				chkptState = !chkptState

			} else {
				clog.Warnf("regions (%d%%) does not meet generated checkpoint status", len(lfs.regions)/10)
			}
		}
	}()
}

func (lfs *LogStructuredFS) StopCheckpoint() {
	lfs.mu.Lock()
	defer lfs.mu.Unlock()

	if lfs.checkpointWorker != nil {
		lfs.checkpointWorker.Stop()
		lfs.checkpointWorker = nil
	}
}

// RunCompactRegion 使用 robfig/cron 调度垃圾回收
func (lfs *LogStructuredFS) RunCompactRegion(schedule string) error {
	lfs.mu.Lock()
	if lfs.compactTask != nil {
		lfs.mu.Unlock()
		return fmt.Errorf("region compact is already running: %v", lfs.gcstate)
	}

	// 初始化 cron 任务
	lfs.compactTask = cron.New(cron.WithSeconds())
	lfs.mu.Unlock()

	// 添加定时任务
	_, err := lfs.compactTask.AddFunc(schedule, func() {
		lfs.mu.Lock()
		lfs.gcstate = _GC_ACTIVE
		lfs.mu.Unlock()

		err := lfs.cleanupDirtyRegions()
		if err != nil {
			clog.Warnf("failed to compact dirty region: %v", err)
		}

		lfs.mu.Lock()
		lfs.gcstate = _GC_INACTIVE
		lfs.mu.Unlock()
	})

	if err != nil {
		return err
	}

	// 启动定时清理 Region 区域的任务
	lfs.compactTask.Start()
	return nil
}

// StopCompactRegion 关闭垃圾回收
func (lfs *LogStructuredFS) StopCompactRegion() {
	lfs.mu.Lock()
	defer lfs.mu.Unlock()

	if lfs.compactTask != nil {
		lfs.compactTask.Stop()
		lfs.compactTask = nil
		lfs.gcstate = _GC_INIT
	}
}

// GCState returns the current garbage collection (GC) state
// of the LogStructuredFS regions compressor worker.
func (lfs *LogStructuredFS) GCState() uint8 {
	return uint8(lfs.gcstate)
}

func OpenFS(opt *Options) (*LogStructuredFS, error) {
	if opt.Threshold <= 0 {
		return nil, fmt.Errorf("single region threshold size limit is too small")
	}

	err := checkFileSystem(opt.Path, opt.FSPerm)
	if err != nil {
		return nil, err
	}

	instance := &LogStructuredFS{
		indexs:    make([]*indexMap, shard),
		regions:   make(map[int64]*os.File, 10),
		offset:    int64(len(dataFileMetadata)),
		regionID:  0,
		directory: opt.Path,
		gcstate:   _GC_INIT,
		fsPerm:    opt.FSPerm,
		// Single region max size = 255GB
		regionThreshold:  int64(opt.Threshold) * gb,
		compactTask:      nil,
		checkpointWorker: nil,
		expireLoopWorker: time.NewTicker(time.Duration(120) * time.Second),
	}

	for i := 0; i < shard; i++ {
		instance.indexs[i] = &indexMap{
			index: make(map[uint64]*inode, 1e6),
		}
	}

	// First, perform recovery operations on existing data files and initialize the in-memory data version number
	err = instance.scanAndRecoverRegions()
	if err != nil {
		return nil, fmt.Errorf("failed to recover data regions: %w", err)
	}

	err = instance.scanAndRecoverIndexs()
	if err != nil {
		return nil, fmt.Errorf("failed to recover regions index: %w", err)
	}

	go expireLoop(instance.indexs, instance.expireLoopWorker)

	// Singleton pattern, but other packages can still create an instance with new(LogStructuredFS), which makes this ineffective
	return instance, nil
}

// Before closing, always check if GC (garbage collection) is executing.
// If GC is executing, do not close blindly.
func (lfs *LogStructuredFS) CloseFS() error {
	lfs.mu.Lock()
	defer lfs.mu.Unlock()
	for _, file := range lfs.regions {
		err := utils.FlushToDisk(file)
		if err != nil {
			// In-memory indexes must be persisted
			inner := lfs.ExportSnapshotIndex()
			if inner != nil {
				return fmt.Errorf("failed to export shapshot index: %w", errors.Join(err, inner))
			}
			return fmt.Errorf("failed to close storage: %w", err)
		}
	}

	// If there is a snapshot of the index file, recover from the snapshot.
	// otherwise, perform a global scan.
	return lfs.ExportSnapshotIndex()
}

func (lfs *LogStructuredFS) GetDirectory() string {
	return lfs.directory
}

// ExportSnapshotIndex is the operation performed during a normal program exit.
// exporting the in-memory index snapshot to a file on disk.
// The current design has limitations for systems with low memory resources,
// such as those with RAM of 512 MB < 1 GB.
// If a 1 GB snapshot cannot be fully serialized to disk,
// mapping large files into memory may not be a good choice,
// as it consumes a significant amount of virtual memory space and may lead to
// swapping memory pages to disk.
func (lfs *LogStructuredFS) ExportSnapshotIndex() error {
	filePath := filepath.Join(lfs.directory, indexFileName)
	fd, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, lfs.fsPerm)
	if err != nil {
		return fmt.Errorf("failed to generate index snapshot file: %w", err)
	}
	defer utils.FlushToDisk(fd)

	n, err := fd.Write(dataFileMetadata)
	if err != nil {
		return fmt.Errorf("failed to write index file metadata: %w", err)
	}

	if n != len(dataFileMetadata) {
		return errors.New("index file metadata write incomplete")
	}

	// 创建一个 buf 缓冲区方便服用内存
	buf := new(bytes.Buffer)

	// 这里后面的版本可以优化为并行任务导出
	// 索引序列化不需要考虑有序的
	// 但是存在并发写一个文件的竞争的问题，最后还是放弃并发方案
	// 可以考虑多开几个文件并行导出，解决了单一文件写入的问题
	for _, imap := range lfs.indexs {
		if err := func() error {
			imap.mu.RLock()
			defer imap.mu.RUnlock()
			for inum, inode := range imap.index {
				bytes, err := serializedIndex(buf, inum, inode)
				if err != nil {
					return fmt.Errorf("failed to serialized index (inum: %d): %w", inum, err)
				}
				_, err = fd.Write(bytes)
				if err != nil {
					return fmt.Errorf("failed to write serialized index (inum: %d): %w", inum, err)
				}
			}
			return nil
		}(); err != nil {
			return fmt.Errorf("failed to export snapshot index file: %w", err)
		}
	}

	return nil
}

func recoveryIndex(fd *os.File, indexs []*indexMap) error {
	offset := int64(len(dataFileMetadata))

	finfo, err := fd.Stat()
	if err != nil {
		return err
	}

	type index struct {
		inum  uint64
		inode *inode
	}

	nqueue := make(chan index, (finfo.Size()-offset)/_INDEX_SEGMENT_SIZE)
	equeue := make(chan error, 1)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(nqueue)

		buf := make([]byte, _INDEX_SEGMENT_SIZE)
		for offset < finfo.Size() && len(equeue) == 0 {
			_, err := fd.ReadAt(buf, offset)
			if err != nil {
				equeue <- fmt.Errorf("failed to read index node: %w", err)
				return
			}

			offset += _INDEX_SEGMENT_SIZE

			inum, inode, err := deserializedIndex(buf)
			if err != nil {
				equeue <- fmt.Errorf("failed to deserialize index (inum: %d): %w", inum, err)
				return
			}

			if inode.ExpiredAt > 0 && inode.ExpiredAt <= time.Now().UnixMicro() {
				continue
			}

			nqueue <- index{inum: inum, inode: inode}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for node := range nqueue {
			imap := indexs[node.inum%uint64(shard)]
			if imap != nil {
				imap.index[node.inum] = node.inode
			} else {
				// This corresponds to the condition len(queue) == 0 in the for loop.
				// It prevents a situation where the consumer goroutine has encountered an error and stopped,
				// But the producer goroutine is still reading and deserializing the index.
				// As a result, it avoids delaying the execution of defer wg.Done(), which would perform meaningless work.
				// The goal is to resume the blocked wg.Wait() as quickly as possible,
				// Allowing the main goroutine to return promptly.
				equeue <- errors.New("no corresponding index shard")
				return
			}
		}
	}()

	wg.Wait()

	select {
	case err := <-equeue:
		close(equeue)
		return err
	default:
		close(equeue)
		return nil
	}
}

// crashRecoveryAllIndex parses the regions file collection and restores the in-memory index with the following.
// Steps:
// 1. Crash recovery logic scans all data files.
// 2. Reads the first 26 bytes of MetaInfo from each data record.
// 3. Replays these records and checks whether the DEL value is 1.
// 4. If DEL is 1, the corresponding entry is deleted from the in-memory index.
// 5. Otherwise, the disk metadata is reconstructed into the index.
// | DEL 1 | KIND 1 | EAT 8 | CAT 8 | KLEN 4 | VLEN 4 | KEY ? | VALUE ? | CRC32 4 |
func crashRecoveryAllIndex(regions map[int64]*os.File, indexs []*indexMap) error {
	var regionIds []int64
	for v := range regions {
		regionIds = append(regionIds, v)
	}

	sort.Slice(regionIds, func(i, j int) bool {
		return regionIds[i] < regionIds[j]
	})

	for _, regionId := range regionIds {
		fd, ok := regions[regionId]
		if !ok {
			return fmt.Errorf("data file does not exist regions id: %d", regionId)
		}

		finfo, err := fd.Stat()
		if err != nil {
			return err
		}

		offset := int64(len(dataFileMetadata))

		for offset < finfo.Size() {
			inum, segment, err := readSegment(fd, offset, _SEGMENT_PADDING)
			if err != nil {
				return fmt.Errorf("failed to parse data file segment: %w", err)
			}

			imap := indexs[inum%uint64(shard)]
			if imap != nil {
				if segment.IsTombstone() {
					delete(imap.index, inum)
					offset += int64(segment.Size())
					continue
				}

				if segment.ExpiredAt > 0 && segment.ExpiredAt <= time.Now().UnixMicro() {
					offset += int64(segment.Size())
					continue
				}

				imap.index[inum] = &inode{
					RegionID:  regionId,
					Position:  offset,
					Length:    segment.Size(),
					CreatedAt: segment.CreatedAt,
					ExpiredAt: segment.ExpiredAt,
					mvcc:      0,
				}

				offset += int64(segment.Size())
			} else {
				return errors.New("no corresponding index shard")
			}
		}
	}

	return nil
}

func validateFileHeader(file *os.File) error {
	var fileHeader [4]byte
	n, err := file.Read(fileHeader[:])
	if err != nil {
		return err
	}

	if n != len(dataFileMetadata) {
		return errors.New("file is too short to contain valid signature")
	}

	if !bytes.Equal(fileHeader[:], dataFileMetadata[:]) {
		return fmt.Errorf("unsupported data file version: %v", file.Name())
	}

	return nil
}

func checkFileSystem(path string, fsPerm fs.FileMode) error {
	if !utils.IsExist(path) {
		err := os.MkdirAll(path, fsPerm)
		if err != nil {
			return err
		}
	}

	files, err := os.ReadDir(path)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	if len(files) > 0 {
		for _, file := range files {
			if !file.IsDir() && strings.HasSuffix(file.Name(), fileExtension) {
				if strings.HasPrefix(file.Name(), "0") {
					file, err := os.Open(filepath.Join(path, file.Name()))
					if err != nil {
						return fmt.Errorf("failed to check data file: %w", err)
					}
					defer file.Close()

					err = validateFileHeader(file)
					if err != nil {
						return fmt.Errorf("failed to validated data file header: %w", err)
					}
				}
			}

			if !file.IsDir() && file.Name() == indexFileName {
				file, err := os.Open(filepath.Join(path, file.Name()))
				if err != nil {
					return fmt.Errorf("failed to check index file: %w", err)
				}
				defer file.Close()

				err = validateFileHeader(file)
				if err != nil {
					return fmt.Errorf("failed to validated index file header: %w", err)
				}
			}
		}
	}

	return nil
}

// | DEL 1 | KIND 1 | EAT 8 | CAT 8 | KLEN 4 | VLEN 4 | KEY ? | VALUE ? | CRC32 4 |
func readSegment(fd *os.File, offset int64, bufsize int64) (uint64, *Segment, error) {
	buf := make([]byte, bufsize)

	_, err := fd.ReadAt(buf, offset)
	if err != nil {
		return 0, nil, err
	}

	var seg Segment
	readOffset := 0

	// Parse Tombstone (1 byte)
	seg.Tombstone = int8(buf[readOffset])
	readOffset++

	// Parse Type (1 byte)
	seg.Type = kind(buf[readOffset])
	readOffset++

	// Parse ExpiredAt (8 bytes)
	seg.ExpiredAt = int64(binary.LittleEndian.Uint64(buf[readOffset : readOffset+8]))
	readOffset += 8

	// Parse CreatedAt (8 bytes)
	seg.CreatedAt = int64(binary.LittleEndian.Uint64(buf[readOffset : readOffset+8]))
	readOffset += 8

	// Parse KeySize (4 bytes)
	seg.KeySize = int32(binary.LittleEndian.Uint32(buf[readOffset : readOffset+4]))
	readOffset += 4

	// Parse ValueSize (4 bytes)
	seg.ValueSize = int32(binary.LittleEndian.Uint32(buf[readOffset : readOffset+4]))
	readOffset += 4

	// End of Header 26 bytes

	// Read Key data
	keybuf := make([]byte, seg.KeySize)
	_, err = fd.ReadAt(keybuf, int64(offset)+int64(readOffset))
	if err != nil {
		return 0, nil, fmt.Errorf("failed to parse key in segment: %w", err)
	}
	readOffset += int(seg.KeySize)

	// Read Value data
	valuebuf := make([]byte, seg.ValueSize)
	_, err = fd.ReadAt(valuebuf, int64(offset)+int64(readOffset))
	if err != nil {
		return 0, nil, fmt.Errorf("failed to parse value in segment: %w", err)
	}
	readOffset += int(seg.ValueSize)

	// Read checksum (4 bytes)
	checksumBuf := make([]byte, 4)
	_, err = fd.ReadAt(checksumBuf, int64(offset)+int64(readOffset))
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read checksum in segment: %w", err)
	}

	// Verify checksum
	checksum := binary.LittleEndian.Uint32(checksumBuf)

	buf = append(buf, keybuf...)
	buf = append(buf, valuebuf...)

	if checksum != crc32.ChecksumIEEE(buf) {
		return 0, nil, fmt.Errorf("failed to crc32 checksum mismatch: %d", checksum)
	}

	// Update Segment data fields with the read valuebuf and process it through Transformer before use
	decodedData, err := transformer.Decode(valuebuf)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to transformer decode value in segment: %w", err)
	}

	seg.Key = keybuf
	seg.Value = decodedData

	return inodeNum(string(keybuf)), &seg, nil
}

func generateFileName(regionID int64) (string, error) {
	fileName := formatDataFileName(regionID)
	// Verify if regionID starts with 0 (valid only for 8 digits)
	if strings.HasPrefix(fileName, "0") {
		return fileName, nil
	}
	// Throw an exception if the regionID exceeds the current set number of data files
	return "", fmt.Errorf("new region id %d cannot be converted to a valid file name", regionID)
}

// parseDataFileName converts the numeric part of the file name (e.g., 0000001.wdb) to uint64
func parseDataFileName(fileName string) (int64, error) {
	parts := strings.Split(fileName, ".")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid file name format: %s", fileName)
	}

	// Convert to uint64
	number, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse number from file name: %w", err)
	}

	return int64(number), nil
}

// formatDataFileName converts uint64 to file name format (e.g., 1 to 0000001.wdb)
func formatDataFileName(number int64) string {
	return fmt.Sprintf("%010d%s", number, fileExtension)
}

func checkpointFileName(regionID int64) string {
	return fmt.Sprintf("ckpt.%d.%d.tmp", time.Now().Unix(), regionID)
}

// serializedIndex serializes the index to a recoverable file snapshot record format:
// | INUM 8 | RID 8  | POS 8 | LEN 4 | EAT 8 | CAT 8 | T 1 | CRC32 4 | = len(48 bytes)
func serializedIndex(buf *bytes.Buffer, inum uint64, inode *inode) ([]byte, error) {
	// reset a byte buffer
	buf.Reset()

	// Write each field in order
	binary.Write(buf, binary.LittleEndian, inum)
	binary.Write(buf, binary.LittleEndian, inode.RegionID)
	binary.Write(buf, binary.LittleEndian, inode.Position)
	binary.Write(buf, binary.LittleEndian, inode.Length)
	binary.Write(buf, binary.LittleEndian, inode.ExpiredAt)
	binary.Write(buf, binary.LittleEndian, inode.CreatedAt)
	binary.Write(buf, binary.LittleEndian, inode.Type)

	// Calculate CRC32 checksum
	checksum := crc32.ChecksumIEEE(buf.Bytes())

	// Write CRC32 checksum to byte buffer (4 bytes)
	binary.Write(buf, binary.LittleEndian, checksum)

	// Return byte slice containing CRC32 checksum
	return buf.Bytes(), nil
}

// deserializedIndex restores the index file snapshot to an in-memory struct:
// | INUM 8 | RID 8  | OFS 8 | LEN 4 | EAT 8 | CAT 8 | CRC32 4 | = len(48 bytes)
func deserializedIndex(data []byte) (uint64, *inode, error) {
	buf := bytes.NewReader(data)
	var inum uint64
	err := binary.Read(buf, binary.LittleEndian, &inum)
	if err != nil {
		return 0, nil, err
	}

	// Deserialize each field of inode
	var inode inode
	err = binary.Read(buf, binary.LittleEndian, &inode.RegionID)
	if err != nil {
		return 0, nil, err
	}

	err = binary.Read(buf, binary.LittleEndian, &inode.Position)
	if err != nil {
		return 0, nil, err
	}

	err = binary.Read(buf, binary.LittleEndian, &inode.Length)
	if err != nil {
		return 0, nil, err
	}

	err = binary.Read(buf, binary.LittleEndian, &inode.ExpiredAt)
	if err != nil {
		return 0, nil, err
	}

	err = binary.Read(buf, binary.LittleEndian, &inode.CreatedAt)
	if err != nil {
		return 0, nil, err
	}

	err = binary.Read(buf, binary.LittleEndian, &inode.Type)
	if err != nil {
		return 0, nil, err
	}

	// Deserialize and verify CRC32 checksum
	var checksum uint32
	err = binary.Read(buf, binary.LittleEndian, &checksum)
	if err != nil {
		return 0, nil, err
	}

	// Calculate CRC32 checksum of data, return an error if checksum does not match
	if checksum != crc32.ChecksumIEEE(data[:len(data)-4]) {
		return 0, nil, fmt.Errorf("failed to crc32 checksum mismatch: %d", checksum)
	}

	return inum, &inode, nil
}

func serializedSegment(seg *Segment) ([]byte, error) {
	buf := new(bytes.Buffer)

	err := binary.Write(buf, binary.LittleEndian, seg.Tombstone)
	if err != nil {
		return nil, fmt.Errorf("failed to write Tombstone: %w", err)
	}

	err = binary.Write(buf, binary.LittleEndian, seg.Type)
	if err != nil {
		return nil, fmt.Errorf("failed to write Type: %w", err)
	}

	err = binary.Write(buf, binary.LittleEndian, seg.ExpiredAt)
	if err != nil {
		return nil, fmt.Errorf("failed to write ExpiredAt: %w", err)
	}

	err = binary.Write(buf, binary.LittleEndian, seg.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to write CreatedAt: %w", err)
	}

	err = binary.Write(buf, binary.LittleEndian, seg.KeySize)
	if err != nil {
		return nil, fmt.Errorf("failed to write KeySize: %w", err)
	}

	err = binary.Write(buf, binary.LittleEndian, seg.ValueSize)
	if err != nil {
		return nil, fmt.Errorf("failed to write ValueSize: %w", err)
	}

	err = binary.Write(buf, binary.LittleEndian, seg.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to write Key: %w", err)
	}

	err = binary.Write(buf, binary.LittleEndian, seg.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to write Value: %w", err)
	}

	checksum := crc32.ChecksumIEEE(buf.Bytes())

	err = binary.Write(buf, binary.LittleEndian, checksum)
	if err != nil {
		return nil, fmt.Errorf("failed to write checksum: %w", err)
	}

	return buf.Bytes(), nil
}

// Garbage Collection Compressor
// Steps:
// 1. If no index snapshot exists on disk, perform a global scan to restore the index.
// 2. After the index is restored, run for a while before triggering garbage collection.
// 3. Start the GC process by scanning disk data files and comparing them with the latest in-memory index.
// 4. If a record in the disk file matches the index record, migrate it to a new file.
// 5. If no match is found, the file is considered outdated; skip it and continue the process.
// 6. Repeat the process until the GC has scanned all data files, then delete the original files.
// 7. Note: The key point is reverse scanning. Use keys from the disk data files to locate and compare records in memory.
// 8. If the in-memory index is used to locate records, it becomes impossible to determine if a file has been fully scanned.
// 9. This is because records in the in-memory index may be distributed across multiple data files on disk.
func (lfs *LogStructuredFS) cleanupDirtyRegions() error {
	if len(lfs.regions) >= 5 {
		var regionIds []int64
		for v := range lfs.regions {
			regionIds = append(regionIds, v)
		}
		sort.Slice(regionIds, func(i, j int) bool {
			return regionIds[i] < regionIds[j]
		})

		// find 40% dirty region
		for i := 0; i < 4 && i < len(regionIds); i++ {
			lfs.dirtyRegions = append(lfs.dirtyRegions, lfs.regions[regionIds[i]])
		}

		// Cleanup dirty region
		defer func() {
			lfs.dirtyRegions = nil
		}()

		for _, fd := range lfs.dirtyRegions {
			finfo, err := fd.Stat()
			if err != nil {
				return err
			}

			readOffset := int64(len(dataFileMetadata))

			for readOffset < finfo.Size() {
				inum, segment, err := readSegment(fd, readOffset, _SEGMENT_PADDING)
				if err != nil {
					return err
				}

				imap := lfs.indexs[inum%uint64(shard)]
				if imap != nil {
					imap.mu.RLock()
					inode, ok := imap.index[inum]
					imap.mu.RUnlock()

					if !ok {
						continue
					}

					if isValid(segment, inode) {
						bytes, err := serializedSegment(segment)
						if err != nil {
							return err
						}

						// 缩小锁的颗粒度
						lfs.mu.Lock()
						err = appendToActiveRegion(lfs.active, bytes)
						if err != nil {
							lfs.mu.Unlock()
							return err
						}

						delete(lfs.regions, inode.RegionID)

						inode.Position = lfs.offset
						inode.RegionID = lfs.regionID

						lfs.offset += int64(segment.Size())
						lfs.mu.Unlock()

						readOffset += int64(segment.Size())

					} else {
						// next segment
						readOffset += int64(segment.Size())
						continue
					}

				} else {
					return fmt.Errorf("imap is nil for inum = %d", inum)
				}

				if atomic.LoadInt64(&lfs.offset) >= lfs.regionThreshold {
					err = lfs.changeRegions()
					if err != nil {
						return fmt.Errorf("failed to close active migrate region: %w", err)
					}
				}

			}

			// Delete dirty region file
			lfs.mu.Lock()
			err = os.Remove(filepath.Join(lfs.directory, fd.Name()))
			lfs.mu.Unlock()
			if err != nil {
				return fmt.Errorf("failed to remove dirty region: %w", err)
			}

		}
	} else {
		clog.Warnf("dirty regions (%d%%) does not meet garbage collection status", len(lfs.regions)/10)
	}

	return nil
}

func isValid(seg *Segment, inode *inode) bool {
	return !seg.IsTombstone() &&
		seg.CreatedAt == inode.CreatedAt &&
		(seg.ExpiredAt == ImmortalTTL || time.Now().UnixMicro() < seg.ExpiredAt)
}

// Start serializing little-endian data, needs to compress seg before writing.
func appendToActiveRegion(fd *os.File, bytes []byte) error {
	// Write the byte stream to the file
	n, err := fd.Write(bytes)
	if err != nil {
		return fmt.Errorf("failed to append binary data to active region: %w", err)
	}

	// Check if the number of written bytes matches
	if n != len(bytes) {
		return fmt.Errorf("partial write error: expected %d bytes, but wrote %d bytes", len(bytes), n)
	}

	return nil
}

func cleanupDirtyCheckpoint(directory, newCheckpoint string) error {
	files, err := filepath.Glob(filepath.Join(directory, "*.ids"))
	if err != nil {
		return err
	}

	for _, file := range files {
		if filepath.Base(file) != newCheckpoint {
			err := os.Remove(file)
			if err != nil {
				return fmt.Errorf("deleted old checkpoint file: %s", err)
			}
		}
	}

	tmps, err := filepath.Glob(filepath.Join(directory, "*.tmp"))
	if err != nil {
		return err
	}

	for _, file := range tmps {
		err := os.Remove(file)
		if err != nil {
			return fmt.Errorf("deleted old temp checkpoint file: %s", err)
		}
	}

	return nil
}

func scanAndRecoverCheckpoint(files []string, regions map[int64]*os.File, indexs []*indexMap) error {
	var (
		ckpt    int
		path    string
		pauseID string
	)

	for _, file := range files {
		parts := strings.Split(file, ".")
		if len(parts) == 4 {
			ts, err := strconv.Atoi(parts[1])
			if err != nil {
				return fmt.Errorf("failed to split checkpoint name: %w", err)
			}

			if ts > ckpt {
				ckpt = ts
				path = file
				pauseID = parts[2]
			}
		}
	}

	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open checkpoint file: %w", err)
	}
	defer file.Close()

	err = recoveryIndex(file, indexs)
	if err != nil {
		return fmt.Errorf("failed to recover data from checkpoint: %w", err)
	}

	// 由于检查点不是实时的索引快照，再从检查点之后数据文件进行恢复完整数据
	pid, err := strconv.Atoi(pauseID)
	if err != nil {
		return err
	}

	var regionIds []int64
	for id := range regions {
		if id >= int64(pid) {
			regionIds = append(regionIds, id)
		}
	}

	sort.Slice(regionIds, func(i, j int) bool {
		return regionIds[i] < regionIds[j]
	})

	for _, regionId := range regionIds {
		fd, ok := regions[regionId]
		if !ok {
			return fmt.Errorf("data file does not exist regions id: %d", regionId)
		}

		finfo, err := fd.Stat()
		if err != nil {
			return err
		}

		offset := int64(len(dataFileMetadata))

		for offset < finfo.Size() {
			inum, segment, err := readSegment(fd, offset, _SEGMENT_PADDING)
			if err != nil {
				return fmt.Errorf("failed to parse data file segment: %w", err)
			}

			imap := indexs[inum%uint64(shard)]
			if imap != nil {
				if segment.IsTombstone() {
					delete(imap.index, inum)
					offset += int64(segment.Size())
					continue
				}

				if segment.ExpiredAt <= time.Now().UnixMicro() && segment.ExpiredAt != 0 {
					offset += int64(segment.Size())
					continue
				}

				imap.index[inum] = &inode{
					RegionID:  regionId,
					Position:  offset,
					Length:    segment.Size(),
					CreatedAt: segment.CreatedAt,
					ExpiredAt: segment.ExpiredAt,
					mvcc:      0,
				}

				offset += int64(segment.Size())
			} else {
				return errors.New("no corresponding index shard")
			}
		}
	}

	return nil
}
