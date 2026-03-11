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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

var ErrEmptyBeginSnapshot = errors.New("unexpected empty begin snapshot")

// 全句事物 ID 每次创建一个新的事物就会自增 1 ，保证每个事物都有一个唯一的 ID 所对应的 .txn 文件了，
// 这个 ID 就是 .txn 文件的文件名，系统重启的时候就会去读取这个 .txn 文件来恢复对应 key 的老数据版本了。
// 每次重新启动之后 ID 源会被重置，之前的 .txn 文件会被删除，新的 .txn 的文件 ID 会重新的从 1 开始了，这样就保证了每次启动之后 ID 都是唯一的了。
var globalTxnId atomic.Uint64

func acquireTxnId() uint64 {
	return globalTxnId.Add(1)
}

// PS：事务的实现是非常复杂的，原子性一定是首要前提，这里的原子就是对涉及到多个 segment 批量原子写！！！
// 没有中间状态，要么全部成功，要么全部失败，不能出现部分成功的情况！！！
// 所以在事务执行过程中要保证数据的一致性和安全性！！！

// 只要磁盘上有 .txn 文件了就说明有未提交的事务了，上次运行过程中有事物为能成功执行。
// 存储引擎在启动的时候就要去读取 .txn 文件中的数据来恢复未提交对应 key 的老数据版本。
// 为什么要有 .txn 文件，是因为事物执行过程中可能会有一些数据写入到磁盘上了。
// 但是还没有提交，万一这个时候系统崩溃了数据就丢了，所以要有 .txn 文件来记录是对应 key 的老数据版本。
// 等系统重启的时候再去读取 .txn 文件中的数据来恢复对应 key 的老数据版本，这样就保证了数据的安全性和一致性了。
type Transaction struct {
	fd        *os.File
	id        uint64
	path      string
	store     *LogStructuredFS
	execute   func(ctx *TxnState) error
	snapshots []*Snapshot
	once      sync.Once
	newKeys   []string
}

type TxnState struct {
	fd     *os.File
	store  *LogStructuredFS
	writes []*Snapshot
	reads  []*Snapshot
	keys   map[string]struct{}
}

// 运算完成之后的结果进行持久化存储，注意这里的 segs 可能有新加入的新 key 不在 Begin 中返回的。
func (ctx *TxnState) Save(snaps []*Snapshot) error {
	ctx.writes = snaps
	return nil
}

// 事物开始的时候对本次事物需要的 keys 进行批量获取操作，方便后面进行运算。
func (ctx *TxnState) Begin(keys []string) ([]*Snapshot, error) {
	var result []*Snapshot
	ctx.keys = make(map[string]struct{}, len(keys))

	for _, key := range keys {
		mvcc, seg, err := ctx.store.FetchSegment(key)
		if err != nil {
			return nil, err
		}
		ctx.keys[key] = struct{}{}
		result = append(result, &Snapshot{
			Segment: seg,
			mvcc:    mvcc,
		})
	}

	if len(result) == 0 {
		return nil, ErrEmptyBeginSnapshot
	}

	// 这里要注意对 result 中的每个 Snapshot 进行深复制，不能直接把 result 中的 Snapshot 的指针赋值给 ctx.reads，
	// 因为后面可能会对 ctx.reads 中的 Snapshot 进行修改，导致 result 中的 Snapshot 也被修改了，这样就不符合事务的 MVCC 原子性了。
	ctx.reads = make([]*Snapshot, len(result))
	for i, s := range result {
		cp := *s
		ctx.reads[i] = &cp
	}

	buf := make([]byte, 0, 1024)
	for _, sp := range result {
		bytes, err := serializedSegment(sp.Segment)
		if err != nil {
			return nil, err
		}
		buf = append(buf, bytes...)
	}

	err := appendToActiveRegion(ctx.fd, buf)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// 为什么单独设计一个 Snapshot 是因为需要做事物中的 key 对应的版本冲突检测。
type Snapshot struct {
	*Segment
	mvcc uint64
}

// hasConflict 用于 MVCC 版本号冲突检测方法，事物提交成功之后必须是批量比较版本号，
// 成功提交条件是 len(tnxs) == len(version) 这里的 version 类型是 bool ，必须所有事物的比较结果都是 true 才能成功提交。
func (s *Snapshot) hasConflict(version uint64) bool {
	return s.mvcc == version
}

// 这里的 keys 是事务涉及到的 key 列表，事务执行过程中会对这些 key 进行读写操作，
// 所以需要在事务开始的时候就把这些 key 传进来，这样就可以在事务执行过程中对这些 key 进行操作了。
// 拿到这些 key 对应磁盘老版本数据写到 .txn 文件中，这样就保证了能在事物执行失败时执行回滚操作。
func (store *LogStructuredFS) NewTransaction() (*Transaction, error) {
	txnId := acquireTxnId()
	txnPath := filepath.Join(store.directory, txnDirName, fmt.Sprintf("%d%s", txnId, txnExtension))
	fd, err := os.OpenFile(txnPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, store.fsPerm)
	if err != nil {
		return nil, fmt.Errorf("failed to create new transaction file: %w", err)
	}

	n, err := fd.Write(dataFileMetadata)
	if err != nil {
		_ = fd.Close()
		_ = os.Remove(txnPath)
		return nil, fmt.Errorf("failed to write metadata to transaction file: %w", err)
	}

	if n != len(dataFileMetadata) {
		_ = fd.Close()
		_ = os.Remove(txnPath)
		return nil, fmt.Errorf("failed to write full metadata to transaction file")
	}

	return &Transaction{
		fd:    fd,
		store: store,
		id:    txnId,
		path:  txnPath,
	}, nil
}

// 本次事物执行过程中新 keys 对应的新版本的 segment 进行持久化到 .db 文件中并且更新索引 inode 和 .db 文件映射关系。
func (t *Transaction) AtomicBatch(callback func(ctx *TxnState) error) {
	t.once.Do(func() {
		t.execute = callback
	})
}

func (t *Transaction) TxnID() uint64 {
	return t.id
}

// PS：这里 Commit 是将缓冲区中的数据和对应 .txn 文件执行删除，Delete 操作是整个 commit 函数结尾执行的函数！！！
// 为什么这样？delete 系统调用就是一个原子操作，如果事物对应的 .txn 文件删除了就说明这个事物提交成功了，系统崩溃了也不会再去读取这个 .txn 文件了，
// 如果没有删除就说明这个事物提交失败了，系统崩溃了就会去读取这个 .txn 文件来恢复对应 key 的老数据版本了。

// 这样下去启动的时候存储引擎就不会再去读取 .txn 文件了，
// 没有 .txn 文件了就说明没有未提交的事务了，事物执行成功了，一定要做版本控制检查器，检查每个数据的版本。
// Commit 最重要一个环境就是对应 key 的新版本的 segment 进行持久化到 .db 文件中并且更新索引 inode 和 .db 文件映射关系。
func (t *Transaction) Commit() error {
	state := &TxnState{store: t.store, fd: t.fd}
	err := t.execute(state)
	if err != nil {
		return err
	}

	t.snapshots = state.reads
	buf := make([]byte, 0, _PAGE_SIZE_4KB)

	var allowed, conflict = true, (*Snapshot)(nil)
	for _, seg := range state.writes {
		key := seg.KeyString()
		// 新 key 就不需要做版本控制检查了，直接添加到 .txn 文件中就好了
		if _, ok := state.keys[key]; !ok {
			// 中途新 key 应该添加一条删除记录
			t.newKeys = append(t.newKeys, key)
			// 事物启动时直接回滚到没有这个 key 状态下
			tombstone := NewTombstoneSegment(key)
			bytes, err := serializedSegment(tombstone)
			if err != nil {
				return err
			}
			buf = append(buf, bytes...)
			continue
		}

		err := appendToActiveRegion(state.fd, buf)
		if err != nil {
			return err
		}

		// 老版本的 key 必须做版本检查。
		if version, ok := t.store.mvcc(key); ok && !seg.hasConflict(version) {
			conflict, allowed = seg, false
		}
	}

	// 全部检查通过之后就可以进行提交了。
	if allowed {
		err := t.store.CommitTxns(state.writes)
		if err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
		err = os.Remove(t.path)
		if err != nil {
			return fmt.Errorf("failed to commit delete transaction: %w", err)
		}
		return nil
	}

	return fmt.Errorf("mvcc version conflict for key %q", conflict.KeyString())
}

// 这里 Rollback 是将缓冲区对应磁盘 .txn 中的数据写会到 .db 文件中，
// 写回操作要注意更新索引 inode 和 .db 文件映射关系，同样删除 .txn 文件，
// 这样下去启动的时候存储引擎就不会再去读取 .txn 文件了。
func (t *Transaction) Rollback() error {
	if len(t.snapshots) == 0 {
		return ErrEmptyBeginSnapshot
	}

	err := t.store.RollbackTxns(t.newKeys, t.snapshots)
	if err != nil {
		return fmt.Errorf("failed to rollback transaction: %w", err)
	}
	err = os.Remove(t.path)
	if err != nil {
		return fmt.Errorf("failed to rollback delete transaction file: %w", err)
	}

	return nil
}
