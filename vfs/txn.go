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
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
)

// 全句事物 ID 每次创建一个新的事物就会自增 1 ，保证每个事物都有一个唯一的 ID 所对应的 .txn 文件了，
// 这个 ID 就是 .txn 文件的文件名，系统重启的时候就会去读取这个 .txn 文件来恢复对应 key 的老数据版本了。
// 每次重新启动之后 ID 源会被重置，之前的 .txn 文件会被删除，新的 .txn 的文件 ID 会重新的从 1 开始了，这样就保证了每次启动之后 ID 都是唯一的了。
var globalTxnId atomic.Int64

func acquireTxnId() int64 {
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
	fd    *os.File
	id    int64
	path  string
	buf   *bytes.Buffer
	store *LogStructuredFS
}

// 这里的 keys 是事务涉及到的 key 列表，事务执行过程中会对这些 key 进行读写操作，
// 所以需要在事务开始的时候就把这些 key 传进来，这样就可以在事务执行过程中对这些 key 进行操作了。
// 拿到这些 key 对应磁盘老版本数据写到 .txn 文件中，这样就保证了能在事物执行失败时执行回滚操作。
func NewTransaction(store *LogStructuredFS, keys []string) (*Transaction, error) {
	txn_id := acquireTxnId()
	txn_path := filepath.Join(store.directory, txnDirName, fmt.Sprintf("%d%s", txn_id, txnExtension))

	txn_fd, err := os.OpenFile(txn_path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, store.fsPerm)
	if err != nil {
		return nil, fmt.Errorf("failed to create new transaction file: %w", err)
	}

	n, err := txn_fd.Write(dataFileMetadata)
	if err != nil {
		_ = txn_fd.Close()
		_ = os.Remove(txn_path)
		return nil, fmt.Errorf("failed to write metadata to transaction file: %w", err)
	}

	if n != len(dataFileMetadata) {
		_ = txn_fd.Close()
		_ = os.Remove(txn_path)
		return nil, fmt.Errorf("failed to write full metadata to transaction file")
	}

	return &Transaction{
		fd:    txn_fd,
		store: store,
		buf:   bytes.NewBuffer(make([]byte, 1024)),
		id:    txn_id,
		path:  txn_path,
	}, nil
}

// 本次事物执行过程中新 keys 对应的新版本的 segment 进行持久化到 .db 文件中并且更新索引 inode 和 .db 文件映射关系。
func (t *Transaction) WriteSegments(segs []*Segment) (int, error) {
	return 0, nil
}

func (t *Transaction) TxnID() int64 {
	return t.id
}

// PS：这里 Commit 是将缓冲区中的数据和对应 .txn 文件执行删除，Delete 操作是整个 commit 函数结尾执行的函数！！！
// 为什么这样？delete 系统调用就是一个原子操作，如果事物对应的 .txn 文件删除了就说明这个事物提交成功了，系统崩溃了也不会再去读取这个 .txn 文件了，
// 如果没有删除就说明这个事物提交失败了，系统崩溃了就会去读取这个 .txn 文件来恢复对应 key 的老数据版本了。

// 这样下去启动的时候存储引擎就不会再去读取 .txn 文件了，
// 没有 .txn 文件了就说明没有未提交的事务了，事物执行成功了，
// Commit 最重要一个环境就是对应 key 的新版本的 segment 进行持久化到 .db 文件中并且更新索引 inode 和 .db 文件映射关系。
func (t *Transaction) Commit() error {
	return nil
}

// 这里 Rollback 是将缓冲区对应磁盘 .txn 中的数据写会到 .db 文件中，
// 写回操作要注意更新索引 inode 和 .db 文件映射关系，同样删除 .txn 文件，
// 这样下去启动的时候存储引擎就不会再去读取 .txn 文件了。
func (t *Transaction) Rollback() error {
	return nil
}

type TransactionManager struct{}
