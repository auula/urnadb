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
	"os"
	"sync"
	"testing"
	"time"

	"github.com/auula/urnadb/conf"
	"github.com/auula/urnadb/types"
)

func testPutSegment(store *LogStructuredFS) {
	variant := types.NewVariant("test variant transaction.")
	seg1, err := NewSegment("key1", variant, ImmortalTTL)
	if err != nil {
		panic(err)
	}

	err = store.PutSegment("key1", seg1)
	if err != nil {
		panic(err)
	}

	seg2, err := NewSegment("key2", variant, ImmortalTTL)
	if err != nil {
		panic(err)
	}

	err = store.PutSegment("key2", seg2)
	if err != nil {
		panic(err)
	}
}

func TestCommitTransaction(t *testing.T) {
	// 这里可以测试一下事务的提交和回滚功能了，确保事务的原子性和一致性了。
	fss, err := OpenFS(&Options{
		FSPerm:    conf.FSPerm,
		Path:      conf.Settings.Path,
		Threshold: conf.Settings.Region.Threshold,
	})
	if err != nil {
		t.Fatal(err)
	}

	testPutSegment(fss)

	txns, err := fss.NewTransaction()
	if err != nil {
		t.Fatal(err)
	}

	txns.AtomicBatch(func(txns *TxnState) error {
		keys := []string{"key1", "key2"}
		snapshots, err := txns.Begin(keys)
		if err != nil {
			return err
		}

		snapshots["key1"].Value = []byte("test transaction 1.")
		snapshots["key1"].ValueSize = int32(len(snapshots["key1"].Value))

		// 模拟事务执行过程中可能会有一些耗时的操作了，方便观察数据目录有没有 .txn 文件产生。
		time.Sleep(3 * time.Second)

		snapshots["key2"].Value = []byte("test transaction 2.")
		snapshots["key2"].ValueSize = int32(len(snapshots["key2"].Value))

		seg, _ := NewSegment("key3", &types.Variant{}, ImmortalTTL)

		snapshots["key3"] = &Snapshot{
			mvcc:    1,
			Segment: seg,
		}

		return txns.Save(snapshots)
	})

	if err := txns.Commit(); err != nil {
		inner := txns.Rollback()
		if !errors.Is(err, ErrEmptyBeginSnapshot) {
			t.Fatal(errors.Join(err, inner))
		}
		t.Fatal(err)
	}

	_, seg, err := fss.FetchSegment("key1")
	if err != nil {
		t.Fatal(err)
	}

	if string(seg.Value) != "test transaction 1." {
		t.Fatalf("expected value to be 'test transaction 1.', but got: %s", string(seg.Value))
	}

	_, seg, err = fss.FetchSegment("key2")
	if err != nil {
		t.Fatal(err)
	}

	if string(seg.Value) != "test transaction 2." {
		t.Fatalf("expected value to be 'test transaction 2.', but got: %s", string(seg.Value))
	}
}

func TestRollbackTransaction(t *testing.T) {
	fss, err := OpenFS(&Options{
		FSPerm:    conf.FSPerm,
		Path:      conf.Settings.Path,
		Threshold: conf.Settings.Region.Threshold,
	})
	if err != nil {
		t.Fatal(err)
	}

	txns, err := fss.NewTransaction()
	if err != nil {
		t.Fatal(err)
	}

	txns.AtomicBatch(func(txns *TxnState) error {
		return errors.New("simulated transaction failure")
	})

	if err := txns.Commit(); err != nil {
		err := txns.Rollback()
		if !errors.Is(err, ErrEmptyBeginSnapshot) {
			t.Fatal(err)
		}
	}
}

func TestEmptyBeginSnapshot(t *testing.T) {
	fss, err := OpenFS(&Options{
		FSPerm:    conf.FSPerm,
		Path:      conf.Settings.Path,
		Threshold: conf.Settings.Region.Threshold,
	})
	if err != nil {
		t.Fatal(err)
	}

	txns, err := fss.NewTransaction()
	if err != nil {
		t.Fatal(err)
	}

	txns.AtomicBatch(func(txns *TxnState) error {
		_, err := txns.Begin([]string{})
		if err != nil {
			return err
		}
		return txns.Save(nil)
	})

	if err := txns.Commit(); err != nil {
		if !errors.Is(err, ErrEmptyBeginSnapshot) {
			t.Fatal(err)
		}
	}
}

func TestConflictTransaction(t *testing.T) {
	var wg sync.WaitGroup
	fss, err := OpenFS(&Options{
		FSPerm:    conf.FSPerm,
		Path:      conf.Settings.Path,
		Threshold: conf.Settings.Region.Threshold,
	})

	if err != nil {
		t.Fatal(err)
	}

	testPutSegment(fss)

	wg.Add(2)

	go func() {
		defer wg.Done()
		txns, _ := fss.NewTransaction()

		txns.AtomicBatch(func(txns *TxnState) error {
			keys := []string{"key1", "key2"}
			snapshots, err := txns.Begin(keys)
			if err != nil {
				return err
			}

			snapshots["key1"].Value = []byte("A-test transaction 1.")
			snapshots["key1"].ValueSize = int32(len(snapshots["key1"].Value))

			time.Sleep(2 * time.Second)

			snapshots["key2"].Value = []byte("A-test transaction 2.")
			snapshots["key2"].ValueSize = int32(len(snapshots["key2"].Value))

			return txns.Save(snapshots)
		})

		if err := txns.Commit(); err != nil {
			t.Log(err)
		}
	}()

	go func() {
		defer wg.Done()
		txns, _ := fss.NewTransaction()

		txns.AtomicBatch(func(txns *TxnState) error {
			keys := []string{"key1", "key2"}
			snapshots, err := txns.Begin(keys)
			if err != nil {
				return err
			}

			time.Sleep(3 * time.Second)

			snapshots["key1"].Value = []byte("B-test transaction 1.")
			snapshots["key1"].ValueSize = int32(len(snapshots["key1"].Value))

			snapshots["key2"].Value = []byte("B-test transaction 2.")
			snapshots["key2"].ValueSize = int32(len(snapshots["key2"].Value))
			return txns.Save(snapshots)
		})

		if err := txns.Commit(); err != nil {
			t.Log(err)
		}
	}()

	wg.Wait()
}

func TestCommitTxnsError(t *testing.T) {
	_ = os.RemoveAll(conf.Settings.Path)

	fss, err := OpenFS(&Options{
		FSPerm:    conf.FSPerm,
		Path:      conf.Settings.Path,
		Threshold: conf.Settings.Region.Threshold,
	})
	if err != nil {
		t.Fatal(err)
	}

	testPutSegment(fss)

	txns, err := fss.NewTransaction()
	if err != nil {
		t.Fatal(err)
	}

	txns.AtomicBatch(func(txns *TxnState) error {
		keys := []string{"key1"}
		snapshots, err := txns.Begin(keys)
		if err != nil {
			return err
		}

		snapshots["key1"].Value = []byte("test commit error")
		snapshots["key1"].ValueSize = int32(len(snapshots["key1"].Value))

		// 关闭 active region 文件，导致 CommitTxns 中的 appendToActiveRegion 失败
		_ = fss.active.Close()

		return txns.Save(snapshots)
	})

	err = txns.Commit()
	if err == nil {
		t.Fatal("expected commit to fail with closed active region")
	}
	t.Logf("Got expected error: %v", err)

	// 验证 rollback 标志被设置
	if !txns.rollback {
		t.Error("expected rollback flag to be true after commit failure")
	}
}

func TestCommitRemoveError(t *testing.T) {
	_ = os.RemoveAll(conf.Settings.Path)

	fss, err := OpenFS(&Options{
		FSPerm:    conf.FSPerm,
		Path:      conf.Settings.Path,
		Threshold: conf.Settings.Region.Threshold,
	})
	if err != nil {
		t.Fatal(err)
	}

	testPutSegment(fss)

	txns, err := fss.NewTransaction()
	if err != nil {
		t.Fatal(err)
	}

	txns.AtomicBatch(func(txns *TxnState) error {
		keys := []string{"key1"}
		snapshots, err := txns.Begin(keys)
		if err != nil {
			return err
		}

		snapshots["key1"].Value = []byte("test remove error")
		snapshots["key1"].ValueSize = int32(len(snapshots["key1"].Value))

		return txns.Save(snapshots)
	})

	// 提前删除 .txn 文件，导致 os.Remove 失败
	_ = txns.fd.Close()
	_ = os.Remove(txns.path)

	err = txns.Commit()
	if err == nil {
		t.Fatal("expected commit to fail when removing txn file")
	}
	t.Logf("Got expected error: %v", err)
}

func TestRollbackWithNewKeys(t *testing.T) {
	_ = os.RemoveAll(conf.Settings.Path)

	fss, err := OpenFS(&Options{
		FSPerm:    conf.FSPerm,
		Path:      conf.Settings.Path,
		Threshold: conf.Settings.Region.Threshold,
	})
	if err != nil {
		t.Fatal(err)
	}

	testPutSegment(fss)

	txns, err := fss.NewTransaction()
	if err != nil {
		t.Fatal(err)
	}

	var activeFd *os.File
	txns.AtomicBatch(func(txns *TxnState) error {
		keys := []string{"key1"}
		snapshots, err := txns.Begin(keys)
		if err != nil {
			return err
		}

		snapshots["key1"].Value = []byte("modified value")
		snapshots["key1"].ValueSize = int32(len(snapshots["key1"].Value))

		// 添加新 key
		seg, _ := NewSegment("new-key", &types.Variant{}, ImmortalTTL)
		snapshots["new-key"] = &Snapshot{
			mvcc:    1,
			Segment: seg,
		}

		// 保存 active fd 引用
		activeFd = txns.store.active

		return txns.Save(snapshots)
	})

	err = txns.Commit()
	if err == nil {
		// Commit 成功，关闭 active region 然后再次尝试 rollback
		_ = activeFd.Close()
		err = txns.Rollback()
		if err == nil {
			t.Fatal("expected rollback to fail with closed active region")
		}
		t.Logf("Got expected rollback error: %v", err)
	} else {
		t.Logf("Commit failed as expected: %v", err)
	}
}

func TestRollbackRemoveError(t *testing.T) {
	_ = os.RemoveAll(conf.Settings.Path)

	fss, err := OpenFS(&Options{
		FSPerm:    conf.FSPerm,
		Path:      conf.Settings.Path,
		Threshold: conf.Settings.Region.Threshold,
	})
	if err != nil {
		t.Fatal(err)
	}

	testPutSegment(fss)

	txns, err := fss.NewTransaction()
	if err != nil {
		t.Fatal(err)
	}

	txns.AtomicBatch(func(txns *TxnState) error {
		keys := []string{"key1"}
		_, err := txns.Begin(keys)
		if err != nil {
			return err
		}
		return errors.New("simulated failure")
	})

	err = txns.Commit()
	if err == nil {
		t.Fatal("expected commit to fail")
	}

	// 删除 .txn 文件导致 Rollback 中的 os.Remove 失败
	_ = txns.fd.Close()
	_ = os.Remove(txns.path)

	err = txns.Rollback()
	if err == nil {
		t.Fatal("expected rollback to fail when removing txn file")
	}
	t.Logf("Got expected error: %v", err)
}

func TestRollbackTxnsError(t *testing.T) {
	_ = os.RemoveAll(conf.Settings.Path)

	fss, err := OpenFS(&Options{
		FSPerm:    conf.FSPerm,
		Path:      conf.Settings.Path,
		Threshold: conf.Settings.Region.Threshold,
	})
	if err != nil {
		t.Fatal(err)
	}

	testPutSegment(fss)

	txns, err := fss.NewTransaction()
	if err != nil {
		t.Fatal(err)
	}

	txns.AtomicBatch(func(txns *TxnState) error {
		keys := []string{"key1"}
		snapshots, err := txns.Begin(keys)
		if err != nil {
			return err
		}

		snapshots["key1"].Value = []byte("modified value")
		snapshots["key1"].ValueSize = int32(len(snapshots["key1"].Value))

		// 添加新 key 触发 rollback 逻辑
		seg, _ := NewSegment("new-key", &types.Variant{}, ImmortalTTL)
		snapshots["new-key"] = &Snapshot{
			mvcc:    1,
			Segment: seg,
		}

		return txns.Save(snapshots)
	})

	// 先提交成功
	err = txns.Commit()
	if err != nil {
		t.Fatal(err)
	}

	// 关闭 active region，然后调用 Rollback 触发 RollbackTxns 错误
	_ = fss.active.Close()
	txns.rollback = true // 强制执行 RollbackTxns

	err = txns.Rollback()
	if err == nil {
		t.Fatal("expected rollback to fail with closed active region")
	}
	t.Logf("Got expected rollback error: %v", err)
}
