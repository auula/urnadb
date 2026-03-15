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

		snapshots[0].Value = []byte("test transaction 1.")
		snapshots[0].ValueSize = int32(len(snapshots[0].Value))

		// 模拟事务执行过程中可能会有一些耗时的操作了，方便观察数据目录有没有 .txn 文件产生。
		time.Sleep(3 * time.Second)

		snapshots[1].Value = []byte("test transaction 2.")
		snapshots[1].ValueSize = int32(len(snapshots[1].Value))

		seg, _ := NewSegment("key3", &types.Variant{}, ImmortalTTL)

		snapshots = append(snapshots, &Snapshot{
			mvcc:    1,
			Segment: seg,
		})

		return txns.Save(snapshots)
	})

	if err := txns.Commit(); err != nil {
		if !errors.Is(err, ErrEmptyBeginSnapshot) {
			t.Fatal(err)
		}
		err := txns.Rollback()
		if !errors.Is(err, ErrEmptyBeginSnapshot) {
			t.Fatal(err)
		}
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

			snapshots[0].Value = []byte("A-test transaction 1.")
			snapshots[0].ValueSize = int32(len(snapshots[0].Value))

			time.Sleep(2 * time.Second)

			snapshots[1].Value = []byte("A-test transaction 2.")
			snapshots[1].ValueSize = int32(len(snapshots[1].Value))

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

			snapshots[0].Value = []byte("B-test transaction 1.")
			snapshots[0].ValueSize = int32(len(snapshots[0].Value))

			snapshots[1].Value = []byte("B-test transaction 2.")
			snapshots[1].ValueSize = int32(len(snapshots[1].Value))

			return txns.Save(snapshots)
		})

		if err := txns.Commit(); err != nil {
			t.Log(err)
		}
	}()

	wg.Wait()
}
