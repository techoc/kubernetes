/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package etcd3

import (
	"fmt"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// event 是 etcd watch 事件的内部归一化表示；把创建/删除/进度通知等不同
// 形态统一成结构体，便于 cache 层按同一套逻辑处理。
type event struct {
	key              string
	value            []byte
	prevValue        []byte
	rev              int64
	isDeleted        bool
	isCreated        bool
	isProgressNotify bool
	// isInitialEventsEndBookmark helps us keep track
	// of whether we have sent an annotated bookmark event.
	//
	// when this variable is set to true,
	// a special annotation will be added
	// to the bookmark event.
	//
	// note that we decided to extend the event
	// struct field to eliminate contention
	// between startWatching and processEvent
	// 用事件字段而非共享状态传递“初始事件已同步完”，避免两个 goroutine
	// 为同一个标记加锁。
	isInitialEventsEndBookmark bool
	// isInitialEvent indicates the event was generated from an initial state sync.
	isInitialEvent bool
	recordTime     time.Time
}

// parseKV converts a KeyValue retrieved from an initial sync() listing to a synthetic isCreated event.
// 初始 list 没有真实的 watch 事件，合成 created 事件让下游与增量路径共用逻辑。
func parseKV(kv *mvccpb.KeyValue) *event {
	return &event{
		key:            string(kv.Key),
		value:          kv.Value,
		prevValue:      nil,
		rev:            kv.ModRevision,
		isDeleted:      false,
		isCreated:      true,
		isInitialEvent: true,
	}
}

// parseEvent 把 etcd 原始事件转换为内部 event。删除事件必须带 PrevKv，
// 否则无法判断对象此前是否存在/内容是否匹配（常见于 revision 已被压缩）。
func parseEvent(e *clientv3.Event) (*event, error) {
	if !e.IsCreate() && e.PrevKv == nil {
		// If the previous value is nil, error. One example of how this is possible is if the previous value has been compacted already.
		return nil, fmt.Errorf("etcd event received with PrevKv=nil (key=%q, modRevision=%d, type=%s)", string(e.Kv.Key), e.Kv.ModRevision, e.Type.String())

	}
	ret := &event{
		key:        string(e.Kv.Key),
		value:      e.Kv.Value,
		rev:        e.Kv.ModRevision,
		isDeleted:  e.Type == clientv3.EventTypeDelete,
		isCreated:  e.IsCreate(),
		recordTime: time.Now(),
	}
	if e.PrevKv != nil {
		ret.prevValue = e.PrevKv.Value
	}
	return ret, nil
}

// progressNotifyEvent 生成 etcd 进度通知事件：不携带数据，只推进 revision，
// 让 watcher 能确认“已看到该 revision 之前的所有事件”。
func progressNotifyEvent(rev int64) *event {
	return &event{
		rev:              rev,
		isProgressNotify: true,
		recordTime:       time.Now(),
	}
}
