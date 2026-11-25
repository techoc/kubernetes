/*
Copyright 2025 The Kubernetes Authors.

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

package architecture

import (
	"fmt"
	"slices"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// 它是线程安全的。
// event 描述一个 add/update/delete 事件。
type eventRecorder struct {
	mutex  sync.Mutex
	events []event
}

// event 描述一个 add/update/delete 事件。
// 可根据设置了哪些对象来区分它们。
// Delete 事件可能包含 tombstone 而不是实际的
// 被删除对象。
type event struct {
	oldObj, newObj any
	isInitialList  bool
}

func (e event) Type() string {
	switch {
	case e.oldObj == nil:
		return "add"
	case e.newObj == nil:
		return "delete"
	case e.oldObj != nil && e.newObj != nil:
		return "update"
	default:
		return "null"
	}
}

// ID 返回事件中所描述对象的 "[namespace/]name, uid"。
// 如果由于某种原因，旧事件和新事件的 ID
// 不同（这是错误！），
// 则用分号分隔返回两个字符串。
// 如果无法访问元数据，则返回错误字符串。
//
// 这旨在与 gomega.HaveEach 及检查期望 ID 的 matcher
// 一起使用。
func (e event) ID() string {
	oldID := id(e.oldObj)
	newID := id(e.newObj)
	if oldID == newID {
		return newID
	}
	if oldID != "" && newID != "" {
		return oldID + "; " + newID
	}
	if newID != "" {
		return newID
	}
	return oldID
}

func id(obj any) string {
	if obj == nil {
		return ""
	}
	if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tomb.Obj
	}
	metaData, err := meta.Accessor(obj)
	if err != nil {
		return err.Error()
	}
	return fmt.Sprintf("%s, %s", klog.KObj(metaData).String(), metaData.GetUID())
}

var _ cache.ResourceEventHandler = &eventRecorder{}

func (er *eventRecorder) OnAdd(obj any, isInitialList bool) {
	er.mutex.Lock()
	defer er.mutex.Unlock()

	er.events = append(er.events, event{
		newObj:        obj,
		isInitialList: isInitialList,
	})
}

func (er *eventRecorder) OnUpdate(oldObj, newObj any) {
	er.mutex.Lock()
	defer er.mutex.Unlock()

	er.events = append(er.events, event{
		oldObj: oldObj,
		newObj: newObj,
	})
}

func (er *eventRecorder) OnDelete(obj any) {
	er.mutex.Lock()
	defer er.mutex.Unlock()

	er.events = append(er.events, event{
		oldObj: obj,
	})
}

// list 返回当前事件列表的浅拷贝。
func (er *eventRecorder) list() eventList {
	er.mutex.Lock()
	defer er.mutex.Unlock()

	return eventList{Events: slices.Clone(er.events)}
}

// reset 清空当前事件列表。
// 只应在空闲期间调用。
func (er *eventRecorder) reset() eventList {
	er.mutex.Lock()
	defer er.mutex.Unlock()

	events := eventList{Events: er.events}
	er.events = nil
	return events
}

// eventList 为事件 slice 添加漂亮打印。
type eventList struct {
	Events []event
}

// Types 返回一个逗号分隔的列表，包含每个事件的类型。
// 为简单起见，最后一项也以逗号结尾。
//
// 可在如下的 Gomega 断言中使用：
//
// \tgomega.Expect(events).To(gomega.HaveField("Types()", gomega.MatchRegexp("^(update,)*$"))'
// poll 是轮询 pod、node 和 claim 的频率。
func (el eventList) Types() string {
	var buffer strings.Builder

	for _, e := range el.Events {
		buffer.WriteString(e.Type())
		buffer.WriteRune(',')
	}

	return buffer.String()
}
