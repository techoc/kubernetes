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

package cache

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"

	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/diff"
)

// mutationDetectionEnabled 由环境变量开关控制。默认关闭：检测需要保存对象副本，
// 在生产环境会造成额外的内存占用。
var mutationDetectionEnabled = false

func init() {
	// 解析失败（如值为 "yes"）时保持关闭，避免误开启高开销检测。
	mutationDetectionEnabled, _ = strconv.ParseBool(os.Getenv("KUBE_CACHE_MUTATION_DETECTOR"))
}

// MutationDetector is able to monitor objects for mutation within a limited window of time
type MutationDetector interface {
	// AddObject adds the given object to the set being monitored for a while from now
	AddObject(obj interface{})

	// Run starts the monitoring and does not return until the monitoring is stopped.
	Run(stopCh <-chan struct{})
}

// NewCacheMutationDetector creates a new instance for the defaultCacheMutationDetector.
// 未启用检测时返回无副作用的 dummy 实现，调用方无需分支判断。
func NewCacheMutationDetector(name string) MutationDetector {
	if !mutationDetectionEnabled {
		return dummyMutationDetector{}
	}
	//nolint:logcheck // This code shouldn't be used in production.
	klog.Warningln("Mutation detector is enabled, this will result in memory leakage.")
	return &defaultCacheMutationDetector{name: name, period: 1 * time.Second, retainDuration: 2 * time.Minute}
}

// dummyMutationDetector 是检测关闭时的空实现，所有方法都是 no-op。
type dummyMutationDetector struct{}

// Run 立即返回。
func (dummyMutationDetector) Run(stopCh <-chan struct{}) {
}

// AddObject 忽略对象。
func (dummyMutationDetector) AddObject(obj interface{}) {
}

// defaultCacheMutationDetector gives a way to detect if a cached object has been mutated
// It has a list of cached objects and their copies.  I haven't thought of a way
// to see WHO is mutating it, just that it's getting mutated.
//
// 轮转策略：每 retainDuration 把当前批次转为"保留批次"，只比较最近两个窗口的对象，
// 从而在持续运行时不无限增长内存。
type defaultCacheMutationDetector struct {
	name   string
	period time.Duration

	// compareLock ensures only a single call to CompareObjects runs at a time
	compareObjectsLock sync.Mutex

	// addLock guards addedObjs between AddObject and CompareObjects
	addedObjsLock sync.Mutex
	addedObjs     []cacheObj

	// cachedObjs 是当前窗口内已纳入比较的对象；只在 compareObjectsLock 下修改。
	cachedObjs []cacheObj

	// retainedCachedObjs 是上一窗口的对象，用于覆盖跨窗口才发生的篡改。
	retainDuration     time.Duration
	lastRotated        time.Time
	retainedCachedObjs []cacheObj

	// failureFunc is injectable for unit testing.  If you don't have it, the process will panic.
	// This panic is intentional, since turning on this detection indicates you want a strong
	// failure signal.  This failure is effectively a p0 bug and you can't trust process results
	// after a mutation anyway.
	failureFunc func(message string)
}

// cacheObj holds the actual object and a copy
// cached 是 informer 缓存中的原对象，copied 是 DeepCopy，用于发现原地修改。
type cacheObj struct {
	cached interface{}
	copied interface{}
}

// Run 周期性比较对象与其副本，直到 stopCh 关闭。
func (d *defaultCacheMutationDetector) Run(stopCh <-chan struct{}) {
	// we DON'T want protection from panics.  If we're running this code, we want to die
	for {
		// 首次进入时只初始化时间戳；之后每个 retainDuration 轮转一次窗口。
		if d.lastRotated.IsZero() {
			d.lastRotated = time.Now()
		} else if time.Since(d.lastRotated) > d.retainDuration {
			d.retainedCachedObjs = d.cachedObjs
			d.cachedObjs = nil
			d.lastRotated = time.Now()
		}

		d.CompareObjects()

		select {
		case <-stopCh:
			return
		case <-time.After(d.period):
		}
	}
}

// AddObject makes a deep copy of the object for later comparison.  It only works on runtime.Object
// but that covers the vast majority of our cached objects
func (d *defaultCacheMutationDetector) AddObject(obj interface{}) {
	// tombstone 包装的不是缓存中的活对象，且可能被反复包装，跳过多余开销。
	if _, ok := obj.(DeletedFinalStateUnknown); ok {
		return
	}
	// 只有能 DeepCopy 的 runtime.Object 才能做前后比较；其他类型静默忽略。
	if obj, ok := obj.(runtime.Object); ok {
		copiedObj := obj.DeepCopyObject()

		d.addedObjsLock.Lock()
		defer d.addedObjsLock.Unlock()
		d.addedObjs = append(d.addedObjs, cacheObj{cached: obj, copied: copiedObj})
	}
}

// CompareObjects 把新增对象并入当前窗口后逐个比较，发现篡改即上报。
func (d *defaultCacheMutationDetector) CompareObjects() {
	d.compareObjectsLock.Lock()
	defer d.compareObjectsLock.Unlock()

	// move addedObjs into cachedObjs under lock
	// this keeps the critical section small to avoid blocking AddObject while we compare cachedObjs
	// 先交换再比较：比较可能很慢，不能长时间持有 addedObjsLock。
	d.addedObjsLock.Lock()
	d.cachedObjs = append(d.cachedObjs, d.addedObjs...)
	d.addedObjs = nil
	d.addedObjsLock.Unlock()

	altered := false
	for i, obj := range d.cachedObjs {
		if !reflect.DeepEqual(obj.cached, obj.copied) {
			fmt.Printf("CACHE %s[%d] ALTERED!\n%v\n", d.name, i, diff.ObjectGoPrintSideBySide(obj.cached, obj.copied))
			altered = true
		}
	}
	for i, obj := range d.retainedCachedObjs {
		if !reflect.DeepEqual(obj.cached, obj.copied) {
			fmt.Printf("CACHE %s[%d] ALTERED!\n%v\n", d.name, i, diff.ObjectGoPrintSideBySide(obj.cached, obj.copied))
			altered = true
		}
	}

	if altered {
		msg := fmt.Sprintf("cache %s modified", d.name)
		// 测试可注入 failureFunc 以避免真的 panic；生产环境则直接崩溃，
		// 因为篡改发生后进程状态已不可信。
		if d.failureFunc != nil {
			d.failureFunc(msg)
			return
		}
		panic(msg)
	}
}
