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
	"testing"
	"time"

	expirationcache "k8s.io/client-go/tools/cache"
	"k8s.io/utils/clock"
	testingclock "k8s.io/utils/clock/testing"
)

// testObject 是测试用的键值对象。
type testObject struct {
	key string
	val string
}

// NewFakeObjectCache 创建供单元测试使用的 ObjectCache。
func NewFakeObjectCache(f func() (interface{}, error), ttl time.Duration, clock clock.Clock) *ObjectCache {
	ttlPolicy := &expirationcache.TTLPolicy{TTL: ttl, Clock: clock}
	deleteChan := make(chan string, 1)
	return &ObjectCache{
		updater: f,
		cache:   expirationcache.NewFakeExpirationStore(stringKeyFunc, deleteChan, ttlPolicy, clock),
	}
}

// TestAddAndGet 验证缓存写入后可以直接读取，且不会调用 updater。
func TestAddAndGet(t *testing.T) {
	testObj := testObject{
		key: "foo",
		val: "bar",
	}
	// updater 不应被调用，命中缓存时直接返回旧值。
	objectCache := NewFakeObjectCache(func() (interface{}, error) {
		return nil, fmt.Errorf("Unexpected Error: updater should never be called in this test")
	}, 1*time.Hour, testingclock.NewFakeClock(time.Now()))

	err := objectCache.Add(testObj.key, testObj.val)
	if err != nil {
		t.Errorf("Unable to add obj %#v by key: %s", testObj, testObj.key)
	}
	value, err := objectCache.Get(testObj.key)
	if err != nil {
		t.Errorf("Unable to get obj %#v by key: %s", testObj, testObj.key)
	}
	if value.(string) != testObj.val {
		t.Errorf("Expected to get cached value: %#v, but got: %s", testObj.val, value.(string))
	}

}

// TestExpirationBasic 验证缓存过期后会通过 updater 获取新值。
func TestExpirationBasic(t *testing.T) {
	unexpectedVal := "bar"
	expectedVal := "bar2"

	testObj := testObject{
		key: "foo",
		val: unexpectedVal,
	}

	fakeClock := testingclock.NewFakeClock(time.Now())

	objectCache := NewFakeObjectCache(func() (interface{}, error) {
		return expectedVal, nil
	}, 1*time.Second, fakeClock)

	err := objectCache.Add(testObj.key, testObj.val)
	if err != nil {
		t.Errorf("Unable to add obj %#v by key: %s", testObj, testObj.key)
	}

	// 推进假时钟 2 秒，使缓存过期。
	fakeClock.Sleep(2 * time.Second)

	// 过期后应调用 updater 获取新值。
	value, err := objectCache.Get(testObj.key)
	if err != nil {
		t.Errorf("Unable to get obj %#v by key: %s", testObj, testObj.key)
	}
	if value.(string) != expectedVal {
		t.Errorf("Expected to get cached value: %#v, but got: %s", expectedVal, value.(string))
	}
}
