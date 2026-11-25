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

// FakeCustomStore 允许为 Store 的每个操作指定自定义函数。
// 任何未设置的回调都会退化为安全的默认行为（读操作返回空值、写操作返回 nil），
// 方便测试只 stub 部分操作。
type FakeCustomStore struct {
	AddFunc                          func(obj interface{}) error
	UpdateFunc                       func(obj interface{}) error
	DeleteFunc                       func(obj interface{}) error
	ListFunc                         func() []interface{}
	ListKeysFunc                     func() []string
	GetFunc                          func(obj interface{}) (item interface{}, exists bool, err error)
	GetByKeyFunc                     func(key string) (item interface{}, exists bool, err error)
	ReplaceFunc                      func(list []interface{}, resourceVersion string) error
	ResyncFunc                       func() error
	BookmarkFunc                     func(rv string)
	LastStoreSyncResourceVersionFunc func() string
}

var _ Store = &FakeCustomStore{}

// Add 在定义了自定义 Add 函数时调用它。
func (f *FakeCustomStore) Add(obj interface{}) error {
	if f.AddFunc != nil {
		return f.AddFunc(obj)
	}
	return nil
}

// Update 在定义了自定义 Update 函数时调用它。
func (f *FakeCustomStore) Update(obj interface{}) error {
	if f.UpdateFunc != nil {
		return f.UpdateFunc(obj)
	}
	return nil
}

// Delete 在定义了自定义 Delete 函数时调用它。
func (f *FakeCustomStore) Delete(obj interface{}) error {
	if f.DeleteFunc != nil {
		return f.DeleteFunc(obj)
	}
	return nil
}

// List 在定义了自定义 List 函数时调用它。
func (f *FakeCustomStore) List() []interface{} {
	if f.ListFunc != nil {
		return f.ListFunc()
	}
	return nil
}

// ListKeys 在定义了自定义 ListKeys 函数时调用它。
func (f *FakeCustomStore) ListKeys() []string {
	if f.ListKeysFunc != nil {
		return f.ListKeysFunc()
	}
	return nil
}

// Get 在定义了自定义 Get 函数时调用它。
func (f *FakeCustomStore) Get(obj interface{}) (item interface{}, exists bool, err error) {
	if f.GetFunc != nil {
		return f.GetFunc(obj)
	}
	return nil, false, nil
}

// GetByKey 在定义了自定义 GetByKey 函数时调用它。
func (f *FakeCustomStore) GetByKey(key string) (item interface{}, exists bool, err error) {
	if f.GetByKeyFunc != nil {
		return f.GetByKeyFunc(key)
	}
	return nil, false, nil
}

// Replace 在定义了自定义 Replace 函数时调用它。
func (f *FakeCustomStore) Replace(list []interface{}, resourceVersion string) error {
	if f.ReplaceFunc != nil {
		return f.ReplaceFunc(list, resourceVersion)
	}
	return nil
}

// Resync 在定义了自定义 Resync 函数时调用它。
func (f *FakeCustomStore) Resync() error {
	if f.ResyncFunc != nil {
		return f.ResyncFunc()
	}
	return nil
}

// Bookmark 在定义了自定义 Bookmark 函数时调用它。
func (f *FakeCustomStore) Bookmark(rv string) {
	if f.BookmarkFunc != nil {
		f.BookmarkFunc(rv)
	}
}

// LastStoreSyncResourceVersion 在定义了自定义 LastStoreSyncResourceVersion 函数时调用它。
func (f *FakeCustomStore) LastStoreSyncResourceVersion() string {
	if f.LastStoreSyncResourceVersionFunc != nil {
		return f.LastStoreSyncResourceVersionFunc()
	}
	return ""
}
