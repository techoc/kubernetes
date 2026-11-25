/*
Copyright 2015 The Kubernetes Authors.

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

package meta

import (
	"errors"
	"fmt"
	"reflect"
	"sync"

	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
)

var (
	// isListCache 缓存已被判定为列表的类型，供 IsListType 复用，避免对同一类型重复做反射探测。
	// 它是进程级全局状态，因此用读写锁保护；容量上限 1024 用于限制内存占用，超出后只查询不再写入。
	isListCache = struct {
		lock   sync.RWMutex
		byType map[reflect.Type]bool
	}{
		byType: make(map[reflect.Type]bool, 1024),
	}
)

// IsListType 判断给定对象是否为列表类型，即是否带有名为 Items 的切片字段。
// 非列表类型返回 false；对 Unstructured 对象直接委托其自身的 IsList 判定。
func IsListType(obj runtime.Object) bool {
	switch t := obj.(type) {
	case runtime.Unstructured:
		return t.IsList()
	}
	// 先查缓存：命中则直接复用之前的判定结果，省去一次反射探测。
	t := reflect.TypeOf(obj)

	isListCache.lock.RLock()
	ok, exists := isListCache.byType[t]
	isListCache.lock.RUnlock()

	if !exists {
		_, err := getItemsPtr(obj)
		ok = err == nil

		// 只缓存前 1024 个类型，避免缓存无限膨胀。
		isListCache.lock.Lock()
		if len(isListCache.byType) < 1024 {
			isListCache.byType[t] = ok
		}
		isListCache.lock.Unlock()
	}

	return ok
}

var (
	// errExpectFieldItems 表示对象没有名为 Items 的字段，因而不足以为列表。
	errExpectFieldItems = errors.New("no Items field in this object")
	// errExpectSliceItems 表示 Items 字段存在但其类型不是对象切片。
	errExpectSliceItems = errors.New("Items field must be a slice of objects")
)

// GetItemsPtr 返回列表对象 Items 成员的指针。
// 若 list 没有 Items 成员，则它并非真正的列表类型，会返回错误。
// 该函数要么返回切片指针，要么返回错误，二者不会同时出现。
func GetItemsPtr(list runtime.Object) (interface{}, error) {
	obj, err := getItemsPtr(list)
	if err != nil {
		return nil, fmt.Errorf("%T is not a list: %v", list, err)
	}
	return obj, nil
}

// getItemsPtr 返回列表对象 Items 成员的指针或错误，是 GetItemsPtr 的底层实现。
func getItemsPtr(list runtime.Object) (interface{}, error) {
	// EnforcePtr 要求传入的是指针，且其指向可寻址的值；列表对象必须为指针类型。
	v, err := conversion.EnforcePtr(list)
	if err != nil {
		return nil, err
	}

	items := v.FieldByName("Items")
	if !items.IsValid() {
		return nil, errExpectFieldItems
	}
	switch items.Kind() {
	case reflect.Interface, reflect.Pointer:
		// 接口或指针字段需先解出其动态元素的类型，再确认该元素是切片。
		target := reflect.TypeOf(items.Interface()).Elem()
		if target.Kind() != reflect.Slice {
			return nil, errExpectSliceItems
		}
		return items.Interface(), nil
	case reflect.Slice:
		// 切片字段本身可寻址，直接返回其地址，调用方即可就地读写。
		return items.Addr().Interface(), nil
	default:
		return nil, errExpectSliceItems
	}
}

// EachListItem 对列表中的每个 runtime.Object 依次调用 fn，任一错误都会立即终止循环。
//
// 若传给 fn 的条目被保留的时长各不相同，且希望避免因其中任一条目被引用而长期持有整个
// obj 的条目切片，请改用 EachListItemWithAlloc。
func EachListItem(obj runtime.Object, fn func(runtime.Object) error) error {
	return eachListItem(obj, fn, false)
}

// EachListItemWithAlloc 行为与 EachListItem 一致，但避免持有 obj 中条目切片的引用。
// 其做法是对 obj 中非指针类型的条目做浅拷贝。
//
// 若传给 fn 的条目未被保留、或保留时长相同，为节省内存应改用 EachListItem。
func EachListItemWithAlloc(obj runtime.Object, fn func(runtime.Object) error) error {
	return eachListItem(obj, fn, true)
}

// allocNew 表示当 Object.Items 的元素为结构体（非指针）时，是否需要为其做浅拷贝。
func eachListItem(obj runtime.Object, fn func(runtime.Object) error, allocNew bool) error {
	if unstructured, ok := obj.(runtime.Unstructured); ok {
		// Unstructured 对象由自身实现条目遍历，无需走反射路径。
		if allocNew {
			return unstructured.EachListItemWithAlloc(fn)
		}
		return unstructured.EachListItem(fn)
	}
	// TODO: 改为接口调用？
	itemsPtr, err := GetItemsPtr(obj)
	if err != nil {
		return err
	}
	items, err := conversion.EnforcePtr(itemsPtr)
	if err != nil {
		return err
	}
	len := items.Len()
	if len == 0 {
		return nil
	}
	takeAddr := false
	// 元素为非指针、非接口的结构体时，反射取出的值不可直接作为对象使用，需要取地址。
	if elemType := items.Type().Elem(); elemType.Kind() != reflect.Pointer && elemType.Kind() != reflect.Interface {
		if !items.Index(0).CanAddr() {
			return fmt.Errorf("unable to take address of items in %T for EachListItem", obj)
		}
		takeAddr = true
	}

	for i := 0; i < len; i++ {
		raw := items.Index(i)
		if takeAddr {
			if allocNew {
				// 浅拷贝，避免持有原始列表条目的引用。
				itemCopy := reflect.New(raw.Type())
				// 将原条目赋值到新分配的结构体并做类型断言。
				itemCopy.Elem().Set(raw)
				// reflect.New 保证 itemCopy 一定是指针。
				raw = itemCopy
			} else {
				raw = raw.Addr()
			}
		}
		// raw 必须是指针或接口；分配一个指针的代价很低，因此这里直接分配。
		switch item := raw.Interface().(type) {
		case *runtime.RawExtension:
			if err := fn(item.Object); err != nil {
				return err
			}
		case runtime.Object:
			if err := fn(item); err != nil {
				return err
			}
		default:
			obj, ok := item.(runtime.Object)
			if !ok {
				return fmt.Errorf("%v: item[%v]: Expected object, got %#v(%s)", obj, i, raw.Interface(), raw.Kind())
			}
			if err := fn(obj); err != nil {
				return err
			}
		}
	}
	return nil
}

// ExtractList 将 obj 的 Items 元素作为 runtime.Object 数组返回。
// 若 obj 不是列表类型（没有 Items 成员）则返回错误。
//
// 若返回列表中的条目被保留的时长各不相同，且希望避免因任一条目被引用而长期持有 obj 的
// 条目切片，请改用 ExtractListWithAlloc。
func ExtractList(obj runtime.Object) ([]runtime.Object, error) {
	return extractList(obj, false)
}

// ExtractListWithAlloc 行为与 ExtractList 一致，但避免持有 obj 中条目切片的引用。
// 其做法是对 obj 中非指针类型的条目做浅拷贝。
//
// 若返回的条目未被保留、或保留时长相同，为节省内存应改用 ExtractList。
func ExtractListWithAlloc(obj runtime.Object) ([]runtime.Object, error) {
	return extractList(obj, true)
}

// allocNew 表示当 Object.Items 的元素为结构体（非指针）时，是否需要为其做浅拷贝。
func extractList(obj runtime.Object, allocNew bool) ([]runtime.Object, error) {
	itemsPtr, err := GetItemsPtr(obj)
	if err != nil {
		return nil, err
	}
	items, err := conversion.EnforcePtr(itemsPtr)
	if err != nil {
		return nil, err
	}
	if items.IsNil() {
		return nil, nil
	}
	list := make([]runtime.Object, items.Len())
	if len(list) == 0 {
		return list, nil
	}
	// 预先判定元素类型，避免在内层循环里反复做类型比较。
	elemType := items.Type().Elem()
	isRawExtension := elemType == rawExtensionObjectType
	implementsObject := elemType.Implements(objectType)
	for i := range list {
		raw := items.Index(i)
		switch {
		case isRawExtension:
			item := raw.Interface().(runtime.RawExtension)
			switch {
			case item.Object != nil:
				list[i] = item.Object
			case item.Raw != nil:
				// 仅有原始字节时包装为 Unknown，交由调用方按需解码。
				list[i] = &runtime.Unknown{Raw: item.Raw}
			default:
				list[i] = nil
			}
		case implementsObject:
			list[i] = raw.Interface().(runtime.Object)
		case allocNew:
			// 浅拷贝，避免持有原始列表条目的引用。
			itemCopy := reflect.New(raw.Type())
			// 将原条目赋值到新分配的结构体并做类型断言。
			itemCopy.Elem().Set(raw)
			var ok bool
			// reflect.New 保证 itemCopy 一定是指针。
			if list[i], ok = itemCopy.Interface().(runtime.Object); !ok {
				return nil, fmt.Errorf("%v: item[%v]: Expected object, got %#v(%s)", obj, i, raw.Interface(), raw.Kind())
			}
		default:
			// 元素可寻址，取地址后即可满足 runtime.Object 接口。
			var found bool
			if list[i], found = raw.Addr().Interface().(runtime.Object); !found {
				return nil, fmt.Errorf("%v: item[%v]: Expected object, got %#v(%s)", obj, i, raw.Interface(), raw.Kind())
			}
		}
	}
	return list, nil
}

var (
	// objectSliceType 是 []runtime.Object 的反射类型，用于 SetList 快速复制。
	objectSliceType = reflect.TypeOf([]runtime.Object{})
	// objectType 是 runtime.Object 的接口类型，用于判断元素是否已实现该接口。
	objectType = reflect.TypeOf((*runtime.Object)(nil)).Elem()
	// rawExtensionObjectType 用于识别 RawExtension 元素并做特殊解包。
	rawExtensionObjectType = reflect.TypeOf(runtime.RawExtension{})
)

// LenList 返回列表的长度；若它不是列表则返回 0。
func LenList(list runtime.Object) int {
	itemsPtr, err := GetItemsPtr(list)
	if err != nil {
		return 0
	}
	items, err := conversion.EnforcePtr(itemsPtr)
	if err != nil {
		return 0
	}
	return items.Len()
}

// SetList 将给定列表对象的 Items 成员设置为 objects 中的元素。
// 若 list 不是列表类型（没有 Items 成员），或任一对象类型不匹配，则返回错误。
func SetList(list runtime.Object, objects []runtime.Object) error {
	itemsPtr, err := GetItemsPtr(list)
	if err != nil {
		return err
	}
	items, err := conversion.EnforcePtr(itemsPtr)
	if err != nil {
		return err
	}
	if items.Type() == objectSliceType {
		// 已是 []runtime.Object，可直接整体赋值，省去逐元素转换。
		items.Set(reflect.ValueOf(objects))
		return nil
	}
	slice := reflect.MakeSlice(items.Type(), len(objects), len(objects))
	for i := range objects {
		dest := slice.Index(i)
		if dest.Type() == rawExtensionObjectType {
			// RawExtension 的对象实际存放在其 Object 字段中。
			dest = dest.FieldByName("Object")
		}

		// 先看能否直接赋值，这是最常见、最廉价的路径。
		if reflect.TypeOf(objects[i]).AssignableTo(dest.Type()) {
			dest.Set(reflect.ValueOf(objects[i]))
			continue
		}

		// 否则取出源值的底层值，再尝试赋值或转换。
		src, err := conversion.EnforcePtr(objects[i])
		if err != nil {
			return err
		}
		if src.Type().AssignableTo(dest.Type()) {
			dest.Set(src)
		} else if src.Type().ConvertibleTo(dest.Type()) {
			dest.Set(src.Convert(dest.Type()))
		} else {
			return fmt.Errorf("item[%d]: can't assign or convert %v into %v", i, src.Type(), dest.Type())
		}
	}
	items.Set(slice)
	return nil
}
