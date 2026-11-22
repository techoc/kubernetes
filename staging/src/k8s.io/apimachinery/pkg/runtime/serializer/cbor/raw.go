/*
Copyright 2024 The Kubernetes Authors.

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

// Package cbor 实现 Kubernetes 的 CBOR 序列化支持。
//
// 核心难点是 RawExtension 与 FieldsV1 这类"原始字节"字段：它们在 JSON 与 CBOR
// 之间必须转码。本文件通过反射遍历对象树，为每种类型预编译一个转码函数，
// 避免每次编码都做完整的反射扫描。
package cbor

import (
	"fmt"
	"reflect"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// sharedTranscoders 缓存所有已见类型的转码器；内部有读写锁保护，可并发使用。
var sharedTranscoders transcoders

// rawTypeTranscodeFuncs 是"需要特殊处理"的原始类型及其转码逻辑。
//
// 这些字段在内存中保存的是另一种编码的字节（通常是 JSON），若直接按 CBOR
// 编码会得到错误结果，因此必须先转换为 JSON 再交给 CBOR 编码器。
var rawTypeTranscodeFuncs = map[reflect.Type]func(reflect.Value) error{
	reflect.TypeFor[runtime.RawExtension](): func(rv reflect.Value) error {
		// 只有可取地址的值才能原地修改；副本无法写回调用方，跳过即可。
		if !rv.CanAddr() {
			return nil
		}
		re := rv.Addr().Interface().(*runtime.RawExtension)
		if re.Raw == nil {
			// When Raw is nil it encodes to null. Don't change nil Raw values during
			// transcoding, they would have unmarshalled from JSON as nil too.
			return nil
		}
		j, err := re.MarshalJSON()
		if err != nil {
			return fmt.Errorf("failed to transcode RawExtension to JSON: %w", err)
		}
		re.Raw = j
		return nil
	},
	reflect.TypeFor[metav1.FieldsV1](): func(rv reflect.Value) error {
		if !rv.CanAddr() {
			return nil
		}
		fields := rv.Addr().Interface().(*metav1.FieldsV1)
		if fields.GetRawReader().Size() == 0 {
			// When Raw is nil it encodes to null. Don't change nil Raw values during
			// transcoding, they would have unmarshalled from JSON as nil too.
			return nil
		}
		j, err := fields.MarshalJSON()
		if err != nil {
			return fmt.Errorf("failed to transcode FieldsV1 to JSON: %w", err)
		}
		fields.SetRawBytes(j)
		return nil
	},
}

// transcodeRawTypes 对 v 执行原地转码；v 必须是可取地址的指针链。
func transcodeRawTypes(v interface{}) error {
	if v == nil {
		return nil
	}

	rv := reflect.ValueOf(v)
	return sharedTranscoders.getTranscoder(rv.Type()).fn(rv)
}

// transcoder 封装某个类型的转码函数。
type transcoder struct {
	fn func(rv reflect.Value) error
}

// noop 用于不包含任何原始类型的类型；用指针比较可快速判断"无事可做"。
var noop = transcoder{
	fn: func(reflect.Value) error {
		return nil
	},
}

// transcoders 是类型到转码器的并发缓存。
type transcoders struct {
	lock sync.RWMutex
	m    map[reflect.Type]**transcoder
}

// getTranscoder 返回 rt 对应的转码器，首次访问时惰性构建。
//
// 读路径无锁（RWMutex 读锁），构建路径加写锁并做双重检查。
func (ts *transcoders) getTranscoder(rt reflect.Type) transcoder {
	ts.lock.RLock()
	tpp, ok := ts.m[rt]
	ts.lock.RUnlock()
	if ok {
		return **tpp
	}

	ts.lock.Lock()
	defer ts.lock.Unlock()
	tp := ts.getTranscoderLocked(rt)
	return *tp
}

// getTranscoderLocked 在持有写锁时构建转码器。
func (ts *transcoders) getTranscoderLocked(rt reflect.Type) *transcoder {
	if tpp, ok := ts.m[rt]; ok {
		// A transcoder for this type was cached while waiting to acquire the lock.
		// 等待锁期间可能已被其他 goroutine 填充，直接复用。
		return *tpp
	}

	// Cache the transcoder now, before populating fn, so that circular references between types
	// don't overflow the call stack.
	// 先占位再递归构建子类型，是处理自引用类型（如链表）的关键。
	t := new(transcoder)
	if ts.m == nil {
		ts.m = make(map[reflect.Type]**transcoder)
	}
	ts.m[rt] = &t

	// 原始类型优先：RawExtension/FieldsV1 需要专门的转码函数。
	for rawType, fn := range rawTypeTranscodeFuncs {
		if rt == rawType {
			t = &transcoder{fn: fn}
			return t
		}
	}

	// 其余类型按 kind 递归组合子转码器；若所有子类型都是 noop，则整体也是 noop。
	switch rt.Kind() {
	case reflect.Array:
		te := ts.getTranscoderLocked(rt.Elem())
		rtlen := rt.Len()
		if rtlen == 0 || te == &noop {
			t = &noop
			break
		}
		t.fn = func(rv reflect.Value) error {
			for i := 0; i < rtlen; i++ {
				if err := te.fn(rv.Index(i)); err != nil {
					return err
				}
			}
			return nil
		}
	case reflect.Interface:
		// Any interface value might have a dynamic type involving RawExtension. It needs to
		// be checked.
		// 接口的静态类型无法确定，只能在运行时按动态类型查表。
		t.fn = func(rv reflect.Value) error {
			if rv.IsNil() {
				return nil
			}
			rv = rv.Elem()
			// The interface element's type is dynamic so its transcoder can't be
			// determined statically.
			return ts.getTranscoder(rv.Type()).fn(rv)
		}
	case reflect.Map:
		rtk := rt.Key()
		tk := ts.getTranscoderLocked(rtk)
		rte := rt.Elem()
		te := ts.getTranscoderLocked(rte)
		if tk == &noop && te == &noop {
			t = &noop
			break
		}
		t.fn = func(rv reflect.Value) error {
			// MapRange 避免为每个条目分配；SetIterKey/SetIterValue 复用临时值。
			iter := rv.MapRange()
			rvk := reflect.New(rtk).Elem()
			rve := reflect.New(rte).Elem()
			for iter.Next() {
				rvk.SetIterKey(iter)
				if err := tk.fn(rvk); err != nil {
					return err
				}
				rve.SetIterValue(iter)
				if err := te.fn(rve); err != nil {
					return err
				}
			}
			return nil
		}
	case reflect.Pointer:
		te := ts.getTranscoderLocked(rt.Elem())
		if te == &noop {
			t = &noop
			break
		}
		t.fn = func(rv reflect.Value) error {
			if rv.IsNil() {
				return nil
			}
			return te.fn(rv.Elem())
		}
	case reflect.Slice:
		te := ts.getTranscoderLocked(rt.Elem())
		if te == &noop {
			t = &noop
			break
		}
		t.fn = func(rv reflect.Value) error {
			for i := 0; i < rv.Len(); i++ {
				if err := te.fn(rv.Index(i)); err != nil {
					return err
				}
			}
			return nil
		}
	case reflect.Struct:
		type fieldTranscoder struct {
			Index      int
			Transcoder *transcoder
		}
		// 只保留需要处理的字段，避免每次遍历整个结构体的所有字段。
		var fieldTranscoders []fieldTranscoder
		for i := 0; i < rt.NumField(); i++ {
			f := rt.Field(i)
			tf := ts.getTranscoderLocked(f.Type)
			if tf == &noop {
				continue
			}
			fieldTranscoders = append(fieldTranscoders, fieldTranscoder{Index: i, Transcoder: tf})
		}
		if len(fieldTranscoders) == 0 {
			t = &noop
			break
		}
		t.fn = func(rv reflect.Value) error {
			for _, ft := range fieldTranscoders {
				if err := ft.Transcoder.fn(rv.Field(ft.Index)); err != nil {
					return err
				}
			}
			return nil
		}
	default:
		// 基本类型（string、int、bool 等）不含原始编码字段。
		t = &noop
	}

	return t
}
