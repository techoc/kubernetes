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

package cbor

import (
	"fmt"
	goruntime "runtime"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/cbor"
)

var (
	scheme      = runtime.NewScheme()
	serializers = []cbor.Serializer{
		cbor.NewSerializer(scheme, scheme),
		cbor.NewSerializer(scheme, scheme, cbor.Strict(true)),
	}
)

// FuzzDecodeAllocations 是一个 go-fuzz target，当输入导致解码时分配了
// 不合理数量的字节时会 panic。
func FuzzDecodeAllocations(data []byte) (result int) {
	const (
		MaxInputBytes     = 128
		MaxAllocatedBytes = 16 * 1024
	)

	if len(data) > MaxInputBytes {
		// 较长的输入会反序列化为更大的对象，
		// 可能需要更多分配。这里专注于寻找分配了不合理
		// 字节数的短输入，以定位病态场景。
		return -1
	}

	decode := func(serializer cbor.Serializer, data []byte) int {
		var u unstructured.Unstructured
		o, gvk, err := serializer.Decode(data, &schema.GroupVersionKind{}, &u)
		if err != nil {
			if o != nil {
				panic("returned non-nil error and non-nil runtime.Object")
			}

			return 0
		}

		if o == nil || gvk == nil {
			panic("returned nil error and nil runtime.Object or nil schema.GroupVersionKind")
		}

		return 1
	}

	for _, serializer := range serializers {
		// 第一轮用于预热所有延迟初始化的内容。
		// 例如进程中首次打印日志可能带来数十 kB
		// 量级的分配。
		decode(serializer, data)

		var nBytesAllocated uint64
		for trial := 1; trial <= 10; trial++ {
			func() {
				defer goruntime.GOMAXPROCS(goruntime.GOMAXPROCS(1))
				var mem goruntime.MemStats
				goruntime.ReadMemStats(&mem)

				result |= decode(serializer, data)

				nBytesAllocated = mem.TotalAlloc
				goruntime.ReadMemStats(&mem)
				nBytesAllocated = mem.TotalAlloc - nBytesAllocated

			}()

			// 确切的分配字节数可能因并发执行的
			// goroutine 中的分配或运行时的实现细节
			// 而变化。仅当输入持续超过分配阈值时才 panic，
			// 以降低误报率。
			if nBytesAllocated <= MaxAllocatedBytes {
				break
			}
		}

		if nBytesAllocated > MaxAllocatedBytes {
			panic(fmt.Sprintf("%d bytes allocated to decode input of length %d exceeds maximum of %d", nBytesAllocated, len(data), MaxAllocatedBytes))
		}
	}

	return result
}
