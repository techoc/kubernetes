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

package hash

import (
	"fmt"
	"hash"

	"k8s.io/utils/dump"
)

// DeepHashObject 使用 spew 库将指定对象写入 hasher。
// spew 会解引用指针、打印嵌套对象的实际值，
// 从而保证指针地址变化不会影响哈希结果。
func DeepHashObject(hasher hash.Hash, objectToWrite interface{}) {
	hasher.Reset()
	fmt.Fprintf(hasher, "%v", dump.ForHash(objectToWrite))
}
