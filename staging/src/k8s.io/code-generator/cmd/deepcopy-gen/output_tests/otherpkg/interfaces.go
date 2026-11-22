/*
Copyright 2017 The Kubernetes Authors.

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

package otherpkg

// Object 模拟 runtime.Object 的最小接口，用于跨包接口 deepcopy 测试。
type Object interface {
	DeepCopyObject() Object
}

// List 模拟列表对象的复制接口，用于验证跨包生成的调用目标。
type List interface {
	DeepCopyList() List
}
