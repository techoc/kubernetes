/*
Copyright The Kubernetes Authors.

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

// 这是一个位于多个包场景中的消费方测试包：Consumer 引用 types 包中的
// 共享类型 T2，用于验证跨包校验生成与类型归属。

package consumer

import "k8s.io/code-generator/cmd/validation-gen/output_tests/multiple_packages/types"

// Consumer 内嵌一个整型 TypeMeta（保证被选为目标类型），
// 并通过 JSON 字段引用外部包类型。
type Consumer struct {
	TypeMeta int

	Shared types.T2 `json:"shared"`
}
