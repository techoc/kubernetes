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

package flag

// OmitEmpty 是 flag 值用来报告自身是否为“空”的接口。
// 若某 flag 实现 OmitEmpty 且 Empty() 返回 true，调用方可认为该 flag
// 可以不出现在命令行中——例如在生成部署清单时略去默认值。
type OmitEmpty interface {
	Empty() bool
}
