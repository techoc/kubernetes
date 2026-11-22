/*
Copyright 2018 The Kubernetes Authors.

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

import (
	goflag "flag"

	"github.com/spf13/pflag"
)

// NoOp 同时实现 goflag.Value 与 pflag.Value，但 Set 不做任何事。
// 用于兼容只接受 flag 的旧接口，或需要占位却不希望参数产生副作用的场景。
type NoOp struct{}

var _ goflag.Value = NoOp{}
var _ pflag.Value = NoOp{}

// String 返回空串：该 flag 从不携带可展示的值。
func (NoOp) String() string {
	return ""
}

// Set 丢弃传入值并始终成功，因此任何参数都不会导致解析失败。
func (NoOp) Set(val string) error {
	return nil
}

// Type 返回 flag 的类型名。
func (NoOp) Type() string {
	return "NoOp"
}
