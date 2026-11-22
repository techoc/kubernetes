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

package clientcmd

// transformingStringValue implements pflag.Value to store string values,
// allowing transforming them while being set
//
// transformingStringValue 实现 pflag.Value：Set 时先经 transformer 规范化
// （例如把 server URL 补全、校验），再写入 target。String 直接读取 target，
// 因此命令行 help 中显示的始终是转换后的值。
type transformingStringValue struct {
	target      *string
	transformer func(string) (string, error)
}

// newTransformingStringValue 立即把默认值写入 target，使未显式设置 flag 时
// 也能得到与 Set 相同的规范化结果；transformer 仅在 Set 时调用。
func newTransformingStringValue(val string, target *string, transformer func(string) (string, error)) *transformingStringValue {
	*target = val
	return &transformingStringValue{
		target:      target,
		transformer: transformer,
	}
}

// Set 实现 pflag.Value；转换失败时不修改 target，保持上一次的有效值。
func (t *transformingStringValue) Set(val string) error {
	val, err := t.transformer(val)
	if err != nil {
		return err
	}
	*t.target = val
	return nil
}

// Type 返回 pflag 中展示的类型名。
func (t *transformingStringValue) Type() string {
	return "string"
}

// String 返回当前目标值；pflag 在打印默认值时也会调用它。
func (t *transformingStringValue) String() string {
	return string(*t.target)
}
