/*
Copyright 2025 The Kubernetes Authors.

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

package app

import "context"

type options struct {
	// cancelMainContext 用于在后台发生错误时取消上层
	// context。
	// 如果设置了它，HandleError 会调用它。
	cancelMainContext context.CancelCauseFunc
}

// TestOption 实现了函数式选项模式，
// 专供测试代码使用。
type TestOption func(o *options) error

// CancelMainContext 为插件设置 context 取消函数。
// 当后台发生错误时，HandleError 会调用该函数。
func CancelMainContext(cancel context.CancelCauseFunc) TestOption {
	return func(o *options) error {
		o.cancelMainContext = cancel
		return nil
	}
}
