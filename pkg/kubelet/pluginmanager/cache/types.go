/*
Copyright 2019 The Kubernetes Authors.

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

package cache

import (
	"context"
	"time"
)

// PluginHandler 是 pluginwatcher API 使用方为消费插件而需要实现的接口。
// PluginHandler 遵循下面的简单状态机：
//
//	                       +--------------------------------------+
//	                       |            ReRegistration            |
//	                       | Socket created with same plugin name |
//	                       |                                      |
//	                       |                                      |
//	  Socket Created       v                                      +        Socket Deleted
//	+------------------> Validate +---------------------------> Register +------------------> DeRegister
//	                       +                                      +                              +
//	                       |                                      |                              |
//	                       | Error                                | Error                        |
//	                       |                                      |                              |
//	                       v                                      v                              v
//	                      Out                                    Out                            Out
//
// pluginwatcher 对每个插件名严格、串行地执行该状态机。
// 例如：正在注册插件 foo 时，Register("foo") 返回前不会收到 foo 的 DeRegister，
// 也不会收到 Validate("foo", "Different endpoint", ...) 调用。
//
// ReRegistration：使用相同插件名创建 socket，通常用于插件升级。
// 例如插件 foo 先注册在 foo.com/foo-1.9.7，之后又注册在 foo.com/foo-1.9.9。
//
// DeRegistration：发生 ReRegistration 后，只有新 socket 被删除才会触发 DeRegister。
type PluginHandler interface {
	// ValidatePlugin 在潜在插件提供的信息有误（如版本不支持）时返回错误。
	ValidatePlugin(ctx context.Context, pluginName string, endpoint string, versions []string) error
	// RegisterPlugin 由插件使用方调用以完成注册；
	// 此处遇到的错误仍可通知给插件。
	RegisterPlugin(ctx context.Context, pluginName, endpoint string, versions []string, pluginClientTimeout *time.Duration) error
	// DeRegisterPlugin 在 pluginwatcher 观察到 socket 被删除后调用。
	DeRegisterPlugin(ctx context.Context, pluginName, endpoint string)
}
