/*
Copyright 2020 The Kubernetes Authors.

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

package profile

import (
	"context"
	"fmt"
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"k8s.io/klog/v2/ktesting"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
)

// fakeRegistry 提供最小插件注册表，避免测试依赖真实插件实现。
var fakeRegistry = frameworkruntime.Registry{
	"QueueSort": newFakePlugin("QueueSort"),
	"Bind1":     newFakePlugin("Bind1"),
	"Bind2":     newFakePlugin("Bind2"),
	"Another":   newFakePlugin("Another"),
}

// TestNewMap 校验 NewMap 对合法配置建表，以及对非法配置给出预期错误。
func TestNewMap(t *testing.T) {
	// 每个用例只改动一个约束，便于定位失败原因。
	cases := []struct {
		name    string                        // 用例名，同时作为子测试名
		cfgs    []config.KubeSchedulerProfile // 待构建的 profile 配置
		wantErr string                        // 期望错误的关键子串；为空表示应成功
	}{
		{
			// 合法配置：多个 profile 共用 QueueSort，但 Bind 插件不同。
			name: "valid",
			cfgs: []config.KubeSchedulerProfile{
				{
					SchedulerName: "profile-1",
					Plugins: &config.Plugins{
						QueueSort: config.PluginSet{
							Enabled: []config.Plugin{
								{Name: "QueueSort"},
							},
						},
						Bind: config.PluginSet{
							Enabled: []config.Plugin{
								{Name: "Bind1"},
							},
						},
					},
				},
				{
					SchedulerName: "profile-2",
					Plugins: &config.Plugins{
						QueueSort: config.PluginSet{
							Enabled: []config.Plugin{
								{Name: "QueueSort"},
							},
						},
						Bind: config.PluginSet{
							Enabled: []config.Plugin{
								{Name: "Bind2"},
							},
						},
					},
					PluginConfig: []config.PluginConfig{
						{
							Name: "Bind2",
							Args: &runtime.Unknown{Raw: []byte("{}")},
						},
					},
				},
			},
		},
		{
			// QueueSort 插件必须全局唯一，配置不同插件应被拒绝。
			name: "different queue sort",
			cfgs: []config.KubeSchedulerProfile{
				{
					SchedulerName: "profile-1",
					Plugins: &config.Plugins{
						QueueSort: config.PluginSet{
							Enabled: []config.Plugin{
								{Name: "QueueSort"},
							},
						},
						Bind: config.PluginSet{
							Enabled: []config.Plugin{
								{Name: "Bind1"},
							},
						},
					},
				},
				{
					SchedulerName: "profile-2",
					Plugins: &config.Plugins{
						QueueSort: config.PluginSet{
							Enabled: []config.Plugin{
								{Name: "Another"},
							},
						},
						Bind: config.PluginSet{
							Enabled: []config.Plugin{
								{Name: "Bind2"},
							},
						},
					},
				},
			},
			wantErr: "different queue sort plugins",
		},
		{
			// 同名插件参数也必须一致，否则队列排序语义会不一致。
			name: "different queue sort args",
			cfgs: []config.KubeSchedulerProfile{
				{
					SchedulerName: "profile-1",
					Plugins: &config.Plugins{
						QueueSort: config.PluginSet{
							Enabled: []config.Plugin{
								{Name: "QueueSort"},
							},
						},
						Bind: config.PluginSet{
							Enabled: []config.Plugin{
								{Name: "Bind1"},
							},
						},
					},
					PluginConfig: []config.PluginConfig{
						{
							Name: "QueueSort",
							Args: &runtime.Unknown{Raw: []byte("{}")},
						},
					},
				},
				{
					SchedulerName: "profile-2",
					Plugins: &config.Plugins{
						QueueSort: config.PluginSet{
							Enabled: []config.Plugin{
								{Name: "QueueSort"},
							},
						},
						Bind: config.PluginSet{
							Enabled: []config.Plugin{
								{Name: "Bind2"},
							},
						},
					},
				},
			},
			wantErr: "different queue sort plugin args",
		},
		{
			// 调度器名称即 profile 标识，重复会覆盖配置，必须拒绝。
			name: "duplicate scheduler name",
			cfgs: []config.KubeSchedulerProfile{
				{
					SchedulerName: "profile-1",
					Plugins: &config.Plugins{
						QueueSort: config.PluginSet{
							Enabled: []config.Plugin{
								{Name: "QueueSort"},
							},
						},
						Bind: config.PluginSet{
							Enabled: []config.Plugin{
								{Name: "Bind1"},
							},
						},
					},
				},
				{
					SchedulerName: "profile-1",
					Plugins: &config.Plugins{
						QueueSort: config.PluginSet{
							Enabled: []config.Plugin{
								{Name: "QueueSort"},
							},
						},
						Bind: config.PluginSet{
							Enabled: []config.Plugin{
								{Name: "Bind2"},
							},
						},
					},
				},
			},
			wantErr: "duplicate profile",
		},
		{
			// 缺少名称无法将 Pod 调度请求匹配到 profile。
			name: "scheduler name is needed",
			cfgs: []config.KubeSchedulerProfile{
				{
					Plugins: &config.Plugins{
						QueueSort: config.PluginSet{
							Enabled: []config.Plugin{
								{Name: "QueueSort"},
							},
						},
						Bind: config.PluginSet{
							Enabled: []config.Plugin{
								{Name: "Bind1"},
							},
						},
					},
				},
			},
			wantErr: "scheduler name is needed",
		},
		{
			// profile 至少要有插件配置，空配置没有意义。
			name: "plugins required for profile",
			cfgs: []config.KubeSchedulerProfile{
				{
					SchedulerName: "profile-1",
				},
			},
			wantErr: "plugins required for profile",
		},
		{
			// 未配置 Bind 插件时框架初始化应失败。
			name: "invalid framework configuration",
			cfgs: []config.KubeSchedulerProfile{
				{
					SchedulerName: "invalid-profile",
					Plugins: &config.Plugins{
						QueueSort: config.PluginSet{
							Enabled: []config.Plugin{
								{Name: "QueueSort"},
							},
						},
					},
				},
			},
			wantErr: "at least one bind plugin is needed",
		},
	}
	// 每个子测试独立建表，避免用例间共享 framework 状态。
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			// 显式 cancel，保证测试结束后 framework 的后台资源可退出。
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			m, err := NewMap(ctx, tc.cfgs, fakeRegistry, nilRecorderFactory)
			defer func() {
				if m != nil {
					// 关闭 map 中所有已注册的 framework，避免 goroutine 泄漏。
					err := m.Close()
					if err != nil {
						t.Errorf("error closing map: %v", err)
					}
				}
			}()

			if err := checkErr(err, tc.wantErr); err != nil {
				t.Fatal(err)
			}
			// 预期失败的用例无需再检查 map 内容。
			if len(tc.wantErr) != 0 {
				return
			}
			// 合法配置下，每个 profile 都应生成一条映射。
			if len(m) != len(tc.cfgs) {
				t.Errorf("got %d profiles, want %d", len(m), len(tc.cfgs))
			}
		})
	}
}

// fakePlugin 是满足 QueueSort/Bind 扩展点的最小插件，仅用于触发框架初始化。
type fakePlugin struct {
	name string // 插件名，由构造工厂注入
}

// Name 返回插件名，供框架按配置查找插件。
func (p *fakePlugin) Name() string {
	return p.name
}

// Less 满足 QueueSort 扩展点；测试不关心排序，恒为 false。
func (p *fakePlugin) Less(fwk.QueuedEntityInfo, fwk.QueuedEntityInfo) bool {
	return false
}

// Bind 满足 Bind 扩展点；测试不执行绑定，恒返回 nil。
func (p *fakePlugin) Bind(context.Context, fwk.CycleState, *v1.Pod, string) *fwk.Status {
	return nil
}

// newFakePlugin 返回插件工厂，忽略运行时参数，直接构造带指定名称的 fakePlugin。
func newFakePlugin(name string) func(ctx context.Context, object runtime.Object, handle fwk.Handle) (fwk.Plugin, error) {
	return func(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
		return &fakePlugin{name: name}, nil
	}
}

// nilRecorderFactory 提供空的事件记录器，测试不校验事件上报。
func nilRecorderFactory(_ string) events.EventRecorderLogger {
	return nil
}

// checkErr 按 wantErr 校验错误：为空时期望无错，否则要求错误包含关键字。
func checkErr(err error, wantErr string) error {
	if len(wantErr) == 0 {
		return err
	}
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		return fmt.Errorf("got error %q, want %q", err, wantErr)
	}
	return nil
}
