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

// Package profile 定义调度器 Profile：把一个调度器名字与一组框架插件绑定起来。
package profile

import (
	"context"
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/diff"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
)

// RecorderFactory 为指定调度器名字创建一个事件记录器。
type RecorderFactory func(string) events.EventRecorderLogger

// newProfile 根据给定配置构建单个 Profile。
func newProfile(ctx context.Context, cfg config.KubeSchedulerProfile, r frameworkruntime.Registry, recorderFact RecorderFactory,
	opts ...frameworkruntime.Option) (framework.Framework, error) {
	recorder := recorderFact(cfg.SchedulerName)
	opts = append(opts, frameworkruntime.WithEventRecorder(recorder))
	return frameworkruntime.NewFramework(ctx, r, &cfg, opts...)
}

// Map 以调度器名字为键保存各个框架实例。
type Map map[string]framework.Framework

// NewMap 根据配置为各调度器构建框架，并按名字索引。
func NewMap(ctx context.Context, cfgs []config.KubeSchedulerProfile, r frameworkruntime.Registry, recorderFact RecorderFactory,
	opts ...frameworkruntime.Option) (Map, error) {
	m := make(Map)
	v := cfgValidator{m: m}

	for _, cfg := range cfgs {
		p, err := newProfile(ctx, cfg, r, recorderFact, opts...)
		if err != nil {
			return nil, fmt.Errorf("creating profile for scheduler name %s: %v", cfg.SchedulerName, err)
		}
		// 所有 Profile 共用同一个队列排序插件和配置，否则无法在多个 Profile 间混排 Pod。
		if err := v.validate(cfg, p); err != nil {
			return nil, err
		}
		m[cfg.SchedulerName] = p
	}
	return m, nil
}

// HandlesSchedulerName 判断是否存在处理该调度器名字的 Profile。
func (m Map) HandlesSchedulerName(name string) bool {
	_, ok := m[name]
	return ok
}

// Close 关闭 Map 中所有已注册的框架。
func (m Map) Close() error {
	var errs []error
	for name, f := range m {
		// 逐个关闭并汇总错误，避免一个框架失败就跳过其余框架的清理。
		err := f.Close()
		if err != nil {
			errs = append(errs, fmt.Errorf("framework %s failed to close: %w", name, err))
		}
	}
	return errors.Join(errs...)
}

// NewRecorderFactory 基于事件广播器创建一个 RecorderFactory。
func NewRecorderFactory(b events.EventBroadcaster) RecorderFactory {
	return func(name string) events.EventRecorderLogger {
		return b.NewRecorder(scheme.Scheme, name)
	}
}

// cfgValidator 校验新 Profile 与已有 Profile 的一致性。
type cfgValidator struct {
	m             Map
	queueSort     string
	queueSortArgs runtime.Object
}

// validate 校验单个 Profile 的合法性，并与先前 Profile 比较队列排序配置。
func (v *cfgValidator) validate(cfg config.KubeSchedulerProfile, f framework.Framework) error {
	if len(f.ProfileName()) == 0 {
		return errors.New("scheduler name is needed")
	}
	if cfg.Plugins == nil {
		return fmt.Errorf("plugins required for profile with scheduler name %q", f.ProfileName())
	}
	if v.m[f.ProfileName()] != nil {
		return fmt.Errorf("duplicate profile with scheduler name %q", f.ProfileName())
	}

	// 队列排序插件只能有一个，取第一个即可。
	queueSort := f.ListPlugins().QueueSort.Enabled[0].Name
	var queueSortArgs runtime.Object
	for _, plCfg := range cfg.PluginConfig {
		if plCfg.Name == queueSort {
			queueSortArgs = plCfg.Args
			break
		}
	}
	if len(v.queueSort) == 0 {
		v.queueSort = queueSort
		v.queueSortArgs = queueSortArgs
		return nil
	}
	if v.queueSort != queueSort {
		return fmt.Errorf("different queue sort plugins for profile %q: %q, first: %q", cfg.SchedulerName, queueSort, v.queueSort)
	}
	if diff.Diff(v.queueSortArgs, queueSortArgs) != "" {
		return fmt.Errorf("different queue sort plugin args for profile %q", cfg.SchedulerName)
	}
	return nil
}
