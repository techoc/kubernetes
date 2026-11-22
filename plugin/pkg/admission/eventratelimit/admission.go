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

package eventratelimit

import (
	"context"
	"io"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/client-go/util/flowcontrol"
	api "k8s.io/kubernetes/pkg/apis/core"
	eventratelimitapi "k8s.io/kubernetes/plugin/pkg/admission/eventratelimit/apis/eventratelimit"
	"k8s.io/kubernetes/plugin/pkg/admission/eventratelimit/apis/eventratelimit/validation"
	"k8s.io/utils/clock"
)

// PluginName 是准入插件的名称。
const PluginName = "EventRateLimit"

// Register 注册插件。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName,
		func(config io.Reader) (admission.Interface, error) {
			// 加载提供的配置（如果有）。
			configuration, err := LoadConfiguration(config)
			if err != nil {
				return nil, err
			}
			// 校验配置（如果有），尽早暴露错误配置。
			if configuration != nil {
				if errs := validation.ValidateConfiguration(configuration); len(errs) != 0 {
					return nil, errs.ToAggregate()
				}
			}
			return newEventRateLimit(configuration, clock.RealClock{})
		})
}

// Plugin 实现可执行事件限流的准入控制器。
type Plugin struct {
	*admission.Handler
	// limitEnforcers 是限流执行器的集合，每种启用的限流类型对应一个。
	// 限流类型共 4 种，因此切片长度最多为 4；构造完成后只读。
	limitEnforcers []*limitEnforcer
}

var _ admission.ValidationInterface = &Plugin{}

// newEventRateLimit 构造按配置执行事件限流的准入控制器。
func newEventRateLimit(config *eventratelimitapi.Configuration, clock flowcontrol.Clock) (*Plugin, error) {
	limitEnforcers := make([]*limitEnforcer, 0, len(config.Limits))
	for _, limitConfig := range config.Limits {
		enforcer, err := newLimitEnforcer(limitConfig, clock)
		if err != nil {
			return nil, err
		}
		limitEnforcers = append(limitEnforcers, enforcer)
	}

	eventRateLimitAdmission := &Plugin{
		Handler:        admission.NewHandler(admission.Create, admission.Update),
		limitEnforcers: limitEnforcers,
	}

	return eventRateLimitAdmission, nil
}

// Validate 在执行事件限流的同时作出准入决策。
func (a *Plugin) Validate(ctx context.Context, attr admission.Attributes, o admission.ObjectInterfaces) (err error) {
	// 只处理 Event Kind，其他资源直接放行。
	if attr.GetKind().GroupKind() != api.Kind("Event") {
		return nil
	}

	// dry-run 请求不会写入 etcd，因此不应消耗限流配额。
	if attr.IsDryRun() {
		return nil
	}

	var errors []error
	// 逐个执行限流检查；任一执行器拒绝即记录错误。
	for _, enforcer := range a.limitEnforcers {
		if err := enforcer.accept(attr); err != nil {
			errors = append(errors, err)
		}
	}

	if aggregatedErr := utilerrors.NewAggregate(errors); aggregatedErr != nil {
		return apierrors.NewTooManyRequestsError(aggregatedErr.Error())
	}

	return nil
}
