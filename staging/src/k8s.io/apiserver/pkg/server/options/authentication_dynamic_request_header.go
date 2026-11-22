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

package options

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apiserver/pkg/authentication/request/headerrequest"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	"k8s.io/client-go/kubernetes"
)

// 编译期断言：该控制器同时充当动态证书控制器、CA 提供者与请求头配置提供者。
var _ dynamiccertificates.ControllerRunner = &DynamicRequestHeaderController{}
var _ dynamiccertificates.CAContentProvider = &DynamicRequestHeaderController{}

var _ headerrequest.RequestHeaderAuthRequestProvider = &DynamicRequestHeaderController{}

// DynamicRequestHeaderController combines DynamicCAFromConfigMapController and RequestHeaderAuthRequestController
// into one controller for dynamically filling RequestHeaderConfig struct
// 两者共用同一个 ConfigMap，由一个控制器统一运行可避免重复的 informer 与生命周期管理。
type DynamicRequestHeaderController struct {
	*dynamiccertificates.ConfigMapCAController
	*headerrequest.RequestHeaderAuthRequestController
}

// newDynamicRequestHeaderController creates a new controller that implements DynamicRequestHeaderController
func newDynamicRequestHeaderController(client kubernetes.Interface) (*DynamicRequestHeaderController, error) {
	requestHeaderCAController, err := dynamiccertificates.NewDynamicCAFromConfigMapController(
		"client-ca",
		authenticationConfigMapNamespace,
		authenticationConfigMapName,
		"requestheader-client-ca-file",
		client)
	if err != nil {
		return nil, fmt.Errorf("unable to create DynamicCAFromConfigMap controller: %v", err)
	}

	requestHeaderAuthRequestController := headerrequest.NewRequestHeaderAuthRequestController(
		authenticationConfigMapName,
		authenticationConfigMapNamespace,
		client,
		"requestheader-username-headers",
		"requestheader-uid-headers",
		"requestheader-group-headers",
		"requestheader-extra-headers-prefix",
		"requestheader-allowed-names",
	)
	return &DynamicRequestHeaderController{
		ConfigMapCAController:              requestHeaderCAController,
		RequestHeaderAuthRequestController: requestHeaderAuthRequestController,
	}, nil
}

// RunOnce 同步执行两个子控制器的首次加载，聚合错误便于启动时一次性暴露所有问题。
func (c *DynamicRequestHeaderController) RunOnce(ctx context.Context) error {
	errs := []error{}
	errs = append(errs, c.ConfigMapCAController.RunOnce(ctx))
	errs = append(errs, c.RequestHeaderAuthRequestController.RunOnce(ctx))
	return errors.NewAggregate(errs)
}

// Run 并发运行两个子控制器，任一上下文取消即整体退出。
func (c *DynamicRequestHeaderController) Run(ctx context.Context, workers int) {
	go c.ConfigMapCAController.Run(ctx, workers)
	go c.RequestHeaderAuthRequestController.Run(ctx, workers)
	<-ctx.Done()
}
