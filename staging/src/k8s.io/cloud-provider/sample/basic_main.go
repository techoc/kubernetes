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

// This file should be written by each cloud provider.
// For an minimal working example, please refer to k8s.io/cloud-provider/sample/basic_main.go
// For more details, please refer to k8s.io/kubernetes/cmd/cloud-controller-manager/main.go
// 中文说明：每个外部云提供商只需要提供这样一个 main：注册 Factory、构造选项、
// 启动命令。控制器本身由 cloud-provider 模块提供，无需重复实现。

package main

import (
	"os"

	"k8s.io/apimachinery/pkg/util/wait"
	cloudprovider "k8s.io/cloud-provider"
	"k8s.io/cloud-provider/app"
	"k8s.io/cloud-provider/app/config"
	"k8s.io/cloud-provider/names"
	"k8s.io/cloud-provider/options"
	"k8s.io/component-base/cli"
	cliflag "k8s.io/component-base/cli/flag"
	_ "k8s.io/component-base/logs/json/register"          // register optional JSON log format
	_ "k8s.io/component-base/metrics/prometheus/clientgo" // load all the prometheus client-go plugins
	_ "k8s.io/component-base/metrics/prometheus/version"  // for version metric registration
	"k8s.io/klog/v2"
	// For existing cloud providers, the option to import legacy providers is still available.
	// e.g. _"k8s.io/legacy-cloud-providers/<provider>"
)

// main 是外部 cloud-controller-manager 的最小启动模板。
func main() {
	ccmOptions, err := options.NewCloudControllerManagerOptions()
	if err != nil {
		klog.Fatalf("unable to initialize command options: %v", err)
	}

	fss := cliflag.NamedFlagSets{}
	// wait.NeverStop 表示进程生命周期由信号处理控制，命令不会因 stopCh 关闭而退出。
	command := app.NewCloudControllerManagerCommand(ccmOptions, cloudInitializer, controllerInitializers(), names.CCMControllerAliases(), fss, wait.NeverStop)
	code := cli.Run(command)
	os.Exit(code)
}

// If custom ClientNames are used, as below, then the controller will not use
// the API server bootstrapped RBAC, and instead will require it to be installed
// separately.
// 自定义 ClientName 会改变派生客户端的身份，因此集群必须预先安装对应的 RBAC，
// 否则控制器会因权限不足而无法工作。
func controllerInitializers() map[string]app.ControllerInitFuncConstructor {
	controllerInitializers := app.DefaultInitFuncConstructors
	if constructor, ok := controllerInitializers[names.CloudNodeController]; ok {
		constructor.InitContext.ClientName = "mycloud-external-cloud-node-controller"
		controllerInitializers[names.CloudNodeController] = constructor
	}
	if constructor, ok := controllerInitializers[names.CloudNodeLifecycleController]; ok {
		constructor.InitContext.ClientName = "mycloud-external-cloud-node-lifecycle-controller"
		controllerInitializers[names.CloudNodeLifecycleController] = constructor
	}
	if constructor, ok := controllerInitializers[names.ServiceLBController]; ok {
		constructor.InitContext.ClientName = "mycloud-external-service-controller"
		controllerInitializers[names.ServiceLBController] = constructor
	}
	if constructor, ok := controllerInitializers[names.NodeRouteController]; ok {
		constructor.InitContext.ClientName = "mycloud-external-route-controller"
		controllerInitializers[names.NodeRouteController] = constructor
	}
	return controllerInitializers
}

// cloudInitializer 根据组件配置实例化云提供商，并强制校验集群 ID。
func cloudInitializer(config *config.CompletedConfig) cloudprovider.Interface {
	cloudConfig := config.ComponentConfig.KubeCloudShared.CloudProvider

	// initialize cloud provider with the cloud provider name and config file provided
	// 初始化失败属于不可恢复的配置错误，直接 Fatal。
	cloud, err := cloudprovider.InitCloudProvider(cloudConfig.Name, cloudConfig.CloudConfigFile)
	if err != nil {
		klog.Fatalf("Cloud provider could not be initialized: %v", err)
	}
	if cloud == nil {
		klog.Fatalf("Cloud provider is nil")
	}

	// ClusterID 用于在多集群环境中区分资源；缺失时默认拒绝启动，
	// 只有显式允许 untagged cloud 才降级为警告。
	if !cloud.HasClusterID() {
		if config.ComponentConfig.KubeCloudShared.AllowUntaggedCloud {
			klog.Warning("detected a cluster without a ClusterID.  A ClusterID will be required in the future.  Please tag your cluster to avoid any future issues")
		} else {
			klog.Fatalf("no ClusterID found.  A ClusterID is required for the cloud provider to function properly.  This check can be bypassed by setting the allow-untagged-cloud option")
		}
	}
	return cloud
}
