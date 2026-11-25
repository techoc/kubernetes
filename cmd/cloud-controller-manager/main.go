/*
Copyright 2016 The Kubernetes Authors.

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

// 外部 controller manager 负责运行依赖云提供商的 controller 循环。
// 它通过 API 监听资源上的新事件。

// 此文件应由各云提供商自行编写。
// 最小可运行示例请参考 k8s.io/cloud-provider/sample/basic_main.go
// 更多细节请参考 k8s.io/kubernetes/cmd/cloud-controller-manager/main.go
// 当前文件演示其他云提供商应如何利用 CCM，其中的参数是虚构的。请根据自身情况修改。

package main

import (
	"os"

	"k8s.io/apimachinery/pkg/util/wait"
	cloudprovider "k8s.io/cloud-provider"
	"k8s.io/cloud-provider/app"
	cloudcontrollerconfig "k8s.io/cloud-provider/app/config"
	"k8s.io/cloud-provider/names"
	"k8s.io/cloud-provider/options"
	"k8s.io/component-base/cli"
	cliflag "k8s.io/component-base/cli/flag"
	_ "k8s.io/component-base/metrics/prometheus/clientgo" // 加载所有 prometheus client-go 插件
	_ "k8s.io/component-base/metrics/prometheus/version"  // 用于注册版本指标
	"k8s.io/klog/v2"
	kcmnames "k8s.io/kubernetes/cmd/kube-controller-manager/names"
	// 对于已有的云提供商，仍可选择导入 legacy provider。
	// e.g. _"k8s.io/legacy-cloud-providers/<provider>"
)

func main() {
	ccmOptions, err := options.NewCloudControllerManagerOptions()
	if err != nil {
		klog.Fatalf("unable to initialize command options: %v", err)
	}

	controllerInitializers := app.DefaultInitFuncConstructors
	controllerAliases := names.CCMControllerAliases()
	// 下面是移除不需要的 controller 的示例。
	// 例如，移除当前云提供商不需要的 cloud-node-lifecycle controller。
	//delete(controllerInitializers, "cloud-node-lifecycle")

	// 下面是添加一个供云提供商用于生成 nodeIPAMConfig 的 controller（NodeIpamController）的示例。
	// 这是示例代码。
	// 如果不需要额外的 controller，请忽略。

	nodeIpamController := nodeIPAMController{}
	nodeIpamController.nodeIPAMControllerOptions.NodeIPAMControllerConfiguration = &nodeIpamController.nodeIPAMControllerConfiguration
	fss := cliflag.NamedFlagSets{}
	nodeIpamController.nodeIPAMControllerOptions.AddFlags(fss.FlagSet(kcmnames.NodeIpamController))

	controllerInitializers[kcmnames.NodeIpamController] = app.ControllerInitFuncConstructor{
		// "node-controller" 是所有节点 controller 共用的身份标识，包括 node、node lifecycle 和 node ipam。
		// 更多背景信息见 https://github.com/kubernetes/kubernetes/pull/72764#issuecomment-453300990 。
		InitContext: app.ControllerInitContext{
			ClientName: "node-controller",
		},
		Constructor: nodeIpamController.StartNodeIpamControllerWrapper,
	}
	controllerAliases["nodeipam"] = kcmnames.NodeIpamController

	command := app.NewCloudControllerManagerCommand(ccmOptions, cloudInitializer, controllerInitializers, controllerAliases, fss, wait.NeverStop)
	code := cli.Run(command)
	os.Exit(code)
}

func cloudInitializer(config *cloudcontrollerconfig.CompletedConfig) cloudprovider.Interface {
	cloudConfig := config.ComponentConfig.KubeCloudShared.CloudProvider
	// 使用提供的云提供商名称和配置文件初始化 cloud provider
	cloud, err := cloudprovider.InitCloudProvider(cloudConfig.Name, cloudConfig.CloudConfigFile)
	if err != nil {
		klog.Fatalf("Cloud provider could not be initialized: %v", err)
	}
	if cloud == nil {
		klog.Fatalf("Cloud provider is nil")
	}

	if !cloud.HasClusterID() {
		if config.ComponentConfig.KubeCloudShared.AllowUntaggedCloud {
			klog.Warning("detected a cluster without a ClusterID.  A ClusterID will be required in the future.  Please tag your cluster to avoid any future issues")
		} else {
			klog.Fatalf("no ClusterID found.  A ClusterID is required for the cloud provider to function properly.  This check can be bypassed by setting the allow-untagged-cloud option")
		}
	}

	return cloud
}
