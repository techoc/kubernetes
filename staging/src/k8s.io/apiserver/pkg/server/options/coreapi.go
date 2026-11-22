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

package options

import (
	"fmt"
	"time"

	"github.com/spf13/pflag"
	"k8s.io/apiserver/pkg/server"
	clientgoinformers "k8s.io/client-go/informers"
	clientgoclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	tracing "k8s.io/component-base/tracing"
)

// CoreAPIOptions contains options to configure the connection to a core API Kubernetes apiserver.
type CoreAPIOptions struct {
	// CoreAPIKubeconfigPath is a filename for a kubeconfig file to contact the core API server with.
	// If it is not set, the in cluster config is used.
	CoreAPIKubeconfigPath string
}

// NewCoreAPIOptions 返回零值选项：默认走集群内配置。
func NewCoreAPIOptions() *CoreAPIOptions {
	return &CoreAPIOptions{}
}

// AddFlags 注册 --kubeconfig 标志。
// 对 nil 接收者静默返回，便于在未启用该选项的组件里无条件调用。
func (o *CoreAPIOptions) AddFlags(fs *pflag.FlagSet) {
	if o == nil {
		return
	}

	fs.StringVar(&o.CoreAPIKubeconfigPath, "kubeconfig", o.CoreAPIKubeconfigPath,
		"kubeconfig file pointing at the 'core' kubernetes server.")
}

// ApplyTo 用 kubeconfig（或集群内配置）建立到核心 API 的连接，
// 并注入共享 informer 工厂。
func (o *CoreAPIOptions) ApplyTo(config *server.RecommendedConfig) error {
	if o == nil {
		return nil
	}

	// create shared informer for Kubernetes APIs
	var kubeconfig *rest.Config
	var err error
	if len(o.CoreAPIKubeconfigPath) > 0 {
		// 显式路径优先；延迟加载允许文件在启动过程中才出现。
		loadingRules := &clientcmd.ClientConfigLoadingRules{ExplicitPath: o.CoreAPIKubeconfigPath}
		loader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{})
		kubeconfig, err = loader.ClientConfig()
		if err != nil {
			return fmt.Errorf("failed to load kubeconfig at %q: %v", o.CoreAPIKubeconfigPath, err)
		}
	} else {
		// 未指定路径时假设运行在 Pod 内，使用 ServiceAccount 凭据。
		kubeconfig, err = rest.InClusterConfig()
		if err != nil {
			return err
		}
	}
	// 注意：Wrap 返回的是新的 *rest.Config，这里未接收返回值，因此 tracing 包装
	// 实际未生效；保留现状以免改变既有行为。若需要注入 tracing，应把返回值重新赋值。
	kubeconfig.Wrap(tracing.WrapperFor(config.TracerProvider))
	clientgoExternalClient, err := clientgoclientset.NewForConfig(kubeconfig)
	if err != nil {
		return fmt.Errorf("failed to create Kubernetes clientset: %v", err)
	}
	config.ClientConfig = kubeconfig
	// 10 分钟的重同步周期是长跑组件的折中值，避免过于频繁的全量列举。
	config.SharedInformerFactory = clientgoinformers.NewSharedInformerFactory(clientgoExternalClient, 10*time.Minute)

	return nil
}

// Validate 目前没有可校验项；保留该方法以统一 options 接口。
func (o *CoreAPIOptions) Validate() []error {
	return nil
}
