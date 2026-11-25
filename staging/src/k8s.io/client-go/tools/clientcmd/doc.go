/*
Copyright 2014 The Kubernetes Authors.

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

/*
Package clientcmd 提供从固定配置、.kubeconfig 文件、命令行参数或它们的任意
合并结果构建可用客户端的统一入口。示例中的加载规则决定多份 kubeconfig
的合并顺序（后者覆盖前者），而 ConfigOverrides 承载命令行覆盖值；两者都在
真正构建 client 时才解析，因此可以安全地延迟到需要时再读取。

从合并的 .kubeconfig 文件（本地目录、主目录）加载的示例用法：

	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	// 如果想更改加载规则（加载哪些文件、按什么顺序），可以在此处设置

	configOverrides := &clientcmd.ConfigOverrides{}
	// 如果想更改覆盖值或将它们绑定到命令行参数，有相应的方法可以帮你

	kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)
	config, err := kubeConfig.ClientConfig()
	if err != nil {
		// 自行处理错误
	}
	client, err := metav1.New(config)
	// ...
*/
package clientcmd
