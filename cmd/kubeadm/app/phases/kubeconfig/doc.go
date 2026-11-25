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

package kubeconfig

/*

	阶段：KUBECONFIG

	输入：
		来自 InitConfiguration
			API Server 端点（AdvertiseAddress + BindPort）用于让 kubeconfig 文件找到控制平面，必填。
			KubernetesDir 路径用于确定 kubeconfig 文件的写入位置，必填。
			PKIPath 用于确定所有证书的存储位置，必填。

	输出：
		文件写入 KubernetesDir（默认 /etc/kubernetes）：
		 - admin.conf
		 - kubelet.conf
		 - scheduler.conf
		 - controller-manager.conf
*/
