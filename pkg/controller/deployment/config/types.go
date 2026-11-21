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

package config

// DeploymentControllerConfiguration 描述 DeploymentController 的配置项。
type DeploymentControllerConfiguration struct {
	// ConcurrentDeploymentSyncs 是允许并发同步的 Deployment 对象数量。
	// 数值越大，Deployment 响应越及时，但 CPU 和网络负载也越高；
	// 需要在收敛速度和 API 服务器压力之间权衡。
	ConcurrentDeploymentSyncs int32
}
