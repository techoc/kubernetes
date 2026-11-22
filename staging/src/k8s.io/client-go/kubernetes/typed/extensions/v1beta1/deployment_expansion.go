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

package v1beta1

import (
	"context"

	"k8s.io/api/extensions/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	scheme "k8s.io/client-go/kubernetes/scheme"
)

// The DeploymentExpansion interface allows manually adding extra methods to the DeploymentInterface.
//
// 中文说明：手写的扩展方法接口，生成代码不会覆盖它。extensions/v1beta1 的 Deployment
// 已弃用，但 rollback 子资源仍只能通过该扩展方法访问。
type DeploymentExpansion interface {
	// Rollback 触发指定 Deployment 回滚到上一个 revision。
	Rollback(context.Context, *v1beta1.DeploymentRollback, metav1.CreateOptions) error
}

// Rollback applied the provided DeploymentRollback to the named deployment in the current namespace.
//
// 中文说明：向 deployments/<name>/rollback 子资源 POST。请求体中的 Name 决定目标对象，
// 因此额外用 Name 指定 URL 路径；CreateOptions 通过参数编码器转为查询参数。
func (c *deployments) Rollback(ctx context.Context, deploymentRollback *v1beta1.DeploymentRollback, opts metav1.CreateOptions) error {
	return c.GetClient().Post().Namespace(c.GetNamespace()).Resource("deployments").Name(deploymentRollback.Name).VersionedParams(&opts, scheme.ParameterCodec).SubResource("rollback").Body(deploymentRollback).Do(ctx).Error()
}
