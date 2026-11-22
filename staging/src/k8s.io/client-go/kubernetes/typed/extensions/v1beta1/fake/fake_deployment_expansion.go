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

package fake

import (
	"context"

	"k8s.io/api/extensions/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	core "k8s.io/client-go/testing"
)

// Rollback 以 create 动作对 deployments/rollback 子资源发起调用。
//
// 历史 API 中没有独立的 rollback 子资源方法，服务端将其建模为子资源上的
// create；fake 客户端复用同一套 action 记录，便于测试断言请求形状。
func (c *fakeDeployments) Rollback(ctx context.Context, deploymentRollback *v1beta1.DeploymentRollback, opts metav1.CreateOptions) error {
	// 显式构造 CreateAction，而不是走通用 Create，是因为对象不是 Deployment
	// 而是 DeploymentRollback，且需要标记 subresource。
	action := core.CreateActionImpl{}
	action.Verb = "create"
	action.Resource = c.Resource()
	action.Subresource = "rollback"
	action.Object = deploymentRollback

	_, err := c.Fake.Invokes(action, deploymentRollback)
	return err
}
