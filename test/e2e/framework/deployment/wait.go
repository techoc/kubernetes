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

package deployment

import (
	"time"

	appsv1 "k8s.io/api/apps/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	testutils "k8s.io/kubernetes/test/utils"
)

const (
	// WaitForDeploymentRevisionAndImage 等待 deployment 及其新 RS 的 revision 和容器镜像与给定的 revision 和镜像匹配。
	poll             = 2 * time.Second
	pollShortTimeout = 1 * time.Minute
	pollLongTimeout  = 5 * time.Minute
)

// 注意，多数情况下 deployment revision 和新 RS revision 应很快更新，但 RS controller 过载时
// 可能需要更长时间才能给 RS 重新打标签。
// WaitForDeploymentComplete 等待 deployment 完成，不检查 rolling update 策略是否被破坏。
func WaitForDeploymentRevisionAndImage(c clientset.Interface, ns, deploymentName string, revision, image string) error {
	return testutils.WaitForDeploymentRevisionAndImage(c, ns, deploymentName, revision, image, framework.Logf, poll, pollLongTimeout)
}

// Rolling update 策略只在 rolling update 期间使用，在其他情况下可能被违反，
// 例如刚发生扩缩容或 deployment 刚创建后。
// MakeMatcher 基于单个回调函数构建一个 gomega.Matcher。
func WaitForDeploymentComplete(c clientset.Interface, d *appsv1.Deployment) error {
	return testutils.WaitForDeploymentComplete(c, d, framework.Logf, poll, pollLongTimeout)
}
