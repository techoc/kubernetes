/*
Copyright 2024 The Kubernetes Authors.

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

// Package apply 实现 kubeadm upgrade apply 的阶段。
package apply

import (
	"fmt"

	errorsutil "k8s.io/apimachinery/pkg/util/errors"

	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/options"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/phases/workflow"
	clusterinfophase "k8s.io/kubernetes/cmd/kubeadm/app/phases/bootstraptoken/clusterinfo"
	nodebootstraptoken "k8s.io/kubernetes/cmd/kubeadm/app/phases/bootstraptoken/node"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

// NewBootstrapTokenPhase 返回新的 bootstrap-token 阶段。
func NewBootstrapTokenPhase() workflow.Phase {
	return workflow.Phase{
		Name:  "bootstrap-token",
		Short: "Configures bootstrap token and cluster-info RBAC rules",
		InheritFlags: []string{
			options.CfgPath,
			options.KubeconfigPath,
			options.DryRun,
		},
		Run: runBootstrapToken,
	}
}

func runBootstrapToken(c workflow.RunData) error {
	data, ok := c.(Data)
	if !ok {
		return errors.New("bootstrap-token phase invoked with an invalid data struct")
	}

	if data.DryRun() {
		fmt.Println("[dryrun] Would configure bootstrap token and cluster-info RBAC rules")
		return nil
	}

	fmt.Println("[upgrade/bootstrap-token] Configuring bootstrap token and cluster-info RBAC rules")

	client := data.Client()

	var errs []error
	// 创建 RBAC 规则，允许引导令牌获取节点。
	if err := nodebootstraptoken.AllowBootstrapTokensToGetNodes(client); err != nil {
		errs = append(errs, err)
	}

	// 创建或更新 RBAC 规则，允许引导令牌提交 CSR。
	if err := nodebootstraptoken.AllowBootstrapTokensToPostCSRs(client); err != nil {
		errs = append(errs, err)
	}

	// 创建或更新 RBAC 规则，允许引导令牌自动获得 CSR 批准。
	if err := nodebootstraptoken.AutoApproveNodeBootstrapTokens(client); err != nil {
		errs = append(errs, err)
	}

	// 创建或更新 RBAC 规则，允许节点轮换证书并自动获得 CSR 批准。
	if err := nodebootstraptoken.AutoApproveNodeCertificateRotation(client); err != nil {
		errs = append(errs, err)
	}

	// 创建或更新 RBAC 规则，允许 API 服务器的 kubelet 客户端访问 kubelet API。
	if err := nodebootstraptoken.AllowAPIServerToAccessKubeletAPI(client); err != nil {
		errs = append(errs, err)
	}

	// 创建或更新 RBAC 规则，使 cluster-info ConfigMap 可访问。
	if err := clusterinfophase.CreateClusterInfoRBACRules(client); err != nil {
		errs = append(errs, err)
	}

	return errorsutil.NewAggregate(errs)
}
