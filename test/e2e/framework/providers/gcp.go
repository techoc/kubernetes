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

package providers

import (
	"context"
	"fmt"
	"path"

	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
)

// LocationParamGKE 返回 gcloud 命令与位置相关的参数。
func LocationParamGKE() string {
	if framework.TestContext.CloudConfig.MultiMaster {
		// 正在测试 GKE 区域集群。
		return fmt.Sprintf("--region=%s", framework.TestContext.CloudConfig.Region)
	}
	return fmt.Sprintf("--zone=%s", framework.TestContext.CloudConfig.Zone)
}

// MasterUpgradeGKE 将 GKE 上的 master 节点升级到指定版本。
func MasterUpgradeGKE(ctx context.Context, namespace string, v string) error {
	framework.Logf("Upgrading master to %q", v)
	args := []string{
		"container",
		"clusters",
		fmt.Sprintf("--project=%s", framework.TestContext.CloudConfig.ProjectID),
		LocationParamGKE(),
		"upgrade",
		framework.TestContext.CloudConfig.Cluster,
		"--master",
		fmt.Sprintf("--cluster-version=%s", v),
		"--quiet",
	}
	_, _, err := framework.RunCmd("gcloud", framework.AppendContainerCommandGroupIfNeeded(args)...)
	if err != nil {
		return err
	}

	e2enode.WaitForSSHTunnels(ctx, namespace)

	return nil
}

// GCEUpgradeScript 返回 GCE 上用于升级的脚本路径。
func GCEUpgradeScript() string {
	if len(framework.TestContext.GCEUpgradeScript) == 0 {
		return path.Join(framework.TestContext.RepoRoot, "cluster/gce/upgrade.sh")
	}
	return framework.TestContext.GCEUpgradeScript
}
