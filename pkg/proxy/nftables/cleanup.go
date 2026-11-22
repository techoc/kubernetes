//go:build linux

/*
Copyright 2015 The Kubernetes Authors.

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

package nftables

import (
	"context"

	"k8s.io/klog/v2"
	"sigs.k8s.io/knftables"
)

// CleanupLeftovers 删除 Proxier 创建的所有 nftables 规则和 chain。
// 遇到错误时记录日志并返回 true。
func CleanupLeftovers(ctx context.Context) (encounteredError bool) {
	for _, family := range []knftables.Family{knftables.IPv4Family, knftables.IPv6Family} {
		nft, err := knftables.New(family, kubeProxyTable)
		if err != nil {
			// 该地址族不可用时直接跳过，另一种地址族仍可能被清理。
			continue
		}
		encounteredError = cleanupLeftoversForFamily(ctx, nft) || encounteredError
	}
	return
}

// cleanupLeftoversForFamily 删除给定地址族下 kube-proxy 的整张 table（连带其中的 chain 和规则）。
func cleanupLeftoversForFamily(ctx context.Context, nft knftables.Interface) (encounteredError bool) {
	logger := klog.FromContext(ctx)

	tx := nft.NewTransaction()
	tx.Delete(&knftables.Table{})
	err := nft.Run(ctx, tx)
	// 表不存在属于正常情况，只有其它错误才算清理失败。
	if err != nil && !knftables.IsNotFound(err) {
		logger.Error(err, "Error cleaning up nftables rules")
		return true
	}

	return false
}
