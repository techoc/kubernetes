//go:build linux

/*
Copyright The Kubernetes Authors.

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
	"errors"
	"testing"

	"sigs.k8s.io/knftables"
)

// addKubeProxyTable 在给定的 nftables fake 中新建 kube-proxy 表，
// 为后续的清理测试提供一张确实存在的表。
func addKubeProxyTable(t *testing.T, nft *knftables.Fake) {
	t.Helper()
	tx := nft.NewTransaction()
	tx.Add(&knftables.Table{})
	if err := nft.Run(context.Background(), tx); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// TestCleanupLeftoversDeletesExistingTables 验证 cleanupLeftoversForFamily
// 能删除 IPv4 与 IPv6 两个协议族中已存在的 kube-proxy 表，且不报告错误。
func TestCleanupLeftoversDeletesExistingTables(t *testing.T) {
	ctx := context.Background()
	ipv4 := knftables.NewFake(knftables.IPv4Family, kubeProxyTable)
	ipv6 := knftables.NewFake(knftables.IPv6Family, kubeProxyTable)
	addKubeProxyTable(t, ipv4)
	addKubeProxyTable(t, ipv6)

	// 两个协议族分别清理，且都必须返回无错误。
	if got := cleanupLeftoversForFamily(ctx, ipv4); got {
		t.Fatalf("cleanupLeftoversForFamily(ipv4) = %v, want false", got)
	}
	if got := cleanupLeftoversForFamily(ctx, ipv6); got {
		t.Fatalf("cleanupLeftoversForFamily(ipv6) = %v, want false", got)
	}
	// 清理完成后 fake 中的表引用应为 nil，确认表确实被删除。
	if ipv4.Table != nil {
		t.Error("IPv4 kube-proxy table still present after cleanup")
	}
	if ipv6.Table != nil {
		t.Error("IPv6 kube-proxy table still present after cleanup")
	}
}

// TestCleanupLeftoversNotFoundIgnored 验证表不存在时清理是幂等的：
// 删除操作返回 NotFound 也不应被视为错误。
func TestCleanupLeftoversNotFoundIgnored(t *testing.T) {
	ctx := context.Background()
	// 不预先建表：fake 上的删除会返回 NotFound，用于验证该错误被忽略。
	nft := knftables.NewFake(knftables.IPv4Family, kubeProxyTable)
	if got := cleanupLeftoversForFamily(ctx, nft); got {
		t.Fatalf("cleanupLeftoversForFamily() = %v, want false when table is absent", got)
	}
}

// fakeWithRunErr 嵌入 knftables.Fake，并让 Run 始终返回 runErr，
// 用于在测试中模拟非 NotFound 的执行失败。
type fakeWithRunErr struct {
	*knftables.Fake
	runErr error
}

// Run 覆盖嵌入 Fake 的 Run，直接返回预设的错误，
// 从而让清理逻辑走到错误处理分支。
func (f *fakeWithRunErr) Run(ctx context.Context, tx *knftables.Transaction) error {
	return f.runErr
}

// TestCleanupLeftoversRunErrorSetsEncountered 验证 Run 返回非 NotFound 错误时，
// cleanupLeftoversForFamily 会将该错误上报（返回 true）。
func TestCleanupLeftoversRunErrorSetsEncountered(t *testing.T) {
	ctx := context.Background()
	runErr := errors.New("nft run failed")
	nft := &fakeWithRunErr{Fake: knftables.NewFake(knftables.IPv4Family, kubeProxyTable), runErr: runErr}
	if got := cleanupLeftoversForFamily(ctx, nft); !got {
		t.Fatal("cleanupLeftoversForFamily() = false, want true when Run returns a non-NotFound error")
	}
}
