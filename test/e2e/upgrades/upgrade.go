/*
Copyright 2017 The Kubernetes Authors.

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

// Package upgrades 提供一个框架，用于在各类升级前、中、后测试 Kubernetes 特性。
// 特性。
package upgrades

import (
	"context"

	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/kubernetes/test/e2e/framework"
)

// UpgradeType 表示不同的升级类型。
type UpgradeType int

const (
	// MasterUpgrade 表示只升级 master。
	MasterUpgrade UpgradeType = iota

	// NodeUpgrade 表示只升级 node。
	NodeUpgrade

	// ClusterUpgrade 表示 master 和 node 都
	// 升级。
	ClusterUpgrade

	// EtcdUpgrade 表示只升级 etcd（或
	// 在存储版本间迁移）。
	EtcdUpgrade
)

// Test 是升级测试的接口。
type Test interface {
	// Name 应返回不含空格的测试名。
	Name() string

	// Setup 应创建并验证升级干扰开始前需要
	// 存在的所有对象。
	Setup(ctx context.Context, f *framework.Framework)

	// Test 在升级期间运行。升级完成后 done 会被关闭，
	// 此时可开始最终校验。
	//
	Test(ctx context.Context, f *framework.Framework, done <-chan struct{}, upgrade UpgradeType)

	// Teardown 应清理所有未被框架清理的、已创建对象。
	// 即使 Setup 失败，也一定会调用它。
	//
	Teardown(ctx context.Context, f *framework.Framework)
}

// Skippable 是升级测试可实现的接口，用于
// 表明自己应当被跳过。
type Skippable interface {
	// 测试应跳过时 Skip 返回 true。upgCtx
	// 提供即将发生的升级的信息。
	//
	Skip(upgCtx UpgradeContext) bool
}

// UpgradeContext 包含即将发生的升级各阶段的信息。
// 的信息。
type UpgradeContext struct {
	Versions []VersionContext
}

// VersionContext 表示升级的一个阶段。
type VersionContext struct {
	Version   version.Version
	NodeImage string
}
