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
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/phases/upgrade"
)

// Data 是 kubeadm upgrade apply 阶段使用的接口。
// cmd/upgrade/apply.go 中的 applyData 类型必须满足此接口。
type Data interface {
	upgrade.Data

	// SessionIsInteractive 返回是否允许交互确认；-y、-f 和 --dry-run 会退出交互模式。
	SessionIsInteractive() bool
	// AllowExperimentalUpgrades 返回是否允许升级到 alpha、beta 或候选发布版本。
	AllowExperimentalUpgrades() bool
	// AllowRCUpgrades 返回是否允许升级到候选发布版本。
	AllowRCUpgrades() bool
	// ForceUpgrade 返回是否允许跳过可跳过的升级错误。
	ForceUpgrade() bool
}
