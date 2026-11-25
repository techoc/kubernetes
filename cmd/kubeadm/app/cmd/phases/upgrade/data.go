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

// Package upgrade 包含 kubeadm upgrade 的公共阶段。
package upgrade

import (
	"io"

	"k8s.io/apimachinery/pkg/util/sets"
	clientset "k8s.io/client-go/kubernetes"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
)

// Data 是 kubeadm upgrade 阶段使用的公共接口。
type Data interface {
	// EtcdUpgrade 返回是否升级 etcd；使用外部 etcd 时应由外部负责。
	EtcdUpgrade() bool
	// RenewCerts 返回是否在升级期间续期证书。
	RenewCerts() bool
	// DryRun 返回是否仅模拟执行；此时写入路径通常指向临时目录。
	DryRun() bool
	// Cfg 返回当前 upgrade 工作流使用的配置。
	Cfg() *kubeadmapi.UpgradeConfiguration
	// InitCfg 返回升级控制平面所需的 init 配置。
	InitCfg() *kubeadmapi.InitConfiguration
	// IsControlPlaneNode 返回节点是否承载控制平面；仅控制平面节点需要相关升级。
	IsControlPlaneNode() bool
	// Client 返回访问集群的客户端。
	Client() clientset.Interface
	// IgnorePreflightErrors 返回允许忽略的预检错误集合。
	IgnorePreflightErrors() sets.Set[string]
	// PatchesDir 返回组件补丁目录。
	PatchesDir() string
	// OutputWriter 返回命令输出目标。
	OutputWriter() io.Writer
	// KubeConfigDir 返回配置写入目录；dry-run 时返回临时目录。
	KubeConfigDir() string
	// KubeletDir 返回 kubelet 配置写入目录；dry-run 时返回临时目录。
	KubeletDir() string
}

// 包内类型，仅供测试使用。
type testData struct{}

// testData 必须满足 Data。
var _ Data = &testData{}

func (t *testData) EtcdUpgrade() bool                       { return false }
func (t *testData) RenewCerts() bool                        { return false }
func (t *testData) DryRun() bool                            { return false }
func (t *testData) Cfg() *kubeadmapi.UpgradeConfiguration   { return nil }
func (t *testData) InitCfg() *kubeadmapi.InitConfiguration  { return nil }
func (t *testData) IsControlPlaneNode() bool                { return false }
func (t *testData) Client() clientset.Interface             { return nil }
func (t *testData) IgnorePreflightErrors() sets.Set[string] { return nil }
func (t *testData) PatchesDir() string                      { return "" }
func (t *testData) OutputWriter() io.Writer                 { return nil }
func (t *testData) KubeConfigDir() string                   { return "" }
func (t *testData) KubeletDir() string                      { return "" }
