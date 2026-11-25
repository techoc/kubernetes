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

// Package phases 包含 kubeadm join 的命令行阶段。
package phases

import (
	"io"

	"k8s.io/apimachinery/pkg/util/sets"
	clientset "k8s.io/client-go/kubernetes"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
)

// JoinData 是 join 阶段使用的接口。
// cmd/join.go 中的 joinData 类型必须满足此接口。
type JoinData interface {
	// CertificateKey 返回用于解密控制平面证书的密钥。
	CertificateKey() string
	// Cfg 返回当前 join 工作流使用的配置。
	Cfg() *kubeadmapi.JoinConfiguration
	// TLSBootstrapCfg 返回发现阶段使用的临时 kubeconfig。
	TLSBootstrapCfg() (*clientcmdapi.Config, error)
	// InitCfg 返回从集群发现并获取的 init 配置。
	InitCfg() (*kubeadmapi.InitConfiguration, error)
	// Client 返回以 admin.conf 中身份访问集群的客户端。
	Client() (clientset.Interface, error)
	// WaitControlPlaneClient 返回指向本地 API 服务器、用于等待控制平面健康的客户端。
	WaitControlPlaneClient() (clientset.Interface, error)
	// IgnorePreflightErrors 返回允许忽略的预检错误集合。
	IgnorePreflightErrors() sets.Set[string]
	// OutputWriter 返回命令输出目标。
	OutputWriter() io.Writer
	// PatchesDir 返回组件补丁目录；命令行值优先于配置值。
	PatchesDir() string
	// DryRun 返回是否仅模拟执行；此时写入路径通常指向临时目录。
	DryRun() bool
	// KubeConfigDir 返回配置写入目录；dry-run 时返回临时目录。
	KubeConfigDir() string
	// KubeletDir 返回 kubelet 配置写入目录；dry-run 时返回临时目录。
	KubeletDir() string
	// ManifestDir 返回静态 Pod 清单写入目录；dry-run 时返回临时目录。
	ManifestDir() string
	// CertificateWriteDir 返回证书写入目录；dry-run 时返回临时目录。
	CertificateWriteDir() string
}

// 包内类型，仅供测试使用。
type testJoinData struct{}

// testJoinData 必须满足 JoinData。
var _ JoinData = &testJoinData{}

func (j *testJoinData) CertificateKey() string                               { return "" }
func (j *testJoinData) Cfg() *kubeadmapi.JoinConfiguration                   { return nil }
func (j *testJoinData) TLSBootstrapCfg() (*clientcmdapi.Config, error)       { return nil, nil }
func (j *testJoinData) InitCfg() (*kubeadmapi.InitConfiguration, error)      { return nil, nil }
func (j *testJoinData) Client() (clientset.Interface, error)                 { return nil, nil }
func (j *testJoinData) WaitControlPlaneClient() (clientset.Interface, error) { return nil, nil }
func (j *testJoinData) IgnorePreflightErrors() sets.Set[string]              { return nil }
func (j *testJoinData) OutputWriter() io.Writer                              { return nil }
func (j *testJoinData) PatchesDir() string                                   { return "" }
func (j *testJoinData) DryRun() bool                                         { return false }
func (j *testJoinData) KubeConfigDir() string                                { return "" }
func (j *testJoinData) KubeletDir() string                                   { return "" }
func (j *testJoinData) ManifestDir() string                                  { return "" }
func (j *testJoinData) CertificateWriteDir() string                          { return "" }
