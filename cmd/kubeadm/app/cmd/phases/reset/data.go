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

package phases

import (
	"io"

	"k8s.io/apimachinery/pkg/util/sets"
	clientset "k8s.io/client-go/kubernetes"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
)

// resetData 是 reset 阶段使用的接口。
// cmd/reset.go 中的 resetData 类型必须满足此接口。
type resetData interface {
	// ForceReset 返回是否跳过清理确认。
	ForceReset() bool
	// InputReader 返回读取用户确认输入的来源。
	InputReader() io.Reader
	// IgnorePreflightErrors 返回允许忽略的预检错误集合。
	IgnorePreflightErrors() sets.Set[string]
	// Cfg 返回用于检查集群组件的 init 配置。
	Cfg() *kubeadmapi.InitConfiguration
	// ResetCfg 返回当前 reset 工作流使用的配置。
	ResetCfg() *kubeadmapi.ResetConfiguration
	// DryRun 返回是否仅模拟清理，不执行实际删除。
	DryRun() bool
	// Client 返回访问集群的客户端；不可用时可能为 nil。
	Client() clientset.Interface
	// CertificatesDir 返回待清理的证书目录。
	CertificatesDir() string
	// CRISocketPath 返回节点的 CRI 套接字路径。
	CRISocketPath() string
	// CleanupTmpDir 返回是否清理 kubeadm 临时目录。
	CleanupTmpDir() bool
}

// 包内类型，仅供测试使用。
type testData struct{}

// testData 必须满足 resetData。
var _ resetData = &testData{}

func (t *testData) ForceReset() bool                         { return false }
func (t *testData) InputReader() io.Reader                   { return nil }
func (t *testData) IgnorePreflightErrors() sets.Set[string]  { return nil }
func (t *testData) Cfg() *kubeadmapi.InitConfiguration       { return nil }
func (t *testData) DryRun() bool                             { return false }
func (t *testData) Client() clientset.Interface              { return nil }
func (t *testData) CertificatesDir() string                  { return "" }
func (t *testData) CRISocketPath() string                    { return "" }
func (t *testData) CleanupTmpDir() bool                      { return false }
func (t *testData) ResetCfg() *kubeadmapi.ResetConfiguration { return nil }
