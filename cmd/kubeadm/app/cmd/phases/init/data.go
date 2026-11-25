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
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
)

// InitData 是 init 阶段使用的接口。
// cmd/init.go 中的 initData 类型必须满足此接口。
type InitData interface {
	// UploadCerts 返回是否将控制平面证书上传到集群。
	UploadCerts() bool
	// CertificateKey 返回用于加密和解密上传证书的密钥。
	CertificateKey() string
	// SetCertificateKey 设置证书密钥；缺失时无法解密上传的证书。
	SetCertificateKey(key string)
	// SkipCertificateKeyPrint 返回是否避免在输出中暴露证书密钥。
	SkipCertificateKeyPrint() bool
	// Cfg 返回当前 init 工作流使用的配置。
	Cfg() *kubeadmapi.InitConfiguration
	// DryRun 返回是否仅模拟执行；此时写入路径通常指向临时目录。
	DryRun() bool
	// SkipTokenPrint 返回是否避免打印默认引导令牌。
	SkipTokenPrint() bool
	// IgnorePreflightErrors 返回允许忽略的预检错误集合。
	IgnorePreflightErrors() sets.Set[string]
	// CertificateWriteDir 返回证书写入目录；dry-run 时返回临时目录。
	CertificateWriteDir() string
	// CertificateDir 返回用户配置的证书目录，不随 dry-run 临时目录改变。
	CertificateDir() string
	// KubeConfig 返回 kubeadm 使用的配置；使用默认路径时仅连接本地 API 服务器。
	KubeConfig() (*clientcmdapi.Config, error)
	// KubeConfigOriginal 返回未经 kubeadm 修改的原始配置。
	KubeConfigOriginal() (*clientcmdapi.Config, error)
	// KubeConfigDir 返回配置写入目录；dry-run 时返回临时目录。
	KubeConfigDir() string
	// KubeConfigPath 返回配置加载路径。
	KubeConfigPath() string
	// ManifestDir 返回静态 Pod 清单写入目录；dry-run 时返回临时目录。
	ManifestDir() string
	// KubeletDir 返回 kubelet 配置写入目录；dry-run 时返回临时目录。
	KubeletDir() string
	// ExternalCA 返回是否由用户提供外部 CA；此时 kubeadm 无法重签缺失证书。
	ExternalCA() bool
	// OutputWriter 返回命令输出目标。
	OutputWriter() io.Writer
	// Client 返回集群客户端；调用前必须已建立所需 kubeconfig。
	Client() (clientset.Interface, error)
	// Tokens 返回可使用的引导令牌。
	Tokens() []string
	// PatchesDir 返回组件补丁目录；命令行值优先于配置值。
	PatchesDir() string
}

// 包内类型，仅供测试使用。
type testInitData struct{}

// testInitData 必须满足 InitData。
var _ InitData = &testInitData{}

func (t *testInitData) UploadCerts() bool                                 { return false }
func (t *testInitData) CertificateKey() string                            { return "" }
func (t *testInitData) SetCertificateKey(key string)                      {}
func (t *testInitData) SkipCertificateKeyPrint() bool                     { return false }
func (t *testInitData) Cfg() *kubeadmapi.InitConfiguration                { return nil }
func (t *testInitData) DryRun() bool                                      { return false }
func (t *testInitData) SkipTokenPrint() bool                              { return false }
func (t *testInitData) IgnorePreflightErrors() sets.Set[string]           { return nil }
func (t *testInitData) CertificateWriteDir() string                       { return "" }
func (t *testInitData) CertificateDir() string                            { return "" }
func (t *testInitData) KubeConfig() (*clientcmdapi.Config, error)         { return nil, nil }
func (t *testInitData) KubeConfigOriginal() (*clientcmdapi.Config, error) { return nil, nil }
func (t *testInitData) KubeConfigDir() string                             { return "" }
func (t *testInitData) KubeConfigPath() string                            { return "" }
func (t *testInitData) ManifestDir() string                               { return "" }
func (t *testInitData) KubeletDir() string                                { return "" }
func (t *testInitData) ExternalCA() bool                                  { return false }
func (t *testInitData) OutputWriter() io.Writer                           { return nil }
func (t *testInitData) Client() (clientset.Interface, error)              { return nil, nil }
func (t *testInitData) Tokens() []string                                  { return nil }
func (t *testInitData) PatchesDir() string                                { return "" }
