/*
Copyright 2018 The Kubernetes Authors.

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

package certificate

import (
	"bytes"
	"context"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/util/cert"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
	netutils "k8s.io/utils/net"
)

// TestAddressesToHostnamesAndIPs 验证节点地址列表到 DNS 名称与 IP 列表的转换：
// 空值、非法 IP 被忽略，重复值被去重且结果有序。
func TestAddressesToHostnamesAndIPs(t *testing.T) {
	tests := []struct {
		name         string
		addresses    []v1.NodeAddress
		wantDNSNames []string
		wantIPs      []net.IP
	}{
		{
			name:         "empty",
			addresses:    nil,
			wantDNSNames: nil,
			wantIPs:      nil,
		},
		{
			name:         "ignore empty values",
			addresses:    []v1.NodeAddress{{Type: v1.NodeHostName, Address: ""}},
			wantDNSNames: nil,
			wantIPs:      nil,
		},
		{
			name: "ignore invalid IPs",
			addresses: []v1.NodeAddress{
				{Type: v1.NodeInternalIP, Address: "1.2"},
				{Type: v1.NodeExternalIP, Address: "3.4"},
			},
			wantDNSNames: nil,
			wantIPs:      nil,
		},
		{
			name: "dedupe values",
			addresses: []v1.NodeAddress{
				{Type: v1.NodeHostName, Address: "hostname"},
				{Type: v1.NodeExternalDNS, Address: "hostname"},
				{Type: v1.NodeInternalDNS, Address: "hostname"},
				{Type: v1.NodeInternalIP, Address: "1.1.1.1"},
				{Type: v1.NodeExternalIP, Address: "1.1.1.1"},
			},
			wantDNSNames: []string{"hostname"},
			wantIPs:      []net.IP{netutils.ParseIPSloppy("1.1.1.1")},
		},
		{
			name: "order values",
			addresses: []v1.NodeAddress{
				{Type: v1.NodeHostName, Address: "hostname-2"},
				{Type: v1.NodeExternalDNS, Address: "hostname-1"},
				{Type: v1.NodeInternalDNS, Address: "hostname-3"},
				{Type: v1.NodeInternalIP, Address: "2.2.2.2"},
				{Type: v1.NodeExternalIP, Address: "1.1.1.1"},
				{Type: v1.NodeInternalIP, Address: "3.3.3.3"},
			},
			wantDNSNames: []string{"hostname-1", "hostname-2", "hostname-3"},
			wantIPs:      []net.IP{netutils.ParseIPSloppy("1.1.1.1"), netutils.ParseIPSloppy("2.2.2.2"), netutils.ParseIPSloppy("3.3.3.3")},
		},
		{
			name: "handle IP and DNS hostnames",
			addresses: []v1.NodeAddress{
				{Type: v1.NodeHostName, Address: "hostname"},
				{Type: v1.NodeHostName, Address: "1.1.1.1"},
			},
			wantDNSNames: []string{"hostname"},
			wantIPs:      []net.IP{netutils.ParseIPSloppy("1.1.1.1")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 转换结果需同时匹配 DNS 名称与 IP 两项期望。
			gotDNSNames, gotIPs := addressesToHostnamesAndIPs(tt.addresses)
			if !reflect.DeepEqual(gotDNSNames, tt.wantDNSNames) {
				t.Errorf("addressesToHostnamesAndIPs() gotDNSNames = %v, want %v", gotDNSNames, tt.wantDNSNames)
			}
			if !reflect.DeepEqual(gotIPs, tt.wantIPs) {
				t.Errorf("addressesToHostnamesAndIPs() gotIPs = %v, want %v", gotIPs, tt.wantIPs)
			}
		})
	}
}

// removeThenCreate 先删除目标文件（不存在则忽略）再重新创建，
// 用于模拟证书文件的“删除后重建”式更新。
func removeThenCreate(name string, data []byte, perm os.FileMode) error {
	if err := os.Remove(name); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}
	return os.WriteFile(name, data, perm)
}

// createCertAndKeyFiles 在 certDir 下生成自签名证书与私钥文件，返回两者路径。
func createCertAndKeyFiles(certDir string) (string, string, error) {
	cert, key, err := cert.GenerateSelfSignedCertKey("k8s.io", nil, nil)
	if err != nil {
		return "", "", nil
	}

	certPath := filepath.Join(certDir, "kubelet.cert")
	keyPath := filepath.Join(certDir, "kubelet.key")
	if err := removeThenCreate(certPath, cert, os.FileMode(0644)); err != nil {
		return "", "", err
	}

	if err := removeThenCreate(keyPath, key, os.FileMode(0600)); err != nil {
		return "", "", err
	}

	return certPath, keyPath, nil
}

// createCertAndKeyFilesUsingRename 先在临时目录中生成证书与私钥，
// 再通过目录重命名将其原子地移动到 <certDir>/identity/kubelet.cert 与
// <certDir>/identity/kubelet.key。
func createCertAndKeyFilesUsingRename(certDir string) (string, string, error) {
	cert, key, err := cert.GenerateSelfSignedCertKey("k8s.io", nil, nil)
	if err != nil {
		return "", "", nil
	}

	var certKeyPathFn = func(dataDir string) (string, string, string) {
		outputDir := filepath.Join(certDir, dataDir)
		return outputDir, filepath.Join(outputDir, "kubelet.cert"), filepath.Join(outputDir, "kubelet.key")
	}

	// 先在临时目录中写入文件，随后整体重命名，模拟 kubelet 证书轮转的原子更新方式。
	writeDir, writeCertPath, writeKeyPath := certKeyPathFn("identity.tmp")
	if err := os.Mkdir(writeDir, 0777); err != nil {
		return "", "", err
	}

	if err := removeThenCreate(writeCertPath, cert, os.FileMode(0644)); err != nil {
		return "", "", err
	}

	if err := removeThenCreate(writeKeyPath, key, os.FileMode(0600)); err != nil {
		return "", "", err
	}

	targetDir, certPath, keyPath := certKeyPathFn("identity")
	if err := os.RemoveAll(targetDir); err != nil {
		if !os.IsNotExist(err) {
			return "", "", err
		}
	}
	if err := os.Rename(writeDir, targetDir); err != nil {
		return "", "", err
	}

	return certPath, keyPath, nil
}

// TestKubeletServerCertificateFromFiles 验证基于文件的动态证书管理器能在证书文件更新后
// 重新加载证书，并在文件短暂缺失时继续提供缓存内容。
func TestKubeletServerCertificateFromFiles(t *testing.T) {
	tCtx := ktesting.Init(t)
	// 覆盖两种常见的证书文件更新方式：
	// 1. 直接删除并重写证书与私钥文件；
	// 2. 先在子目录中生成文件，更新时重命名整个目录。
	tests := []struct {
		name      string
		useRename bool
	}{
		{
			name:      "remove and create",
			useRename: false,
		},
		{
			name:      "rename cert dir",
			useRename: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			createFn := createCertAndKeyFiles
			if tt.useRename {
				createFn = createCertAndKeyFilesUsingRename
			}

			certDir := t.TempDir()
			certPath, keyPath, err := createFn(certDir)
			if err != nil {
				t.Fatalf("Unable to setup cert files: %v", err)
			}

			m, err := NewKubeletServerCertificateDynamicFileManager(certPath, keyPath)
			if err != nil {
				t.Fatalf("Unable to create certificte provider: %v", err)
			}

			// 启动管理器并获取当前加载的证书。
			m.Start()
			defer m.Stop()

			c := m.Current()
			if c == nil {
				t.Fatal("failed to provide valid certificate")
			}
			// 文件未变化时，多次读取应返回同一个证书对象。
			time.Sleep(100 * time.Millisecond)
			c2 := m.Current()
			if c2 == nil {
				t.Fatal("failed to provide valid certificate")
			}
			if c2 != c {
				t.Errorf("expected the same loaded certificate object when there is no cert file change, got different")
			}

			// 模拟后台更新证书文件。
			if _, _, err := createFn(certDir); err != nil {
				t.Fatalf("got errors when rotating certificate files in the test: %v", err)
			}

			// 轮询等待管理器加载出新证书，避免依赖固定 sleep 造成测试不稳定。
			err = wait.PollUntilContextTimeout(tCtx,
				100*time.Millisecond, 10*time.Second, true,
				func(_ context.Context) (bool, error) {
					c3 := m.Current()
					if c3 == nil {
						return false, fmt.Errorf("expected valid certificate regardless of file changes, but got nil")
					}
					if bytes.Equal(c.Certificate[0], c3.Certificate[0]) {
						t.Logf("loaded certificate is not updated")
						return false, nil
					}
					return true, nil
				})
			if err != nil {
				t.Errorf("failed to provide the updated certificate after file changes: %v", err)
			}

			if err = os.Remove(certPath); err != nil {
				t.Errorf("could not delete file in order to perform test")
			}

			// 证书文件被删除后，管理器应继续提供缓存内容而非返回 nil。
			time.Sleep(1 * time.Second)
			if m.Current() == nil {
				t.Errorf("expected the manager still provides cached content when certificate file was not available")
			}
		})
	}
}

// TestNewCertificateManagerConfigGetTemplate 验证 CSR 模板生成函数在不同节点地址
// 与 AllowDNSOnlyNodeCSR 特性门控组合下的输出。
func TestNewCertificateManagerConfigGetTemplate(t *testing.T) {
	nodeName := "fake-node"
	nodeIP := netutils.ParseIPSloppy("192.168.1.1")
	tests := []struct {
		name          string
		nodeAddresses []v1.NodeAddress
		want          *x509.CertificateRequest
		featuregate   bool
	}{
		{
			name:        "node addresses or hostnames and gate enabled",
			featuregate: true,
		},
		{
			name:        "node addresses or hostnames and gate disabled",
			featuregate: false,
		},
		{
			name: "only hostnames and gate enabled",
			nodeAddresses: []v1.NodeAddress{
				{
					Type:    v1.NodeHostName,
					Address: nodeName,
				},
			},
			want: &x509.CertificateRequest{
				Subject: pkix.Name{
					CommonName:   fmt.Sprintf("system:node:%s", nodeName),
					Organization: []string{"system:nodes"},
				},
				DNSNames: []string{nodeName},
			},
			featuregate: true,
		},
		{
			name: "only hostnames and gate disabled",
			nodeAddresses: []v1.NodeAddress{
				{
					Type:    v1.NodeHostName,
					Address: nodeName,
				},
			},
			featuregate: false,
		},
		{
			name: "only IP addresses and gate enabled",
			nodeAddresses: []v1.NodeAddress{
				{
					Type:    v1.NodeInternalIP,
					Address: nodeIP.String(),
				},
			},
			want: &x509.CertificateRequest{
				Subject: pkix.Name{
					CommonName:   fmt.Sprintf("system:node:%s", nodeName),
					Organization: []string{"system:nodes"},
				},
				IPAddresses: []net.IP{nodeIP},
			},
			featuregate: true,
		},
		{
			name: "only IP addresses and gate disabled",
			nodeAddresses: []v1.NodeAddress{
				{
					Type:    v1.NodeInternalIP,
					Address: nodeIP.String(),
				},
			},
			want: &x509.CertificateRequest{
				Subject: pkix.Name{
					CommonName:   fmt.Sprintf("system:node:%s", nodeName),
					Organization: []string{"system:nodes"},
				},
				IPAddresses: []net.IP{nodeIP},
			},
			featuregate: false,
		},
		{
			name: "IP addresses and hostnames and gate enabled",
			nodeAddresses: []v1.NodeAddress{
				{
					Type:    v1.NodeHostName,
					Address: nodeName,
				},
				{
					Type:    v1.NodeInternalIP,
					Address: nodeIP.String(),
				},
			},
			want: &x509.CertificateRequest{
				Subject: pkix.Name{
					CommonName:   fmt.Sprintf("system:node:%s", nodeName),
					Organization: []string{"system:nodes"},
				},
				DNSNames:    []string{nodeName},
				IPAddresses: []net.IP{nodeIP},
			},
			featuregate: true,
		},
		{
			name: "IP addresses and hostnames and gate disabled",
			nodeAddresses: []v1.NodeAddress{
				{
					Type:    v1.NodeHostName,
					Address: nodeName,
				},
				{
					Type:    v1.NodeInternalIP,
					Address: nodeIP.String(),
				},
			},
			want: &x509.CertificateRequest{
				Subject: pkix.Name{
					CommonName:   fmt.Sprintf("system:node:%s", nodeName),
					Organization: []string{"system:nodes"},
				},
				DNSNames:    []string{nodeName},
				IPAddresses: []net.IP{nodeIP},
			},
			featuregate: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 按用例开关 AllowDNSOnlyNodeCSR 特性门控。
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.AllowDNSOnlyNodeCSR, tt.featuregate)
			// 提供静态节点地址，保证模板生成结果可预期。
			getAddresses := func() []v1.NodeAddress {
				return tt.nodeAddresses
			}
			// 生成模板并与期望的证书请求逐字段比较。
			getTemplate := newGetTemplateFn(types.NodeName(nodeName), getAddresses)
			got := getTemplate()
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Wrong certificate, got %v expected %v", got, tt.want)
				return
			}
		})
	}
}
