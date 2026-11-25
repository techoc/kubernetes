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

package kubeconfig

import (
	"bytes"
	"context"
	"crypto"
	"crypto/x509"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/lithammer/dedent"

	rbac "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientset "k8s.io/client-go/kubernetes"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	clientgotesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	certsphase "k8s.io/kubernetes/cmd/kubeadm/app/phases/certs"
	kubeadmutil "k8s.io/kubernetes/cmd/kubeadm/app/util"
	certstestutil "k8s.io/kubernetes/cmd/kubeadm/app/util/certs/testing"
	configutil "k8s.io/kubernetes/cmd/kubeadm/app/util/config/testing"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	filesutil "k8s.io/kubernetes/cmd/kubeadm/app/util/files/testing"
	kubeconfigutil "k8s.io/kubernetes/cmd/kubeadm/app/util/kubeconfig/testing"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/pkiutil"
)

func TestGetKubeConfigSpecsFailsIfCADoesntExists(t *testing.T) {
	// 为测试用例创建临时目录（不含 CA）
	tmpdir := t.TempDir()

	// 创建指向 pkidir 目录的 InitConfiguration
	cfg := &kubeadmapi.InitConfiguration{
		ClusterConfiguration: kubeadmapi.ClusterConfiguration{
			CertificatesDir: tmpdir,
		},
	}

	// 执行 getKubeConfigSpecs
	if _, err := getKubeConfigSpecs(cfg); err == nil {
		t.Error("getKubeConfigSpecs didnt failed when expected")
	}
}

func TestGetKubeConfigSpecs(t *testing.T) {
	// 为测试用例创建临时目录
	tmpdir := t.TempDir()

	// 在临时目录中添加含 CA 证书的 pki 目录
	pkidir := certstestutil.SetupPkiDirWithCertificateAuthority(t, tmpdir)

	// 创建指向 pkidir 目录的 InitConfiguration
	cfgs := []*kubeadmapi.InitConfiguration{
		{
			LocalAPIEndpoint: kubeadmapi.APIEndpoint{AdvertiseAddress: "1.2.3.4", BindPort: 1234},
			ClusterConfiguration: kubeadmapi.ClusterConfiguration{
				CertificatesDir:     pkidir,
				EncryptionAlgorithm: kubeadmapi.EncryptionAlgorithmECDSAP256,
			},
			NodeRegistration: kubeadmapi.NodeRegistrationOptions{Name: "valid-node-name"},
		},
		{
			LocalAPIEndpoint: kubeadmapi.APIEndpoint{AdvertiseAddress: "1.2.3.4", BindPort: 1234},
			ClusterConfiguration: kubeadmapi.ClusterConfiguration{
				CertificatesDir:     pkidir,
				EncryptionAlgorithm: kubeadmapi.EncryptionAlgorithmECDSAP384,
			},
			NodeRegistration: kubeadmapi.NodeRegistrationOptions{Name: "valid-node-name"},
		},
		{
			LocalAPIEndpoint: kubeadmapi.APIEndpoint{AdvertiseAddress: "1.2.3.4", BindPort: 1234},
			ClusterConfiguration: kubeadmapi.ClusterConfiguration{
				ControlPlaneEndpoint: "api.k8s.io",
				CertificatesDir:      pkidir,
			},
			NodeRegistration: kubeadmapi.NodeRegistrationOptions{Name: "valid-node-name"},
		},
		{
			LocalAPIEndpoint: kubeadmapi.APIEndpoint{AdvertiseAddress: "1.2.3.4", BindPort: 1234},
			ClusterConfiguration: kubeadmapi.ClusterConfiguration{
				ControlPlaneEndpoint: "api.k8s.io:4321",
				CertificatesDir:      pkidir,
			},
			NodeRegistration: kubeadmapi.NodeRegistrationOptions{Name: "valid-node-name"},
		},
		{
			LocalAPIEndpoint: kubeadmapi.APIEndpoint{AdvertiseAddress: "1.2.3.4", BindPort: 1234},
			ClusterConfiguration: kubeadmapi.ClusterConfiguration{
				ControlPlaneEndpoint: "api.k8s.io",
				CertificatesDir:      pkidir,
			},
			NodeRegistration: kubeadmapi.NodeRegistrationOptions{Name: "valid-node-name"},
		},
		{
			LocalAPIEndpoint: kubeadmapi.APIEndpoint{AdvertiseAddress: "1.2.3.4", BindPort: 1234},
			ClusterConfiguration: kubeadmapi.ClusterConfiguration{
				ControlPlaneEndpoint: "api.k8s.io:4321",
				CertificatesDir:      pkidir,
			},
			NodeRegistration: kubeadmapi.NodeRegistrationOptions{Name: "valid-node-name"},
		},
	}

	for i, cfg := range cfgs {
		var assertions = []struct {
			kubeConfigFile string
			clientName     string
			organizations  []string
		}{
			{
				kubeConfigFile: kubeadmconstants.AdminKubeConfigFileName,
				clientName:     "kubernetes-admin",
				organizations:  []string{kubeadmconstants.ClusterAdminsGroupAndClusterRoleBinding},
			},
			{
				kubeConfigFile: kubeadmconstants.SuperAdminKubeConfigFileName,
				clientName:     "kubernetes-super-admin",
				organizations:  []string{kubeadmconstants.SystemPrivilegedGroup},
			},
			{
				kubeConfigFile: kubeadmconstants.KubeletKubeConfigFileName,
				clientName:     fmt.Sprintf("%s%s", kubeadmconstants.NodesUserPrefix, cfg.NodeRegistration.Name),
				organizations:  []string{kubeadmconstants.NodesGroup},
			},
			{
				kubeConfigFile: kubeadmconstants.ControllerManagerKubeConfigFileName,
				clientName:     kubeadmconstants.ControllerManagerUser,
			},
			{
				kubeConfigFile: kubeadmconstants.SchedulerKubeConfigFileName,
				clientName:     kubeadmconstants.SchedulerUser,
			},
		}

		for _, assertion := range assertions {
			t.Run(fmt.Sprintf("%d-%s", i, assertion.clientName), func(t *testing.T) {
				// 执行 getKubeConfigSpecs
				specs, err := getKubeConfigSpecs(cfg)
				if err != nil {
					t.Fatal("getKubeConfigSpecs failed!")
				}

				var spec *kubeConfigSpec
				var ok bool

				// 断言 kubeConfigFile 对应的配置存在
				if spec, ok = specs[assertion.kubeConfigFile]; !ok {
					t.Errorf("getKubeConfigSpecs didn't create spec for %s ", assertion.kubeConfigFile)
					return
				}

				// 断言 clientName
				if spec.ClientName != assertion.clientName {
					t.Errorf("getKubeConfigSpecs for %s clientName is %s, expected %s", assertion.kubeConfigFile, spec.ClientName, assertion.clientName)
				}

				// 断言 Organizations
				if spec.ClientCertAuth == nil || !reflect.DeepEqual(spec.ClientCertAuth.Organizations, assertion.organizations) {
					t.Errorf("getKubeConfigSpecs for %s Organizations is %v, expected %v", assertion.kubeConfigFile, spec.ClientCertAuth.Organizations, assertion.organizations)
				}

				// 断言 EncryptionAlgorithm
				if spec.EncryptionAlgorithm != cfg.EncryptionAlgorithm {
					t.Errorf("getKubeConfigSpecs for %s EncryptionAlgorithm is %s, expected %s", assertion.kubeConfigFile, spec.EncryptionAlgorithm, cfg.EncryptionAlgorithm)
				}

				// 断言 InitConfiguration 的值已注入配置
				controlPlaneEndpoint, err := kubeadmutil.GetControlPlaneEndpoint(cfg.ControlPlaneEndpoint, &cfg.LocalAPIEndpoint)
				if err != nil {
					t.Error(err)
				}
				localAPIEndpoint, err := kubeadmutil.GetLocalAPIEndpoint(&cfg.LocalAPIEndpoint)
				if err != nil {
					t.Error(err)
				}

				switch assertion.kubeConfigFile {
				case kubeadmconstants.AdminKubeConfigFileName, kubeadmconstants.SuperAdminKubeConfigFileName, kubeadmconstants.KubeletKubeConfigFileName:
					if spec.APIServer != controlPlaneEndpoint {
						t.Errorf("expected getKubeConfigSpecs for %s to set cfg.APIServer to %s, got %s",
							assertion.kubeConfigFile, controlPlaneEndpoint, spec.APIServer)
					}
				case kubeadmconstants.ControllerManagerKubeConfigFileName, kubeadmconstants.SchedulerKubeConfigFileName:
					if spec.APIServer != localAPIEndpoint {
						t.Errorf("expected getKubeConfigSpecs for %s to set cfg.APIServer to %s, got %s",
							assertion.kubeConfigFile, localAPIEndpoint, spec.APIServer)
					}
				}

				// 断言 CA 证书和私钥已加载到配置中
				if spec.CACert == nil {
					t.Errorf("getKubeConfigSpecs didn't loaded CACert into spec for %s!", assertion.kubeConfigFile)
				}
				if spec.ClientCertAuth == nil || spec.ClientCertAuth.CAKey == nil {
					t.Errorf("getKubeConfigSpecs didn't loaded CAKey into spec for %s!", assertion.kubeConfigFile)
				}
			})
		}
	}
}

func TestBuildKubeConfigFromSpecWithClientAuth(t *testing.T) {
	// 创建 CA
	caCert, caKey := certstestutil.SetupCertificateAuthority(t)

	notAfter, _ := time.Parse(time.RFC3339, "2026-01-02T15:04:05Z")

	// 执行 buildKubeConfigFromSpec，传入含 ClientAuth 的 KubeConfigSpec
	config := setupKubeConfigWithClientAuth(t, caCert, caKey, notAfter, "https://1.2.3.4:1234", "myClientName", "test-cluster", "myOrg1", "myOrg2")

	// 断言配置数据已传递到 kubeconfig
	kubeconfigutil.AssertKubeConfigCurrentCluster(t, config, "https://1.2.3.4:1234", caCert)
	kubeconfigutil.AssertKubeConfigCurrentAuthInfoWithClientCert(t, config, caCert, notAfter, "myClientName", "myOrg1", "myOrg2")
}

func TestBuildKubeConfigFromSpecWithTokenAuth(t *testing.T) {
	// 创建 CA
	caCert, _ := certstestutil.SetupCertificateAuthority(t)

	// 执行 buildKubeConfigFromSpec，传入含 Token 的 KubeConfigSpec
	config := setupKubeConfigWithTokenAuth(t, caCert, "https://1.2.3.4:1234", "myClientName", "123456", "test-cluster")

	// 断言配置数据已传递到 kubeconfig
	kubeconfigutil.AssertKubeConfigCurrentCluster(t, config, "https://1.2.3.4:1234", caCert)
	kubeconfigutil.AssertKubeConfigCurrentAuthInfoWithToken(t, config, "myClientName", "123456")
}

func TestCreateKubeConfigFileIfNotExists(t *testing.T) {

	// 创建多个 CA
	caCert, caKey := certstestutil.SetupCertificateAuthority(t)
	anotherCaCert, anotherCaKey := certstestutil.SetupCertificateAuthority(t)

	notAfter, _ := time.Parse(time.RFC3339, "2026-01-02T15:04:05Z")

	// 构建 kubeconfig（用于测试内容是否相等）
	config := setupKubeConfigWithClientAuth(t, caCert, caKey, notAfter, "https://1.2.3.4:1234", "test-cluster", "myOrg1", "myOrg2")
	configWithAnotherClusterCa := setupKubeConfigWithClientAuth(t, anotherCaCert, anotherCaKey, notAfter, "https://1.2.3.4:1234", "test-cluster", "myOrg1", "myOrg2")
	configWithAnotherClusterAddress := setupKubeConfigWithClientAuth(t, caCert, caKey, notAfter, "https://3.4.5.6:3456", "myOrg1", "test-cluster", "myOrg2")
	invalidConfig := setupKubeConfigWithClientAuth(t, caCert, caKey, notAfter, "https://1.2.3.4:1234", "test-cluster", "myOrg1", "myOrg2")
	invalidConfig.CurrentContext = "invalid context"

	var tests = []struct {
		name               string
		existingKubeConfig *clientcmdapi.Config
		kubeConfig         *clientcmdapi.Config
		expectedError      bool
	}{
		{ // 如果不存在 KubeConfig，则创建
			name:       "KubeConfig doesn't exist",
			kubeConfig: config,
		},
		{ // 如果 KubeConfig 无效则返回错误
			name:               "KubeConfig is invalid",
			existingKubeConfig: invalidConfig,
			kubeConfig:         invalidConfig,
			expectedError:      true,
		},
		{ // 如果 KubeConfig 与现有文件内容一致（指向同一集群），复用现有文件（测试幂等性）
			name:               "KubeConfig refers to the same cluster",
			existingKubeConfig: config,
			kubeConfig:         config,
		},
		{ // 如果 KubeConfig 与现有文件不一致（指向另一个集群或使用另一个 CA），返回错误
			name:               "KubeConfig refers to the cluster with another CA",
			existingKubeConfig: config,
			kubeConfig:         configWithAnotherClusterCa,
			expectedError:      true,
		},
		{ // 如果 KubeConfig 与现有文件不一致，允许自定义服务器地址
			name:               "KubeConfig referst to the cluster with another address",
			existingKubeConfig: config,
			kubeConfig:         configWithAnotherClusterAddress,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// 为测试用例创建临时目录
			tmpdir := t.TempDir()

			// 将现有 kubeconfig 文件写入磁盘
			if test.existingKubeConfig != nil {
				if err := createKubeConfigFileIfNotExists(tmpdir, "test.conf", test.existingKubeConfig); err != nil {
					t.Errorf("createKubeConfigFileIfNotExists failed")
				}
			}

			// 将 kubeconfig 文件写入磁盘
			err := createKubeConfigFileIfNotExists(tmpdir, "test.conf", test.kubeConfig)
			if test.expectedError && err == nil {
				t.Errorf("createKubeConfigFileIfNotExists didn't failed when expected to fail")
			}
			if !test.expectedError && err != nil {
				t.Errorf("createKubeConfigFileIfNotExists failed")
			}

			// 断言文件已创建
			filesutil.AssertFileExists(t, tmpdir, "test.conf")
		})
	}
}

func TestCreateKubeconfigFilesAndWrappers(t *testing.T) {
	var tests = []struct {
		name                     string
		createKubeConfigFunction func(outDir string, cfg *kubeadmapi.InitConfiguration) error
		expectedFiles            []string
		expectedError            bool
	}{
		{ // 测试请求未知 kubeconfig 时 createKubeConfigFiles 失败
			name: "createKubeConfigFiles",
			createKubeConfigFunction: func(outDir string, cfg *kubeadmapi.InitConfiguration) error {
				return createKubeConfigFiles(outDir, cfg, "unknown.conf")
			},
			expectedError: true,
		},
		{ // 测试 CreateJoinControlPlaneKubeConfigFiles（createKubeConfigFile 的包装函数）
			name:                     "CreateJoinControlPlaneKubeConfigFiles",
			createKubeConfigFunction: CreateJoinControlPlaneKubeConfigFiles,
			expectedFiles: []string{
				kubeadmconstants.AdminKubeConfigFileName,
				kubeadmconstants.ControllerManagerKubeConfigFileName,
				kubeadmconstants.SchedulerKubeConfigFileName,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// 为测试用例创建临时目录
			tmpdir := t.TempDir()

			// 在临时目录中添加含 CA 证书的 pki 目录
			pkidir := certstestutil.SetupPkiDirWithCertificateAuthority(t, tmpdir)

			// 创建指向 pkidir 目录的 InitConfiguration
			cfg := &kubeadmapi.InitConfiguration{
				LocalAPIEndpoint: kubeadmapi.APIEndpoint{AdvertiseAddress: "1.2.3.4", BindPort: 1234},
				ClusterConfiguration: kubeadmapi.ClusterConfiguration{
					CertificatesDir: pkidir,
				},
			}

			// 执行 createKubeConfigFunction
			err := test.createKubeConfigFunction(tmpdir, cfg)
			if test.expectedError && err == nil {
				t.Errorf("createKubeConfigFunction didn't failed when expected to fail")
				return
			}
			if !test.expectedError && err != nil {
				t.Errorf("createKubeConfigFunction failed")
				return
			}

			// 断言预期文件均存在
			filesutil.AssertFileExists(t, tmpdir, test.expectedFiles...)
		})
	}
}

func TestWriteKubeConfigFailsIfCADoesntExists(t *testing.T) {
	// 测试用例的临时目录（不含 CA）
	tmpdir := t.TempDir()

	// 创建指向 tmpdir 目录的 InitConfiguration
	cfg := &kubeadmapi.InitConfiguration{
		ClusterConfiguration: kubeadmapi.ClusterConfiguration{
			CertificatesDir: tmpdir,
		},
	}

	notAfter, _ := time.Parse(time.RFC3339, "2026-01-02T15:04:05Z")

	var tests = []struct {
		name                    string
		writeKubeConfigFunction func(out io.Writer) error
	}{
		{
			name: "WriteKubeConfigWithClientCert",
			writeKubeConfigFunction: func(out io.Writer) error {
				return WriteKubeConfigWithClientCert(out, cfg, "myUser", []string{"myOrg"}, notAfter)
			},
		},
		{
			name: "WriteKubeConfigWithToken",
			writeKubeConfigFunction: func(out io.Writer) error {
				return WriteKubeConfigWithToken(out, cfg, "myUser", "12345", notAfter)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			buf := new(bytes.Buffer)

			// 执行 writeKubeConfigFunction
			if err := test.writeKubeConfigFunction(buf); err == nil {
				t.Error("writeKubeConfigFunction didnt failed when expected")
			}
		})
	}
}

func TestWriteKubeConfig(t *testing.T) {
	// 测试用例的临时目录
	tmpdir := t.TempDir()

	// 在临时目录中添加含 CA 证书的 pki 目录
	pkidir := certstestutil.SetupPkiDirWithCertificateAuthority(t, tmpdir)

	// 获取用于断言的 CA 证书
	caCert, _, err := pkiutil.TryLoadCertAndKeyFromDisk(pkidir, kubeadmconstants.CACertAndKeyBaseName)
	if err != nil {
		t.Fatalf("couldn't retrieve ca cert: %v", err)
	}

	// 创建指向 pkidir 目录的 InitConfiguration
	cfg := &kubeadmapi.InitConfiguration{
		LocalAPIEndpoint: kubeadmapi.APIEndpoint{AdvertiseAddress: "1.2.3.4", BindPort: 1234},
		ClusterConfiguration: kubeadmapi.ClusterConfiguration{
			CertificatesDir: pkidir,
			CertificateValidityPeriod: &metav1.Duration{
				Duration: time.Hour * 10,
			},
		},
	}

	notAfter, _ := time.Parse(time.RFC3339, "2026-01-02T15:04:05Z")

	var tests = []struct {
		name                    string
		writeKubeConfigFunction func(out io.Writer) error
		withClientCert          bool
		withToken               bool
	}{
		{
			name: "WriteKubeConfigWithClientCert",
			writeKubeConfigFunction: func(out io.Writer) error {
				return WriteKubeConfigWithClientCert(out, cfg, "myUser", []string{"myOrg"}, notAfter)
			},
			withClientCert: true,
		},
		{
			name: "WriteKubeConfigWithToken",
			writeKubeConfigFunction: func(out io.Writer) error {
				return WriteKubeConfigWithToken(out, cfg, "myUser", "12345", notAfter)
			},
			withToken: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			buf := new(bytes.Buffer)

			// 执行 writeKubeConfigFunction
			if err := test.writeKubeConfigFunction(buf); err != nil {
				t.Error("writeKubeConfigFunction failed")
				return
			}

			// 读取写入标准输出的 kubeconfig
			config, err := clientcmd.Load(buf.Bytes())
			if err != nil {
				t.Errorf("Couldn't read kubeconfig file from buffer: %v", err)
				return
			}

			// 检查 CLI 参数是否正确传递
			kubeconfigutil.AssertKubeConfigCurrentCluster(t, config, "https://1.2.3.4:1234", caCert)

			if test.withClientCert {
				// 检查 kubeconfig 文件是否包含预期客户端证书
				kubeconfigutil.AssertKubeConfigCurrentAuthInfoWithClientCert(t, config, caCert, notAfter, "myUser", "myOrg")
			}

			if test.withToken {
				// 检查 kubeconfig 文件是否包含预期令牌
				kubeconfigutil.AssertKubeConfigCurrentAuthInfoWithToken(t, config, "myUser", "12345")
			}
		})
	}
}

func TestValidateKubeConfig(t *testing.T) {
	caCert, caKey := certstestutil.SetupCertificateAuthority(t)
	anotherCaCert, anotherCaKey := certstestutil.SetupCertificateAuthority(t)

	notAfter, _ := time.Parse(time.RFC3339, "2026-01-02T15:04:05Z")

	config := setupKubeConfigWithClientAuth(t, caCert, caKey, notAfter, "https://1.2.3.4:1234", "test-cluster", "myOrg1")
	configWithAnotherClusterCa := setupKubeConfigWithClientAuth(t, anotherCaCert, anotherCaKey, notAfter, "https://1.2.3.4:1234", "test-cluster", "myOrg1")
	configWithAnotherServerURL := setupKubeConfigWithClientAuth(t, caCert, caKey, notAfter, "https://4.3.2.1:4321", "test-cluster", "myOrg1")

	configWithSameClusterCaByExternalFile := config.DeepCopy()
	currentCtx, exists := configWithSameClusterCaByExternalFile.Contexts[configWithSameClusterCaByExternalFile.CurrentContext]
	if !exists {
		t.Fatal("failed to find CurrentContext in Contexts of the kubeconfig")
	}
	if configWithSameClusterCaByExternalFile.Clusters[currentCtx.Cluster] == nil {
		t.Fatal("failed to find the given CurrentContext Cluster in Clusters of the kubeconfig")
	}
	tmpfile, err := os.CreateTemp("", "external-ca.crt")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	if _, err := tmpfile.Write(pkiutil.EncodeCertPEM(caCert)); err != nil {
		t.Fatal(err)
	}
	configWithSameClusterCaByExternalFile.Clusters[currentCtx.Cluster].CertificateAuthorityData = nil
	configWithSameClusterCaByExternalFile.Clusters[currentCtx.Cluster].CertificateAuthority = tmpfile.Name()

	// 创建 CA PEM 两端含空白的有效配置，validateKubeConfig() 应能容忍。
	configWhitespace := config.DeepCopy()
	configWhitespaceCtx := configWhitespace.Contexts[configWhitespace.CurrentContext]
	configWhitespaceCA := string(configWhitespace.Clusters[configWhitespaceCtx.Cluster].CertificateAuthorityData)
	configWhitespaceCA = "\n" + configWhitespaceCA + "\n"
	configWhitespace.Clusters[configWhitespaceCtx.Cluster].CertificateAuthorityData = []byte(configWhitespaceCA)

	tests := map[string]struct {
		existingKubeConfig *clientcmdapi.Config
		kubeConfig         *clientcmdapi.Config
		expectedError      bool
	}{
		"kubeconfig don't exist": {
			kubeConfig:    config,
			expectedError: true,
		},
		"kubeconfig exist and has invalid ca": {
			existingKubeConfig: configWithAnotherClusterCa,
			kubeConfig:         config,
			expectedError:      true,
		},
		"kubeconfig exist and has a different server url": {
			existingKubeConfig: configWithAnotherServerURL,
			kubeConfig:         config,
		},
		"kubeconfig exist and is valid": {
			existingKubeConfig: config,
			kubeConfig:         config,
			expectedError:      false,
		},
		"kubeconfig exist and is valid even if its CA contains whitespace": {
			existingKubeConfig: configWhitespace,
			kubeConfig:         config,
			expectedError:      false,
		},
		"kubeconfig exist and is valid even if its CA is provided as an external file": {
			existingKubeConfig: configWithSameClusterCaByExternalFile,
			kubeConfig:         config,
			expectedError:      false,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			tmpdir := t.TempDir()

			if test.existingKubeConfig != nil {
				if err := createKubeConfigFileIfNotExists(tmpdir, "test.conf", test.existingKubeConfig); err != nil {
					t.Errorf("createKubeConfigFileIfNotExists failed")
				}
			}

			err := validateKubeConfig(tmpdir, "test.conf", test.kubeConfig)
			if (err != nil) != test.expectedError {
				t.Fatalf(dedent.Dedent(
					"validateKubeConfig failed\n%s\nexpected error: %t\n\tgot: %t\nerror: %v"),
					name,
					test.expectedError,
					(err != nil),
					err,
				)
			}
		})
	}
}

func TestValidateKubeconfigsForExternalCA(t *testing.T) {
	tmpDir := t.TempDir()
	pkiDir := filepath.Join(tmpDir, "pki")

	initConfig := &kubeadmapi.InitConfiguration{
		ClusterConfiguration: kubeadmapi.ClusterConfiguration{
			CertificatesDir: pkiDir,
		},
		LocalAPIEndpoint: kubeadmapi.APIEndpoint{
			BindPort:         1234,
			AdvertiseAddress: "1.2.3.4",
		},
	}

	// 创建 CA、写入 pkiDir 并删除 ca.key，以进入外部 CA 场景
	caCert, caKey := certstestutil.SetupCertificateAuthority(t)

	if err := pkiutil.WriteCertBundle(pkiDir, kubeadmconstants.CACertAndKeyBaseName, []*x509.Certificate{caCert}); err != nil {
		t.Fatalf("failure while saving CA certificate: %v", err)
	}

	notAfter, _ := time.Parse(time.RFC3339, "2026-01-02T15:04:05Z")

	// 创建有效配置
	config := setupKubeConfigWithClientAuth(t, caCert, caKey, notAfter, "https://1.2.3.4:1234", "test-cluster", "myOrg1")

	// 创建使用另一个 CA 的配置
	anotherCaCert, anotherCaKey := certstestutil.SetupCertificateAuthority(t)
	configWithAnotherClusterCa := setupKubeConfigWithClientAuth(t, anotherCaCert, anotherCaKey, notAfter, "https://1.2.3.4:1234", "test-cluster", "myOrg1")

	// 创建使用另一个服务器 URL 的配置
	configWithAnotherServerURL := setupKubeConfigWithClientAuth(t, caCert, caKey, notAfter, "https://4.3.2.1:4321", "test-cluster", "myOrg1")

	tests := map[string]struct {
		filesToWrite  map[string]*clientcmdapi.Config
		initConfig    *kubeadmapi.InitConfiguration
		expectedError bool
	}{
		"files don't exist": {
			initConfig:    initConfig,
			expectedError: true,
		},
		"some files don't exist": {
			filesToWrite: map[string]*clientcmdapi.Config{
				kubeadmconstants.AdminKubeConfigFileName:      config,
				kubeadmconstants.SuperAdminKubeConfigFileName: config,
				kubeadmconstants.KubeletKubeConfigFileName:    config,
			},
			initConfig:    initConfig,
			expectedError: true,
		},
		"some files have invalid CA": {
			filesToWrite: map[string]*clientcmdapi.Config{
				kubeadmconstants.AdminKubeConfigFileName:             config,
				kubeadmconstants.SuperAdminKubeConfigFileName:        config,
				kubeadmconstants.KubeletKubeConfigFileName:           config,
				kubeadmconstants.ControllerManagerKubeConfigFileName: configWithAnotherClusterCa,
				kubeadmconstants.SchedulerKubeConfigFileName:         config,
			},
			initConfig:    initConfig,
			expectedError: true,
		},
		"some files have a different Server URL": {
			filesToWrite: map[string]*clientcmdapi.Config{
				kubeadmconstants.AdminKubeConfigFileName:             config,
				kubeadmconstants.SuperAdminKubeConfigFileName:        config,
				kubeadmconstants.KubeletKubeConfigFileName:           config,
				kubeadmconstants.ControllerManagerKubeConfigFileName: config,
				kubeadmconstants.SchedulerKubeConfigFileName:         configWithAnotherServerURL,
			},
			initConfig: initConfig,
		},
		"all files are valid": {
			filesToWrite: map[string]*clientcmdapi.Config{
				kubeadmconstants.AdminKubeConfigFileName:             config,
				kubeadmconstants.SuperAdminKubeConfigFileName:        config,
				kubeadmconstants.KubeletKubeConfigFileName:           config,
				kubeadmconstants.ControllerManagerKubeConfigFileName: config,
				kubeadmconstants.SchedulerKubeConfigFileName:         config,
			},
			initConfig:    initConfig,
			expectedError: false,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			tmpdir := t.TempDir()

			for name, config := range test.filesToWrite {
				if err := createKubeConfigFileIfNotExists(tmpdir, name, config); err != nil {
					t.Errorf("createKubeConfigFileIfNotExists failed: %v", err)
				}
			}

			err := ValidateKubeconfigsForExternalCA(tmpdir, test.initConfig)
			if (err != nil) != test.expectedError {
				t.Fatalf(dedent.Dedent(
					"ValidateKubeconfigsForExternalCA failed\n%s\nexpected error: %t\n\tgot: %t\nerror: %v"),
					name,
					test.expectedError,
					(err != nil),
					err,
				)
			}
		})
	}
}

func TestValidateKubeconfigsForExternalCAMissingRoot(t *testing.T) {
	tmpDir := t.TempDir()
	pkiDir := filepath.Join(tmpDir, "pki")

	initConfig := &kubeadmapi.InitConfiguration{
		ClusterConfiguration: kubeadmapi.ClusterConfiguration{
			CertificatesDir: pkiDir,
		},
		LocalAPIEndpoint: kubeadmapi.APIEndpoint{
			BindPort:         1234,
			AdvertiseAddress: "1.2.3.4",
		},
	}

	// 创建 CA、写入 pkiDir 并删除 ca.key，以进入外部 CA 模式
	caCert, caKey := certstestutil.SetupCertificateAuthority(t)

	// 为测试设置多个中间 CA。
	// 证书链为 "Root CA" 签发 "Intermediate Authority 1A"，再签发 "Intermediate Authority 2A"。
	intermediateCACert1a, intermediateCAKey1a := certstestutil.SetupIntermediateCertificateAuthority(t, caCert, caKey, "Intermediate Authority 1A")
	intermediateCACert2a, intermediateCAKey2a := certstestutil.SetupIntermediateCertificateAuthority(t, intermediateCACert1a, intermediateCAKey1a, "Intermediate Authority 1A")

	// 这两个 CA 证书都应能通过 Intermediate CA 2B 验证。
	// 证书链为 "Root CA" 签发 "Intermediate Authority 1B"，再签发 "Intermediate Authority 2B"。
	intermediateCACert1b, intermediateCAKey1b := certstestutil.SetupIntermediateCertificateAuthority(t, caCert, caKey, "Intermediate Authority 1B")
	intermediateCACert2b, intermediateCAKey2b := certstestutil.SetupIntermediateCertificateAuthority(t, intermediateCACert1b, intermediateCAKey1b, "Intermediate Authority 2B")

	notAfter, _ := time.Parse(time.RFC3339, "2036-01-02T15:04:05Z")
	clusterName := "myOrg1"

	var validCaCertBundle []*x509.Certificate
	validCaCertBundle = append(validCaCertBundle, caCert, intermediateCACert1a, intermediateCACert2a)
	multipleCAConfigRootCAIssuer := setupKubeConfigWithClientAuth(t, caCert, caKey, notAfter, "https://1.2.3.4:1234", "test-cluster", clusterName)
	multipleCAConfigIntermediateCA1aIssuer := setupKubeConfigWithClientAuth(t, intermediateCACert1a, intermediateCAKey1a, notAfter, "https://1.2.3.4:1234", "test-cluster", clusterName)
	multipleCAConfigIntermediateCA2aIssuer := setupKubeConfigWithClientAuth(t, intermediateCACert2a, intermediateCAKey2a, notAfter, "https://1.2.3.4:1234", "test-cluster", clusterName)

	var caBundleMissingRootCA []*x509.Certificate
	caBundleMissingRootCA = append(caBundleMissingRootCA, intermediateCACert1b, intermediateCACert2b)
	multipleCAConfigNoRootCA := setupKubeConfigWithClientAuth(t, intermediateCACert2b, intermediateCAKey2b, notAfter, "https://1.2.3.4:1234", "test-cluster", clusterName)
	multipleCAConfigDifferentIssuer := setupKubeConfigWithClientAuth(t, intermediateCACert2a, intermediateCAKey2a, notAfter, "https://1.2.3.4:1234", "test-cluster", clusterName)

	var caBundlePartialChain []*x509.Certificate
	caBundlePartialChain = append(caBundlePartialChain, intermediateCACert1a)
	multipleCaPartialCA := setupKubeConfigWithClientAuth(t, intermediateCACert2b, intermediateCAKey2b, notAfter, "https://1.2.3.4:1234", "test-cluster", clusterName)

	tests := map[string]struct {
		filesToWrite  map[string]*clientcmdapi.Config
		initConfig    *kubeadmapi.InitConfiguration
		expectedError bool
		caCertificate []*x509.Certificate
	}{
		// 正向测试用例
		"valid config issued from RootCA": {
			filesToWrite: map[string]*clientcmdapi.Config{
				kubeadmconstants.AdminKubeConfigFileName:             multipleCAConfigRootCAIssuer,
				kubeadmconstants.SuperAdminKubeConfigFileName:        multipleCAConfigRootCAIssuer,
				kubeadmconstants.KubeletKubeConfigFileName:           multipleCAConfigRootCAIssuer,
				kubeadmconstants.ControllerManagerKubeConfigFileName: multipleCAConfigRootCAIssuer,
				kubeadmconstants.SchedulerKubeConfigFileName:         multipleCAConfigRootCAIssuer,
			},
			caCertificate: validCaCertBundle,
			initConfig:    initConfig,
			expectedError: false,
		},
		"valid config issued from IntermediateCA 1A": {
			filesToWrite: map[string]*clientcmdapi.Config{
				kubeadmconstants.AdminKubeConfigFileName:             multipleCAConfigIntermediateCA1aIssuer,
				kubeadmconstants.SuperAdminKubeConfigFileName:        multipleCAConfigIntermediateCA1aIssuer,
				kubeadmconstants.KubeletKubeConfigFileName:           multipleCAConfigIntermediateCA1aIssuer,
				kubeadmconstants.ControllerManagerKubeConfigFileName: multipleCAConfigIntermediateCA1aIssuer,
				kubeadmconstants.SchedulerKubeConfigFileName:         multipleCAConfigIntermediateCA1aIssuer,
			},
			caCertificate: validCaCertBundle,
			initConfig:    initConfig,
			expectedError: false,
		},
		"valid config issued from IntermediateCA 2A": {
			filesToWrite: map[string]*clientcmdapi.Config{
				kubeadmconstants.AdminKubeConfigFileName:             multipleCAConfigIntermediateCA2aIssuer,
				kubeadmconstants.SuperAdminKubeConfigFileName:        multipleCAConfigIntermediateCA2aIssuer,
				kubeadmconstants.KubeletKubeConfigFileName:           multipleCAConfigIntermediateCA2aIssuer,
				kubeadmconstants.ControllerManagerKubeConfigFileName: multipleCAConfigIntermediateCA2aIssuer,
				kubeadmconstants.SchedulerKubeConfigFileName:         multipleCAConfigIntermediateCA2aIssuer,
			},
			caCertificate: validCaCertBundle,
			initConfig:    initConfig,
			expectedError: false,
		},
		"valid config issued from IntermediateCA 2B, CA missing root certificate": {
			filesToWrite: map[string]*clientcmdapi.Config{
				kubeadmconstants.AdminKubeConfigFileName:             multipleCAConfigNoRootCA,
				kubeadmconstants.SuperAdminKubeConfigFileName:        multipleCAConfigNoRootCA,
				kubeadmconstants.KubeletKubeConfigFileName:           multipleCAConfigNoRootCA,
				kubeadmconstants.ControllerManagerKubeConfigFileName: multipleCAConfigNoRootCA,
				kubeadmconstants.SchedulerKubeConfigFileName:         multipleCAConfigNoRootCA,
			},
			caCertificate: caBundleMissingRootCA,
			initConfig:    initConfig,
			expectedError: false,
		},
		// 负向测试用例
		"invalid config issued from IntermediateCA 2A, testing a chain with a different issuer": {
			filesToWrite: map[string]*clientcmdapi.Config{
				kubeadmconstants.AdminKubeConfigFileName:             multipleCAConfigDifferentIssuer,
				kubeadmconstants.SuperAdminKubeConfigFileName:        multipleCAConfigDifferentIssuer,
				kubeadmconstants.KubeletKubeConfigFileName:           multipleCAConfigDifferentIssuer,
				kubeadmconstants.ControllerManagerKubeConfigFileName: multipleCAConfigDifferentIssuer,
				kubeadmconstants.SchedulerKubeConfigFileName:         multipleCAConfigDifferentIssuer,
			},
			caCertificate: caBundleMissingRootCA,
			initConfig:    initConfig,
			expectedError: true,
		},
		"invalid config issued from IntermediateCA 2B chain, CA only contains Intermediate 1A": {
			filesToWrite: map[string]*clientcmdapi.Config{
				kubeadmconstants.AdminKubeConfigFileName:             multipleCaPartialCA,
				kubeadmconstants.SuperAdminKubeConfigFileName:        multipleCaPartialCA,
				kubeadmconstants.KubeletKubeConfigFileName:           multipleCaPartialCA,
				kubeadmconstants.ControllerManagerKubeConfigFileName: multipleCaPartialCA,
				kubeadmconstants.SchedulerKubeConfigFileName:         multipleCaPartialCA,
			},
			caCertificate: caBundlePartialChain,
			initConfig:    initConfig,
			expectedError: true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			tmpdir := t.TempDir()

			for name, config := range test.filesToWrite {
				if err := createKubeConfigFileIfNotExists(tmpdir, name, config); err != nil {
					t.Errorf("createKubeConfigFileIfNotExists failed: %v", err)
				}
			}

			if err := pkiutil.WriteCertBundle(pkiDir, kubeadmconstants.CACertAndKeyBaseName, test.caCertificate); err != nil {
				t.Fatalf("Failure while saving CA certificate: %v", err)
			}

			err := ValidateKubeconfigsForExternalCA(tmpdir, test.initConfig)
			if (err != nil) != test.expectedError {
				t.Fatalf("ValidateKubeconfigsForExternalCA failed\n%s\nexpected error: %t\n\tgot: %t\nerror: %v",
					name, test.expectedError, (err != nil), err)
			}
		})
	}
}

// setupKubeConfigWithClientAuth 是测试工具函数，封装 buildKubeConfigFromSpec 以构建使用 ClientAuth 的 KubeConfig。
func setupKubeConfigWithClientAuth(t *testing.T, caCert *x509.Certificate, caKey crypto.Signer, notAfter time.Time, apiServer, clientName, clustername string, organizations ...string) *clientcmdapi.Config {
	spec := &kubeConfigSpec{
		CACert:     caCert,
		APIServer:  apiServer,
		ClientName: clientName,
		ClientCertAuth: &clientCertAuth{
			CAKey:         caKey,
			Organizations: organizations,
		},
		ClientCertNotAfter: notAfter,
	}

	config, err := buildKubeConfigFromSpec(spec, clustername)
	if err != nil {
		t.Fatal("buildKubeConfigFromSpec failed!")
	}

	return config
}

// setupKubeConfigWithTokenAuth 是测试工具函数，封装 buildKubeConfigFromSpec 以构建使用 Token 的 KubeConfig。
func setupKubeConfigWithTokenAuth(t *testing.T, caCert *x509.Certificate, apiServer, clientName, token, clustername string) *clientcmdapi.Config {
	spec := &kubeConfigSpec{
		CACert:     caCert,
		APIServer:  apiServer,
		ClientName: clientName,
		TokenAuth: &tokenAuth{
			Token: token,
		},
	}

	config, err := buildKubeConfigFromSpec(spec, clustername)
	if err != nil {
		t.Fatal("buildKubeConfigFromSpec failed!")
	}

	return config
}

func TestEnsureAdminClusterRoleBinding(t *testing.T) {
	dir := t.TempDir()

	cfg := configutil.GetDefaultInternalConfig(t)
	cfg.CertificatesDir = dir

	ca := certsphase.KubeadmCertRootCA()
	_, _, err := ca.CreateAsCA(cfg)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name                  string
		expectedRBACError     bool
		expectedError         bool
		missingAdminConf      bool
		missingSuperAdminConf bool
	}{
		{
			name: "no errors",
		},
		{
			name:              "expect RBAC error",
			expectedRBACError: true,
			expectedError:     true,
		},
		{
			name:             "admin.conf is missing",
			missingAdminConf: true,
			expectedError:    true,
		},
		{
			name:                  "super-admin.conf is missing",
			missingSuperAdminConf: true,
			expectedError:         false, // 此文件为可选文件。
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ensureRBACFunc := func(_ context.Context, adminClient clientset.Interface, superAdminClient clientset.Interface,
				_ time.Duration, _ time.Duration) (clientset.Interface, error) {

				if tc.expectedRBACError {
					return nil, errors.New("ensureRBACFunc error")
				}
				return adminClient, nil
			}

			// 创建 admin.conf 和 super-admin.conf，供 EnsureAdminClusterRoleBinding 从中创建客户端。
			os.Remove(filepath.Join(dir, kubeadmconstants.AdminKubeConfigFileName))
			if !tc.missingAdminConf {
				if err := CreateKubeConfigFile(kubeadmconstants.AdminKubeConfigFileName, dir, cfg); err != nil {
					t.Fatal(err)
				}
			}
			os.Remove(filepath.Join(dir, kubeadmconstants.SuperAdminKubeConfigFileName))
			if !tc.missingSuperAdminConf {
				if err := CreateKubeConfigFile(kubeadmconstants.SuperAdminKubeConfigFileName, dir, cfg); err != nil {
					t.Fatal(err)
				}
			}

			client, err := EnsureAdminClusterRoleBinding(dir, &kubeadmapi.APIEndpoint{}, ensureRBACFunc)
			if (err != nil) != tc.expectedError {
				t.Fatalf("expected error: %v, got: %v, error: %v", err != nil, tc.expectedError, err)
			}

			if err == nil && client == nil {
				t.Fatal("got nil client")
			}
		})
	}
}

func TestEnsureAdminClusterRoleBindingImpl(t *testing.T) {
	tests := []struct {
		name                  string
		setupAdminClient      func(*clientsetfake.Clientset)
		setupSuperAdminClient func(*clientsetfake.Clientset)
		expectedError         bool
	}{
		{
			name: "admin.conf: handle forbidden errors when the super-admin.conf client is nil",
			setupAdminClient: func(client *clientsetfake.Clientset) {
				client.PrependReactor("create", "clusterrolebindings", func(action clientgotesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewForbidden(
						schema.GroupResource{}, "name", errors.New(""))
				})
			},
			expectedError: true,
		},
		{
			// 对真实服务器执行 "create" 时，可能同时返回禁止访问错误和非 nil CRB
			name: "admin.conf: handle forbidden error and returned CRBs, when the super-admin.conf client is nil",
			setupAdminClient: func(client *clientsetfake.Clientset) {
				client.PrependReactor("create", "clusterrolebindings", func(action clientgotesting.Action) (bool, runtime.Object, error) {
					return true, &rbac.ClusterRoleBinding{}, apierrors.NewForbidden(
						schema.GroupResource{}, "name", errors.New(""))
				})
			},
			expectedError: true,
		},
		{
			name: "admin.conf: CRB already exists, use the admin.conf client",
			setupAdminClient: func(client *clientsetfake.Clientset) {
				client.PrependReactor("create", "clusterrolebindings", func(action clientgotesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewAlreadyExists(
						schema.GroupResource{}, "name")
				})
			},
			setupSuperAdminClient: func(client *clientsetfake.Clientset) {
				client.PrependReactor("create", "clusterrolebindings", func(action clientgotesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewAlreadyExists(
						schema.GroupResource{}, "name")
				})
			},
			expectedError: false,
		},
		{
			name: "admin.conf: handle other errors, such as a server timeout",
			setupAdminClient: func(client *clientsetfake.Clientset) {
				client.PrependReactor("create", "clusterrolebindings", func(action clientgotesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewServerTimeout(
						schema.GroupResource{}, "create", 0)
				})
			},
			expectedError: true,
		},
		{
			name: "admin.conf: CRB exists, return a client from admin.conf",
			setupAdminClient: func(client *clientsetfake.Clientset) {
				client.PrependReactor("create", "clusterrolebindings", func(action clientgotesting.Action) (bool, runtime.Object, error) {
					return true, &rbac.ClusterRoleBinding{}, nil
				})
			},
			expectedError: false,
		},
		{
			name: "super-admin.conf: error while creating CRB",
			setupAdminClient: func(client *clientsetfake.Clientset) {
				client.PrependReactor("create", "clusterrolebindings", func(action clientgotesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewForbidden(
						schema.GroupResource{}, "name", errors.New(""))
				})
			},
			setupSuperAdminClient: func(client *clientsetfake.Clientset) {
				client.PrependReactor("create", "clusterrolebindings", func(action clientgotesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewServerTimeout(
						schema.GroupResource{}, "create", 0)
				})
			},
			expectedError: true,
		},
		{
			name: "super-admin.conf: admin.conf cannot create CRB, create CRB with super-admin.conf, return client from admin.conf",
			setupAdminClient: func(client *clientsetfake.Clientset) {
				client.PrependReactor("create", "clusterrolebindings", func(action clientgotesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewForbidden(
						schema.GroupResource{}, "name", errors.New(""))
				})
			},
			setupSuperAdminClient: func(client *clientsetfake.Clientset) {
				client.PrependReactor("create", "clusterrolebindings", func(action clientgotesting.Action) (bool, runtime.Object, error) {
					return true, &rbac.ClusterRoleBinding{}, nil
				})
			},
			expectedError: false,
		},
		{
			name: "super-admin.conf: admin.conf cannot create CRB, try to create CRB with super-admin.conf, encounter 'already exists' error",
			setupAdminClient: func(client *clientsetfake.Clientset) {
				client.PrependReactor("create", "clusterrolebindings", func(action clientgotesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewForbidden(
						schema.GroupResource{}, "name", errors.New(""))
				})
			},
			setupSuperAdminClient: func(client *clientsetfake.Clientset) {
				client.PrependReactor("create", "clusterrolebindings", func(action clientgotesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewAlreadyExists(
						schema.GroupResource{}, "name")
				})
			},
			expectedError: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			adminClient := clientsetfake.NewSimpleClientset()
			tc.setupAdminClient(adminClient)

			var superAdminClient clientset.Interface // 确保 superAdminClient 默认为 nil
			if tc.setupSuperAdminClient != nil {
				fakeSuperAdminClient := clientsetfake.NewSimpleClientset()
				tc.setupSuperAdminClient(fakeSuperAdminClient)
				superAdminClient = fakeSuperAdminClient
			}

			client, err := EnsureAdminClusterRoleBindingImpl(
				context.Background(), adminClient, superAdminClient, 0, 0)
			if (err != nil) != tc.expectedError {
				t.Fatalf("expected error: %v, got %v, error: %v", tc.expectedError, err != nil, err)
			}

			if err == nil && client == nil {
				t.Fatal("got nil client")
			}
		})
	}
}

func TestCreateKubeConfigAndCSR(t *testing.T) {
	tmpDir := t.TempDir()
	filesutil.SetupEmptyFiles(t, tmpDir, "testfile", "bar.csr", "bar.key")
	caCert, caKey := certstestutil.SetupCertificateAuthority(t)

	type args struct {
		kubeConfigDir string
		kubeadmConfig *kubeadmapi.InitConfiguration
		name          string
		spec          *kubeConfigSpec
	}
	tests := []struct {
		name          string
		args          args
		expectedError bool
	}{
		{
			name: "kubeadmConfig is nil",
			args: args{
				kubeConfigDir: tmpDir,
				kubeadmConfig: nil,
				name:          "foo",
				spec: &kubeConfigSpec{
					CACert:         caCert,
					APIServer:      "10.0.0.1",
					ClientName:     "foo",
					TokenAuth:      &tokenAuth{Token: "test"},
					ClientCertAuth: &clientCertAuth{CAKey: caKey},
				},
			},
			expectedError: true,
		},
		{
			name: "The kubeConfigDir is empty",
			args: args{
				kubeConfigDir: "",
				kubeadmConfig: &kubeadmapi.InitConfiguration{},
				name:          "foo",
				spec: &kubeConfigSpec{
					CACert:         caCert,
					APIServer:      "10.0.0.1",
					ClientName:     "foo",
					TokenAuth:      &tokenAuth{Token: "test"},
					ClientCertAuth: &clientCertAuth{CAKey: caKey},
				},
			},
			expectedError: true,
		},
		{
			name: "The name is empty",
			args: args{
				kubeConfigDir: tmpDir,
				kubeadmConfig: &kubeadmapi.InitConfiguration{},
				name:          "",
				spec: &kubeConfigSpec{
					CACert:         caCert,
					APIServer:      "10.0.0.1",
					ClientName:     "foo",
					TokenAuth:      &tokenAuth{Token: "test"},
					ClientCertAuth: &clientCertAuth{CAKey: caKey},
				},
			},
			expectedError: true,
		},
		{
			name: "The spec is empty",
			args: args{
				kubeConfigDir: tmpDir,
				kubeadmConfig: &kubeadmapi.InitConfiguration{},
				name:          "foo",
				spec:          nil,
			},
			expectedError: true,
		},
		{
			name: "The kubeconfig file already exists",
			args: args{
				kubeConfigDir: tmpDir,
				kubeadmConfig: &kubeadmapi.InitConfiguration{},
				name:          "testfile",
				spec: &kubeConfigSpec{
					CACert:         caCert,
					APIServer:      "10.0.0.1",
					ClientName:     "foo",
					TokenAuth:      &tokenAuth{Token: "test"},
					ClientCertAuth: &clientCertAuth{CAKey: caKey},
				},
			},
			expectedError: true,
		},
		{
			name: "The CSR or key files already exists",
			args: args{
				kubeConfigDir: tmpDir,
				kubeadmConfig: &kubeadmapi.InitConfiguration{},
				name:          "bar",
				spec: &kubeConfigSpec{
					CACert:         caCert,
					APIServer:      "10.0.0.1",
					ClientName:     "foo",
					TokenAuth:      &tokenAuth{Token: "test"},
					ClientCertAuth: &clientCertAuth{CAKey: caKey},
				},
			},
			expectedError: true,
		},
		{
			name: "configuration is valid, expect no errors",
			args: args{
				kubeConfigDir: tmpDir,
				kubeadmConfig: &kubeadmapi.InitConfiguration{},
				name:          "test",
				spec: &kubeConfigSpec{
					CACert:         caCert,
					APIServer:      "10.0.0.1",
					ClientName:     "foo",
					TokenAuth:      &tokenAuth{Token: "test"},
					ClientCertAuth: &clientCertAuth{CAKey: caKey},
				},
			},
			expectedError: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := createKubeConfigAndCSR(tc.args.kubeConfigDir, tc.args.kubeadmConfig, tc.args.name, tc.args.spec); (err != nil) != tc.expectedError {
				t.Errorf("createKubeConfigAndCSR() error = %v, wantErr %v", err, tc.expectedError)
			}
		})
	}
}

func TestCreateDefaultKubeConfigsAndCSRFiles(t *testing.T) {
	tmpDir := t.TempDir()
	type args struct {
		kubeConfigDir string
		kubeadmConfig *kubeadmapi.InitConfiguration
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "kubeadmConfig is empty",
			args: args{
				kubeConfigDir: tmpDir,
				kubeadmConfig: &kubeadmapi.InitConfiguration{},
			},
			wantErr: true,
		},
		{
			name: "The APIEndpoint is invalid",
			args: args{
				kubeConfigDir: tmpDir,
				kubeadmConfig: &kubeadmapi.InitConfiguration{
					LocalAPIEndpoint: kubeadmapi.APIEndpoint{
						AdvertiseAddress: "x.12.FOo.1",
						BindPort:         6443,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "The APIEndpoint is valid",
			args: args{
				kubeConfigDir: tmpDir,
				kubeadmConfig: &kubeadmapi.InitConfiguration{
					LocalAPIEndpoint: kubeadmapi.APIEndpoint{
						AdvertiseAddress: "127.0.0.1",
						BindPort:         6443,
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := &bytes.Buffer{}
			if err := CreateDefaultKubeConfigsAndCSRFiles(out, tc.args.kubeConfigDir, tc.args.kubeadmConfig); (err != nil) != tc.wantErr {
				t.Errorf("CreateDefaultKubeConfigsAndCSRFiles() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
		})
	}
}
