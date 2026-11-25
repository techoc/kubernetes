/*
Copyright 2014 The Kubernetes Authors.

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

package clientcmd

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	goruntime "runtime"
	"strings"
	"testing"

	utiltesting "k8s.io/client-go/util/testing"

	"github.com/google/go-cmp/cmp"
	"sigs.k8s.io/yaml"

	"k8s.io/apimachinery/pkg/runtime"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	clientcmdlatest "k8s.io/client-go/tools/clientcmd/api/latest"
	"k8s.io/client-go/util/homedir"
	"k8s.io/klog/v2"
)

var (
	// 这些配置作为测试夹具：token 使用占位符，避免真实凭据进入仓库。
	testConfigAlfa = clientcmdapi.Config{
		AuthInfos: map[string]*clientcmdapi.AuthInfo{
			"red-user": {Token: "red-token"}},
		Clusters: map[string]*clientcmdapi.Cluster{
			"cow-cluster": {Server: "http://cow.org:8080"}},
		Contexts: map[string]*clientcmdapi.Context{
			"federal-context": {AuthInfo: "red-user", Cluster: "cow-cluster", Namespace: "hammer-ns"}},
	}
	testConfigBravo = clientcmdapi.Config{
		AuthInfos: map[string]*clientcmdapi.AuthInfo{
			"black-user": {Token: "black-token"}},
		Clusters: map[string]*clientcmdapi.Cluster{
			"pig-cluster": {Server: "http://pig.org:8080"}},
		Contexts: map[string]*clientcmdapi.Context{
			"queen-anne-context": {AuthInfo: "black-user", Cluster: "pig-cluster", Namespace: "saw-ns"}},
	}
	testConfigCharlie = clientcmdapi.Config{
		AuthInfos: map[string]*clientcmdapi.AuthInfo{
			"green-user": {Token: "green-token"}},
		Clusters: map[string]*clientcmdapi.Cluster{
			"horse-cluster": {Server: "http://horse.org:8080"}},
		Contexts: map[string]*clientcmdapi.Context{
			"shaker-context": {AuthInfo: "green-user", Cluster: "horse-cluster", Namespace: "chisel-ns"}},
	}
	testConfigDelta = clientcmdapi.Config{
		AuthInfos: map[string]*clientcmdapi.AuthInfo{
			"blue-user": {Token: "blue-token"}},
		Clusters: map[string]*clientcmdapi.Cluster{
			"chicken-cluster": {Server: "http://chicken.org:8080"}},
		Contexts: map[string]*clientcmdapi.Context{
			"gothic-context": {AuthInfo: "blue-user", Cluster: "chicken-cluster", Namespace: "plane-ns"}},
	}

	testConfigConflictAlfa = clientcmdapi.Config{
		AuthInfos: map[string]*clientcmdapi.AuthInfo{
			"red-user":    {Token: "a-different-red-token"},
			"yellow-user": {Token: "yellow-token"}},
		Clusters: map[string]*clientcmdapi.Cluster{
			"cow-cluster":    {Server: "http://a-different-cow.org:8080", InsecureSkipTLSVerify: true, DisableCompression: true},
			"donkey-cluster": {Server: "http://donkey.org:8080", InsecureSkipTLSVerify: true, DisableCompression: true}},
		CurrentContext: "federal-context",
	}
)

// TestNilOutMap 校验解码不含部分 map 的 kubeconfig 不会因 nil map 写入而失败。
func TestNilOutMap(t *testing.T) {
	var fakeKubeconfigData = `apiVersion: v1
kind: Config
clusters:
- cluster:
    certificate-authority-data: UEhPTlkK
    server: https://1.1.1.1
  name: production
contexts:
- context:
    cluster: production
    user: production
  name: production
current-context: production
users:
- name: production
  user:
    auth-provider:
      name: gcp`

	_, _, err := clientcmdlatest.Codec.Decode([]byte(fakeKubeconfigData), nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestNonExistentCommandLineFile 校验显式指定的文件不存在时报错，且错误信息包含路径。
func TestNonExistentCommandLineFile(t *testing.T) {
	loadingRules := ClientConfigLoadingRules{
		ExplicitPath: "bogus_file",
	}

	_, err := loadingRules.Load()
	if err == nil {
		t.Fatalf("Expected error for missing command-line file, got none")
	}
	if !strings.Contains(err.Error(), "bogus_file") {
		t.Fatalf("Expected error about 'bogus_file', got %s", err.Error())
	}
}

// TestToleratingMissingFiles 校验 WarnIfAllMissing 时缺失文件只告警、不阻断加载。
//
//nolint:logcheck // Tests klog APIs.
func TestToleratingMissingFiles(t *testing.T) {
	envVarValue := "bogus"
	loadingRules := ClientConfigLoadingRules{
		Precedence:       []string{"bogus1", "bogus2", "bogus3"},
		WarnIfAllMissing: true,
		Warner:           func(err error) { klog.Warning(err) },
	}

	buffer := &bytes.Buffer{}

	klog.LogToStderr(false)
	klog.SetOutput(buffer)

	_, err := loadingRules.Load()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	klog.Flush()
	expectedLog := fmt.Sprintf("Config not found: %s", envVarValue)
	if !strings.Contains(buffer.String(), expectedLog) {
		t.Fatalf("expected log: \"%s\"", expectedLog)
	}
}

// TestWarningMissingFiles 校验 v>=1 时会输出 “Config not found” 告警。
//
//nolint:logcheck // Tests klog APIs.
func TestWarningMissingFiles(t *testing.T) {
	envVarValue := "bogus"
	t.Setenv(RecommendedConfigPathEnvVar, envVarValue)
	loadingRules := NewDefaultClientConfigLoadingRules()

	buffer := &bytes.Buffer{}

	flags := &flag.FlagSet{}
	klog.InitFlags(flags)
	flags.Set("v", "1")
	klog.LogToStderr(false)
	klog.SetOutput(buffer)

	_, err := loadingRules.Load()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	klog.Flush()

	expectedLog := fmt.Sprintf("Config not found: %s", envVarValue)
	if !strings.Contains(buffer.String(), expectedLog) {
		t.Fatalf("expected log: \"%s\"", expectedLog)
	}
}

// TestNoWarningMissingFiles 校验默认 verbosity 下不输出缺失文件告警，避免污染普通用户日志。
//
//nolint:logcheck // Tests klog APIs.
func TestNoWarningMissingFiles(t *testing.T) {
	envVarValue := "bogus"
	t.Setenv(RecommendedConfigPathEnvVar, envVarValue)
	loadingRules := NewDefaultClientConfigLoadingRules()

	buffer := &bytes.Buffer{}

	flags := &flag.FlagSet{}
	klog.InitFlags(flags)
	flags.Set("v", "0")
	klog.LogToStderr(false)
	klog.SetOutput(buffer)

	_, err := loadingRules.Load()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	klog.Flush()

	logNotExpected := fmt.Sprintf("Config not found: %s", envVarValue)
	if strings.Contains(buffer.String(), logNotExpected) {
		t.Fatalf("log not expected: \"%s\"", logNotExpected)
	}
}

// TestErrorReadingFile 校验内容非法的 kubeconfig 会返回包含文件名的解析错误。
func TestErrorReadingFile(t *testing.T) {
	commandLineFile, _ := os.CreateTemp("", "")
	defer utiltesting.CloseAndRemove(t, commandLineFile)

	if err := os.WriteFile(commandLineFile.Name(), []byte("bogus value"), 0644); err != nil {
		t.Fatalf("Error creating tempfile: %v", err)
	}

	loadingRules := ClientConfigLoadingRules{
		ExplicitPath: commandLineFile.Name(),
	}

	_, err := loadingRules.Load()
	if err == nil {
		t.Fatalf("Expected error for unloadable file, got none")
	}
	if !strings.Contains(err.Error(), commandLineFile.Name()) {
		t.Fatalf("Expected error about '%s', got %s", commandLineFile.Name(), err.Error())
	}
}

// TestErrorReadingNonFile 校验把目录当作 kubeconfig 时会报错，而不是静默忽略。
func TestErrorReadingNonFile(t *testing.T) {
	tmpdir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatalf("Couldn't create tmpdir")
	}
	defer os.RemoveAll(tmpdir)

	loadingRules := ClientConfigLoadingRules{
		ExplicitPath: tmpdir,
	}

	_, err = loadingRules.Load()
	if err == nil {
		t.Fatalf("Expected error for non-file, got none")
	}
	if !strings.Contains(err.Error(), tmpdir) {
		t.Fatalf("Expected error about '%s', got %s", tmpdir, err.Error())
	}
}

// TestConflictingCurrentContext 校验显式指定的文件优先于环境变量文件，其 current-context 胜出。
func TestConflictingCurrentContext(t *testing.T) {
	commandLineFile, _ := os.CreateTemp("", "")
	envVarFile, _ := os.CreateTemp("", "")
	defer utiltesting.CloseAndRemove(t, commandLineFile, envVarFile)

	mockCommandLineConfig := clientcmdapi.Config{
		CurrentContext: "any-context-value",
	}
	mockEnvVarConfig := clientcmdapi.Config{
		CurrentContext: "a-different-context",
	}

	WriteToFile(mockCommandLineConfig, commandLineFile.Name())
	WriteToFile(mockEnvVarConfig, envVarFile.Name())

	loadingRules := ClientConfigLoadingRules{
		ExplicitPath: commandLineFile.Name(),
		Precedence:   []string{envVarFile.Name()},
	}

	mergedConfig, err := loadingRules.Load()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if mergedConfig.CurrentContext != mockCommandLineConfig.CurrentContext {
		t.Errorf("expected %v, got %v", mockCommandLineConfig.CurrentContext, mergedConfig.CurrentContext)
	}
}

// TestEncodeYAML 校验序列化输出的字段顺序与键排序稳定，便于 diff 与版本控制。
func TestEncodeYAML(t *testing.T) {
	config := clientcmdapi.Config{
		CurrentContext: "any-context-value",
		Contexts: map[string]*clientcmdapi.Context{
			"433e40": {
				Cluster: "433e40",
			},
		},
		Clusters: map[string]*clientcmdapi.Cluster{
			"0": {
				Server: "https://localhost:1234",
			},
			"1": {
				Server: "https://localhost:1234",
			},
			"433e40": {
				Server: "https://localhost:1234",
			},
		},
	}
	data, err := Write(config)
	if err != nil {
		t.Fatal(err)
	}
	expected := []byte(`apiVersion: v1
clusters:
- cluster:
    server: https://localhost:1234
  name: "0"
- cluster:
    server: https://localhost:1234
  name: "1"
- cluster:
    server: https://localhost:1234
  name: "433e40"
contexts:
- context:
    cluster: "433e40"
    user: ""
  name: "433e40"
current-context: any-context-value
kind: Config
users: null
`)
	if !bytes.Equal(expected, data) {
		t.Error(cmp.Diff(string(expected), string(data)))
	}
}

// TestLoadingEmptyMaps 校验空 kubeconfig 加载后 Clusters/AuthInfos/Contexts 均非 nil，
// 调用方因此可以直接写入而无需初始化 map。
func TestLoadingEmptyMaps(t *testing.T) {
	configFile, _ := os.CreateTemp("", "")
	defer utiltesting.CloseAndRemove(t, configFile)

	mockConfig := clientcmdapi.Config{
		CurrentContext: "any-context-value",
	}

	WriteToFile(mockConfig, configFile.Name())

	config, err := LoadFromFile(configFile.Name())
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if config.Clusters == nil {
		t.Error("expected config.Clusters to be non-nil")
	}
	if config.AuthInfos == nil {
		t.Error("expected config.AuthInfos to be non-nil")
	}
	if config.Contexts == nil {
		t.Error("expected config.Contexts to be non-nil")
	}
}

// TestDuplicateClusterName 校验列表转 map 时重名 cluster 会报错，避免静默覆盖。
func TestDuplicateClusterName(t *testing.T) {
	configFile, _ := os.CreateTemp("", "")
	defer utiltesting.CloseAndRemove(t, configFile)

	err := os.WriteFile(configFile.Name(), []byte(`
kind: Config
apiVersion: v1
clusters:
- cluster:
    api-version: v1
    server: https://kubernetes.default.svc:443
    certificate-authority: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
  name: kubeconfig-cluster
- cluster:
    api-version: v2
    server: https://test.example.server:443
    certificate-authority: /var/run/secrets/test.example.io/serviceaccount/ca.crt
  name: kubeconfig-cluster
contexts:
- context:
    cluster: kubeconfig-cluster
    namespace: default
    user: kubeconfig-user
  name: kubeconfig-context
current-context: kubeconfig-context
users:
- name: kubeconfig-user
  user:
    tokenFile: /var/run/secrets/kubernetes.io/serviceaccount/token
`), os.FileMode(0755))

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	_, err = LoadFromFile(configFile.Name())
	if err == nil || !strings.Contains(err.Error(),
		"error converting *[]NamedCluster into *map[string]*api.Cluster: duplicate name \"kubeconfig-cluster\" in list") {
		t.Error("Expected error in loading duplicate cluster name, got none")
	}
}

// TestDuplicateContextName 校验重名 context 会报错。
func TestDuplicateContextName(t *testing.T) {
	configFile, _ := os.CreateTemp("", "")
	defer utiltesting.CloseAndRemove(t, configFile)

	err := os.WriteFile(configFile.Name(), []byte(`
kind: Config
apiVersion: v1
clusters:
- cluster:
    api-version: v1
    server: https://kubernetes.default.svc:443
    certificate-authority: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
  name: kubeconfig-cluster
contexts:
- context:
    cluster: kubeconfig-cluster
    namespace: default
    user: kubeconfig-user
  name: kubeconfig-context
- context:
    cluster: test-example-cluster
    namespace: test-example
    user: test-example-user
  name: kubeconfig-context
current-context: kubeconfig-context
users:
- name: kubeconfig-user
  user:
    tokenFile: /var/run/secrets/kubernetes.io/serviceaccount/token
`), os.FileMode(0755))

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	_, err = LoadFromFile(configFile.Name())
	if err == nil || !strings.Contains(err.Error(),
		"error converting *[]NamedContext into *map[string]*api.Context: duplicate name \"kubeconfig-context\" in list") {
		t.Error("Expected error in loading duplicate context name, got none")
	}
}

// TestDuplicateUserName 校验重名 user 会报错。
func TestDuplicateUserName(t *testing.T) {
	configFile, _ := os.CreateTemp("", "")
	defer utiltesting.CloseAndRemove(t, configFile)

	err := os.WriteFile(configFile.Name(), []byte(`
kind: Config
apiVersion: v1
clusters:
- cluster:
    api-version: v1
    server: https://kubernetes.default.svc:443
    certificate-authority: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
  name: kubeconfig-cluster
contexts:
- context:
    cluster: kubeconfig-cluster
    namespace: default
    user: kubeconfig-user
  name: kubeconfig-context
current-context: kubeconfig-context
users:
- name: kubeconfig-user
  user:
    tokenFile: /var/run/secrets/kubernetes.io/serviceaccount/token
- name: kubeconfig-user
  user:
    tokenFile: /var/run/secrets/test.example.com/serviceaccount/token
`), os.FileMode(0755))

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	_, err = LoadFromFile(configFile.Name())
	if err == nil || !strings.Contains(err.Error(),
		"error converting *[]NamedAuthInfo into *map[string]*api.AuthInfo: duplicate name \"kubeconfig-user\" in list") {
		t.Error("Expected error in loading duplicate user name, got none")
	}
}

// TestDuplicateExtensionName 校验重名 extension 会报错。
func TestDuplicateExtensionName(t *testing.T) {
	configFile, _ := os.CreateTemp("", "")
	defer utiltesting.CloseAndRemove(t, configFile)

	err := os.WriteFile(configFile.Name(), []byte(`
kind: Config
apiVersion: v1
clusters:
- cluster:
    api-version: v1
    server: https://kubernetes.default.svc:443
    certificate-authority: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
  name: kubeconfig-cluster
contexts:
- context:
    cluster: kubeconfig-cluster
    namespace: default
    user: kubeconfig-user
  name: kubeconfig-context
current-context: kubeconfig-context
users:
- name: kubeconfig-user
  user:
    tokenFile: /var/run/secrets/kubernetes.io/serviceaccount/token
extensions:
- extension:
    bytes: test
  name: test-extension
- extension:
    bytes: some-example
  name: test-extension
`), os.FileMode(0755))

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	_, err = LoadFromFile(configFile.Name())
	if err == nil || !strings.Contains(err.Error(),
		"error converting *[]NamedExtension into *map[string]runtime.Object: duplicate name \"test-extension\" in list") {
		t.Error("Expected error in loading duplicate extension name, got none")
	}
}

// TestRelativizePathWithNoBacksteps 校验路径相对化规则：
// 仅当目标位于 base 之下才转相对路径；需要 “..” 回退或本身是绝对路径时保持原样。
func TestRelativizePathWithNoBacksteps(t *testing.T) {
	var root string
	var base string
	var otherBase string
	if goruntime.GOOS == "windows" {
		root = "C:"
		base = root + "\\base"
		otherBase = root + "\\" + "other"
	} else {
		root = ""
		base = "/base"
		otherBase = "/other"
	}

	tests := []struct {
		base          string
		name          string
		inputPaths    []string
		expectedPaths []string
		expectError   bool
	}{
		{
			base:          root + "/base",
			name:          "path in subdirectory with slashes - should relativize",
			inputPaths:    []string{root + "/base/subdir/file.txt"},
			expectedPaths: []string{filepath.Join("subdir", "file.txt")},
			expectError:   false,
		},
		{
			base:          base,
			name:          "path in subdirectory - should relativize",
			inputPaths:    []string{filepath.Join(base, "subdir", "file.txt")},
			expectedPaths: []string{filepath.Join("subdir", "file.txt")},
			expectError:   false,
		},
		{
			base:          base,
			name:          "path in same directory - should relativize",
			inputPaths:    []string{filepath.Join(base, "file.txt")},
			expectedPaths: []string{"file.txt"},
			expectError:   false,
		},
		{
			base:          root + "/base",
			name:          "absolute path requiring backsteps with slashes - should remain absolute",
			inputPaths:    []string{root + "/other/dir/file.txt"},
			expectedPaths: []string{root + "/other/dir/file.txt"},
			expectError:   false,
		},
		{
			base:          base,
			name:          "absolute path requiring backsteps - should remain absolute",
			inputPaths:    []string{filepath.Join(otherBase, "dir", "file.txt")},
			expectedPaths: []string{filepath.Join(otherBase, "dir", "file.txt")},
			expectError:   false,
		},
		{
			base:          base,
			name:          "relative path requiring backsteps - should error",
			inputPaths:    []string{filepath.Join("..", "other", "file.txt")},
			expectedPaths: []string{filepath.Join("..", "other", "file.txt")},
			expectError:   true,
		},
		{
			base:          base,
			name:          "empty path - should remain empty",
			inputPaths:    []string{""},
			expectedPaths: []string{""},
			expectError:   false,
		},
		{
			base:          base,
			name:          "multiple paths no backsteps - should relativize all",
			inputPaths:    []string{filepath.Join(base, "file1.txt"), filepath.Join(base, "subdir", "file2.txt")},
			expectedPaths: []string{"file1.txt", filepath.Join("subdir", "file2.txt")},
			expectError:   false,
		},
		{
			base:          base,
			name:          "mixed paths with absolute requiring backsteps - should keep absolute unchanged",
			inputPaths:    []string{filepath.Join(base, "file1.txt"), filepath.Join(otherBase, "file2.txt"), filepath.Join(base, "subdir", "file3.txt")},
			expectedPaths: []string{"file1.txt", filepath.Join(otherBase, "file2.txt"), filepath.Join("subdir", "file3.txt")},
			expectError:   false,
		},
		{
			base:          base,
			name:          "mixed with empty paths",
			inputPaths:    []string{filepath.Join(base, "file1.txt"), "", filepath.Join(base, "subdir", "file2.txt")},
			expectedPaths: []string{"file1.txt", "", filepath.Join("subdir", "file2.txt")},
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 为输入字符串创建指针
			refs := make([]*string, len(tt.inputPaths))
			for i := range tt.inputPaths {
				s := tt.inputPaths[i]
				refs[i] = &s
			}

			// 调用被测函数
			err := RelativizePathWithNoBacksteps(refs, tt.base)

			// 检查是否符合预期的错误
			if tt.expectError && err == nil {
				t.Errorf("expected an error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			// 若不期望报错，则检查结果
			if !tt.expectError {
				for i, ref := range refs {
					if *ref != tt.expectedPaths[i] {
						t.Errorf("path %d: expected %q, got %q", i, tt.expectedPaths[i], *ref)
					}
				}
			}
		})
	}
}

// TestResolveRelativePaths 校验相对路径按“该配置项所在文件”的目录解析，
// 而不是按第一个或当前工作目录解析；exec 命令相对路径同样处理，PATH 中的裸命令保持不变。
func TestResolveRelativePaths(t *testing.T) {
	absoluteDir := filepath.Join(t.TempDir(), "absolute")
	pathResolutionConfig1 := clientcmdapi.Config{
		AuthInfos: map[string]*clientcmdapi.AuthInfo{
			"relative-user-1": {ClientCertificate: filepath.Join("relative", "client", "cert"), ClientKey: filepath.Join("..", "relative", "client", "key")},
			"absolute-user-1": {ClientCertificate: filepath.Join(absoluteDir, "client", "cert"), ClientKey: filepath.Join(absoluteDir, "client", "key")},
			"relative-cmd-1":  {Exec: &clientcmdapi.ExecConfig{Command: filepath.Join("..", "relative", "client", "cmd")}},
			"absolute-cmd-1":  {Exec: &clientcmdapi.ExecConfig{Command: filepath.Join(absoluteDir, "client", "cmd")}},
			"PATH-cmd-1":      {Exec: &clientcmdapi.ExecConfig{Command: "cmd"}},
		},
		Clusters: map[string]*clientcmdapi.Cluster{
			"relative-server-1": {CertificateAuthority: filepath.Join("..", "relative", "ca")},
			"absolute-server-1": {CertificateAuthority: filepath.Join(absoluteDir, "ca")},
		},
	}
	pathResolutionConfig2 := clientcmdapi.Config{
		AuthInfos: map[string]*clientcmdapi.AuthInfo{
			"relative-user-2": {ClientCertificate: filepath.Join("relative", "client", "cert2"), ClientKey: filepath.Join("..", "relative", "client", "key2")},
			"absolute-user-2": {ClientCertificate: filepath.Join(absoluteDir, "client", "cert2"), ClientKey: filepath.Join(absoluteDir, "client", "key2")},
		},
		Clusters: map[string]*clientcmdapi.Cluster{
			"relative-server-2": {CertificateAuthority: filepath.Join("..", "relative", "ca2")},
			"absolute-server-2": {CertificateAuthority: filepath.Join(absoluteDir, "ca2")},
		},
	}

	configDir1, _ := os.MkdirTemp("", "")
	defer os.RemoveAll(configDir1)
	configFile1 := filepath.Join(configDir1, ".kubeconfig")
	configDir1, _ = filepath.Abs(configDir1)

	configDir2, _ := os.MkdirTemp("", "")
	defer os.RemoveAll(configDir2)
	configDir2, _ = os.MkdirTemp(configDir2, "")
	configFile2 := filepath.Join(configDir2, ".kubeconfig")
	configDir2, _ = filepath.Abs(configDir2)

	WriteToFile(pathResolutionConfig1, configFile1)
	WriteToFile(pathResolutionConfig2, configFile2)

	loadingRules := ClientConfigLoadingRules{
		Precedence: []string{configFile1, configFile2},
	}

	mergedConfig, err := loadingRules.Load()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	foundClusterCount := 0
	for key, cluster := range mergedConfig.Clusters {
		if key == "relative-server-1" {
			foundClusterCount++
			matchStringArg(filepath.Join(configDir1, pathResolutionConfig1.Clusters["relative-server-1"].CertificateAuthority), cluster.CertificateAuthority, t)
		}
		if key == "relative-server-2" {
			foundClusterCount++
			matchStringArg(filepath.Join(configDir2, pathResolutionConfig2.Clusters["relative-server-2"].CertificateAuthority), cluster.CertificateAuthority, t)
		}
		if key == "absolute-server-1" {
			foundClusterCount++
			matchStringArg(pathResolutionConfig1.Clusters["absolute-server-1"].CertificateAuthority, cluster.CertificateAuthority, t)
		}
		if key == "absolute-server-2" {
			foundClusterCount++
			matchStringArg(pathResolutionConfig2.Clusters["absolute-server-2"].CertificateAuthority, cluster.CertificateAuthority, t)
		}
	}
	if foundClusterCount != 4 {
		t.Errorf("Expected 4 clusters, found %v: %v", foundClusterCount, mergedConfig.Clusters)
	}

	foundAuthInfoCount := 0
	for key, authInfo := range mergedConfig.AuthInfos {
		if key == "relative-user-1" {
			foundAuthInfoCount++
			matchStringArg(filepath.Join(configDir1, pathResolutionConfig1.AuthInfos["relative-user-1"].ClientCertificate), authInfo.ClientCertificate, t)
			matchStringArg(filepath.Join(configDir1, pathResolutionConfig1.AuthInfos["relative-user-1"].ClientKey), authInfo.ClientKey, t)
		}
		if key == "relative-user-2" {
			foundAuthInfoCount++
			matchStringArg(filepath.Join(configDir2, pathResolutionConfig2.AuthInfos["relative-user-2"].ClientCertificate), authInfo.ClientCertificate, t)
			matchStringArg(filepath.Join(configDir2, pathResolutionConfig2.AuthInfos["relative-user-2"].ClientKey), authInfo.ClientKey, t)
		}
		if key == "absolute-user-1" {
			foundAuthInfoCount++
			matchStringArg(pathResolutionConfig1.AuthInfos["absolute-user-1"].ClientCertificate, authInfo.ClientCertificate, t)
			matchStringArg(pathResolutionConfig1.AuthInfos["absolute-user-1"].ClientKey, authInfo.ClientKey, t)
		}
		if key == "absolute-user-2" {
			foundAuthInfoCount++
			matchStringArg(pathResolutionConfig2.AuthInfos["absolute-user-2"].ClientCertificate, authInfo.ClientCertificate, t)
			matchStringArg(pathResolutionConfig2.AuthInfos["absolute-user-2"].ClientKey, authInfo.ClientKey, t)
		}
		if key == "relative-cmd-1" {
			foundAuthInfoCount++
			matchStringArg(filepath.Join(configDir1, pathResolutionConfig1.AuthInfos[key].Exec.Command), authInfo.Exec.Command, t)
		}
		if key == "absolute-cmd-1" {
			foundAuthInfoCount++
			matchStringArg(pathResolutionConfig1.AuthInfos[key].Exec.Command, authInfo.Exec.Command, t)
		}
		if key == "PATH-cmd-1" {
			foundAuthInfoCount++
			matchStringArg(pathResolutionConfig1.AuthInfos[key].Exec.Command, authInfo.Exec.Command, t)
		}
	}
	if foundAuthInfoCount != 7 {
		t.Errorf("Expected 7 users, found %v: %v", foundAuthInfoCount, mergedConfig.AuthInfos)
	}

}

// TestMigratingFile 校验 MigrationRules 会把旧文件迁移到目标位置，并保留权限。
func TestMigratingFile(t *testing.T) {
	tempDir := t.TempDir()
	sourceFile := filepath.Join(tempDir, "source")
	destinationFile := filepath.Join(tempDir, "destination")

	if err := WriteToFile(testConfigAlfa, sourceFile); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	loadingRules := ClientConfigLoadingRules{
		MigrationRules: map[string]string{destinationFile: sourceFile},
	}

	if _, err := loadingRules.Load(); err != nil {
		t.Errorf("unexpected error %v", err)
	}
	sourceContent, err := os.ReadFile(sourceFile)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
	destinationContent, err := os.ReadFile(destinationFile)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}

	if !reflect.DeepEqual(sourceContent, destinationContent) {
		t.Errorf("source and destination do not match")
	}

	// 目标文件的权限应与源文件权限一致
	sourceInfo, err := os.Stat(sourceFile)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
	destinationInfo, err := os.Stat(destinationFile)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
	if destinationInfo.Mode().Perm() != sourceInfo.Mode().Perm() {
		t.Errorf("expected permissions %v, got %v", sourceInfo.Mode().Perm(), destinationInfo.Mode().Perm())
	}
}

// TestMigratingFileLeaveExistingFileAlone 校验目标文件已存在时迁移不会覆盖它。
func TestMigratingFileLeaveExistingFileAlone(t *testing.T) {
	sourceFile, _ := os.CreateTemp("", "")
	destinationFile, _ := os.CreateTemp("", "")
	defer utiltesting.CloseAndRemove(t, sourceFile, destinationFile)

	WriteToFile(testConfigAlfa, sourceFile.Name())

	loadingRules := ClientConfigLoadingRules{
		MigrationRules: map[string]string{destinationFile.Name(): sourceFile.Name()},
	}

	if _, err := loadingRules.Load(); err != nil {
		t.Errorf("unexpected error %v", err)
	}

	destinationContent, err := os.ReadFile(destinationFile.Name())
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}

	if len(destinationContent) > 0 {
		t.Errorf("destination should not have been touched")
	}
}

// TestMigratingFileSourceMissingSkip 校验源文件缺失时跳过迁移，且不会创建空目标文件。
func TestMigratingFileSourceMissingSkip(t *testing.T) {
	sourceFilename := "some-missing-file"
	destinationFile, _ := os.CreateTemp("", "")
	// 删除该文件，这样后续就会写入它
	utiltesting.CloseAndRemove(t, destinationFile)

	loadingRules := ClientConfigLoadingRules{
		MigrationRules: map[string]string{destinationFile.Name(): sourceFilename},
	}

	if _, err := loadingRules.Load(); err != nil {
		t.Errorf("unexpected error %v", err)
	}

	if _, err := os.Stat(destinationFile.Name()); !os.IsNotExist(err) {
		t.Errorf("destination should not exist")
	}
}

// TestFileLocking 校验同一文件不能被重复加锁，防止并发写入互相覆盖。
func TestFileLocking(t *testing.T) {
	f, _ := os.CreateTemp("", "")
	defer utiltesting.CloseAndRemove(t, f)

	err := lockFile(f.Name())
	if err != nil {
		t.Errorf("unexpected error while locking file: %v", err)
	}
	defer unlockFile(f.Name())

	err = lockFile(f.Name())
	if err == nil {
		t.Error("expected error while locking file.")
	}
}

// Example_noMergingOnExplicitPaths 演示设置了 ExplicitPath 后不再合并其他来源。
func Example_noMergingOnExplicitPaths() {
	commandLineFile, _ := os.CreateTemp("", "")
	envVarFile, _ := os.CreateTemp("", "")
	defer utiltesting.CloseAndRemove(&testing.T{}, commandLineFile, envVarFile)

	WriteToFile(testConfigAlfa, commandLineFile.Name())
	WriteToFile(testConfigConflictAlfa, envVarFile.Name())

	loadingRules := ClientConfigLoadingRules{
		ExplicitPath: commandLineFile.Name(),
		Precedence:   []string{envVarFile.Name()},
	}

	mergedConfig, err := loadingRules.Load()
	if err != nil {
		fmt.Printf("Unexpected error: %v", err)
	}
	json, err := runtime.Encode(clientcmdlatest.Codec, mergedConfig)
	if err != nil {
		fmt.Printf("Unexpected error: %v", err)
	}
	output, err := yaml.JSONToYAML(json)
	if err != nil {
		fmt.Printf("Unexpected error: %v", err)
	}

	fmt.Printf("%v", string(output))
	// Output:
	// apiVersion: v1
	// clusters:
	// - cluster:
	//     server: http://cow.org:8080
	//   name: cow-cluster
	// contexts:
	// - context:
	//     cluster: cow-cluster
	//     namespace: hammer-ns
	//     user: red-user
	//   name: federal-context
	// current-context: ""
	// kind: Config
	// users:
	// - name: red-user
	//   user:
	//     token: red-token
}

// Example_mergingSomeWithConflict 演示合并多个文件时，同名字段以先出现者为准，不同名字段取并集。
func Example_mergingSomeWithConflict() {
	commandLineFile, _ := os.CreateTemp("", "")
	envVarFile, _ := os.CreateTemp("", "")
	defer utiltesting.CloseAndRemove(&testing.T{}, commandLineFile, envVarFile)

	WriteToFile(testConfigAlfa, commandLineFile.Name())
	WriteToFile(testConfigConflictAlfa, envVarFile.Name())

	loadingRules := ClientConfigLoadingRules{
		Precedence: []string{commandLineFile.Name(), envVarFile.Name()},
	}

	mergedConfig, err := loadingRules.Load()
	if err != nil {
		fmt.Printf("Unexpected error: %v", err)
	}
	json, err := runtime.Encode(clientcmdlatest.Codec, mergedConfig)
	if err != nil {
		fmt.Printf("Unexpected error: %v", err)
	}
	output, err := yaml.JSONToYAML(json)
	if err != nil {
		fmt.Printf("Unexpected error: %v", err)
	}

	fmt.Printf("%v", string(output))
	// Output:
	// apiVersion: v1
	// clusters:
	// - cluster:
	//     server: http://cow.org:8080
	//   name: cow-cluster
	// - cluster:
	//     disable-compression: true
	//     insecure-skip-tls-verify: true
	//     server: http://donkey.org:8080
	//   name: donkey-cluster
	// contexts:
	// - context:
	//     cluster: cow-cluster
	//     namespace: hammer-ns
	//     user: red-user
	//   name: federal-context
	// current-context: federal-context
	// kind: Config
	// users:
	// - name: red-user
	//   user:
	//     token: red-token
	// - name: yellow-user
	//   user:
	//     token: yellow-token
}

// Example_mergingEverythingNoConflicts 演示无冲突时所有来源的 cluster/context/user 被合并。
func Example_mergingEverythingNoConflicts() {
	commandLineFile, _ := os.CreateTemp("", "")
	envVarFile, _ := os.CreateTemp("", "")
	currentDirFile, _ := os.CreateTemp("", "")
	homeDirFile, _ := os.CreateTemp("", "")
	defer utiltesting.CloseAndRemove(&testing.T{}, commandLineFile, envVarFile, currentDirFile, homeDirFile)

	WriteToFile(testConfigAlfa, commandLineFile.Name())
	WriteToFile(testConfigBravo, envVarFile.Name())
	WriteToFile(testConfigCharlie, currentDirFile.Name())
	WriteToFile(testConfigDelta, homeDirFile.Name())

	loadingRules := ClientConfigLoadingRules{
		Precedence: []string{commandLineFile.Name(), envVarFile.Name(), currentDirFile.Name(), homeDirFile.Name()},
	}

	mergedConfig, err := loadingRules.Load()
	if err != nil {
		fmt.Printf("Unexpected error: %v", err)
	}
	json, err := runtime.Encode(clientcmdlatest.Codec, mergedConfig)
	if err != nil {
		fmt.Printf("Unexpected error: %v", err)
	}
	output, err := yaml.JSONToYAML(json)
	if err != nil {
		fmt.Printf("Unexpected error: %v", err)
	}

	fmt.Printf("%v", string(output))
	// Output:
	// 	apiVersion: v1
	// clusters:
	// - cluster:
	//     server: http://chicken.org:8080
	//   name: chicken-cluster
	// - cluster:
	//     server: http://cow.org:8080
	//   name: cow-cluster
	// - cluster:
	//     server: http://horse.org:8080
	//   name: horse-cluster
	// - cluster:
	//     server: http://pig.org:8080
	//   name: pig-cluster
	// contexts:
	// - context:
	//     cluster: cow-cluster
	//     namespace: hammer-ns
	//     user: red-user
	//   name: federal-context
	// - context:
	//     cluster: chicken-cluster
	//     namespace: plane-ns
	//     user: blue-user
	//   name: gothic-context
	// - context:
	//     cluster: pig-cluster
	//     namespace: saw-ns
	//     user: black-user
	//   name: queen-anne-context
	// - context:
	//     cluster: horse-cluster
	//     namespace: chisel-ns
	//     user: green-user
	//   name: shaker-context
	// current-context: ""
	// kind: Config
	// users:
	// - name: black-user
	//   user:
	//     token: black-token
	// - name: blue-user
	//   user:
	//     token: blue-token
	// - name: green-user
	//   user:
	//     token: green-token
	// - name: red-user
	//   user:
	//     token: red-token
}

// TestDeduplicate 校验去重保留首次出现的顺序，供加载 precedence 使用。
func TestDeduplicate(t *testing.T) {
	testCases := []struct {
		src    []string
		expect []string
	}{
		{
			src:    []string{"a", "b", "c", "d", "e", "f"},
			expect: []string{"a", "b", "c", "d", "e", "f"},
		},
		{
			src:    []string{"a", "b", "c", "b", "e", "f"},
			expect: []string{"a", "b", "c", "e", "f"},
		},
		{
			src:    []string{"a", "a", "b", "b", "c", "b"},
			expect: []string{"a", "b", "c"},
		},
	}

	for _, testCase := range testCases {
		get := deduplicate(testCase.src)
		if !reflect.DeepEqual(get, testCase.expect) {
			t.Errorf("expect: %v, get: %v", testCase.expect, get)
		}
	}
}

// TestLoadingGetLoadingPrecedence 校验默认路径、显式路径与 KUBECONFIG 多路径的优先级顺序。
func TestLoadingGetLoadingPrecedence(t *testing.T) {
	testCases := map[string]struct {
		rules      *ClientConfigLoadingRules
		env        string
		precedence []string
	}{
		"default": {
			// 独立于主目录重新构造期望的默认路径，
			// 使断言校验的是 RecommendedHomeFile 的组装方式，
			// 而不是拿它和自身比较。
			precedence: []string{filepath.Join(homedir.HomeDir(), RecommendedHomeDir, RecommendedFileName)},
		},
		"explicit": {
			rules: &ClientConfigLoadingRules{
				ExplicitPath: "/explicit/kubeconfig",
			},
			precedence: []string{"/explicit/kubeconfig"},
		},
		"envvar-single": {
			env:        "/env/kubeconfig",
			precedence: []string{"/env/kubeconfig"},
		},
		"envvar-multiple": {
			env:        strings.Join([]string{"/env/kubeconfig", "/other/kubeconfig"}, string(filepath.ListSeparator)),
			precedence: []string{"/env/kubeconfig", "/other/kubeconfig"},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Setenv("KUBECONFIG", test.env)
			rules := test.rules
			if rules == nil {
				rules = NewDefaultClientConfigLoadingRules()
			}
			actual := rules.GetLoadingPrecedence()
			if !reflect.DeepEqual(actual, test.precedence) {
				t.Errorf("expect %v, got %v", test.precedence, actual)
			}
		})
	}
}
