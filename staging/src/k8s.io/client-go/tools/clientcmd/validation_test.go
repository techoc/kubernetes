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
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	utiltesting "k8s.io/client-go/util/testing"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// TestConfirmUsableBadInfoButOkConfig 验证 ConfirmUsable 只校验被选中的 context：
// 即使配置中另有一个损坏的 context，使用 "clean" 仍应成功。
func TestConfirmUsableBadInfoButOkConfig(t *testing.T) {
	config := clientcmdapi.NewConfig()
	config.Clusters["missing ca"] = &clientcmdapi.Cluster{
		Server:               "anything",
		CertificateAuthority: "missing",
	}
	config.AuthInfos["error"] = &clientcmdapi.AuthInfo{
		Username: "anything",
		Token:    "here",
	}
	config.Contexts["dirty"] = &clientcmdapi.Context{
		Cluster:  "missing ca",
		AuthInfo: "error",
	}
	config.Clusters["clean"] = &clientcmdapi.Cluster{
		Server: "anything",
	}
	config.AuthInfos["clean"] = &clientcmdapi.AuthInfo{
		Token: "here",
	}
	config.Contexts["clean"] = &clientcmdapi.Context{
		Cluster:  "clean",
		AuthInfo: "clean",
	}

	badValidation := configValidationTest{
		config:                 config,
		expectedErrorSubstring: []string{"unable to read certificate-authority"},
	}
	okTest := configValidationTest{
		config: config,
	}

	okTest.testConfirmUsable("clean", t)
	badValidation.testConfig(t)
}

// TestConfirmUsableMissingObjects 验证 context 引用了不存在的 user/cluster 时，
// ConfirmUsable 与 Validate 都给出指向性的错误信息。
func TestConfirmUsableMissingObjects(t *testing.T) {
	config := clientcmdapi.NewConfig()
	config.Clusters["kind-cluster"] = &clientcmdapi.Cluster{
		Server: "anything",
	}
	config.AuthInfos["kind-user"] = &clientcmdapi.AuthInfo{
		Token: "any-value",
	}
	config.Contexts["missing-user"] = &clientcmdapi.Context{
		Cluster:  "kind-cluster",
		AuthInfo: "garbage",
	}
	config.Contexts["missing-cluster"] = &clientcmdapi.Context{
		Cluster:  "garbage",
		AuthInfo: "kind-user",
	}

	missingUser := configValidationTest{
		config: config,
		expectedErrorSubstring: []string{
			`user "garbage" was not found for context "missing-user"`,
		},
	}
	missingUser.testConfirmUsable("missing-user", t)
	missingUser.testConfig(t)

	missingCluster := configValidationTest{
		config: config,
		expectedErrorSubstring: []string{
			`cluster "garbage" was not found for context "missing-cluster"`,
		},
	}
	missingCluster.testConfirmUsable("missing-cluster", t)
	missingCluster.testConfig(t)
}

// TestConfirmUsableBadInfoConfig 验证被选中的 context 本身有问题时 ConfirmUsable 失败。
func TestConfirmUsableBadInfoConfig(t *testing.T) {
	config := clientcmdapi.NewConfig()
	config.Clusters["missing ca"] = &clientcmdapi.Cluster{
		Server:               "anything",
		CertificateAuthority: "missing",
	}
	config.AuthInfos["error"] = &clientcmdapi.AuthInfo{
		Username: "anything",
		Token:    "here",
	}
	config.Contexts["first"] = &clientcmdapi.Context{
		Cluster:  "missing ca",
		AuthInfo: "error",
	}
	test := configValidationTest{
		config:                 config,
		expectedErrorSubstring: []string{"unable to read certificate-authority"},
	}

	test.testConfirmUsable("first", t)
}

// TestConfirmUsableEmptyConfig 验证空配置给出“no configuration has been provided”。
func TestConfirmUsableEmptyConfig(t *testing.T) {
	config := clientcmdapi.NewConfig()
	test := configValidationTest{
		config:                 config,
		expectedErrorSubstring: []string{"invalid configuration: no configuration has been provided"},
	}

	test.testConfirmUsable("", t)
}

// TestConfirmUsableMissingConfig 验证请求的 context 名不存在时的错误。
func TestConfirmUsableMissingConfig(t *testing.T) {
	config := clientcmdapi.NewConfig()
	test := configValidationTest{
		config:                 config,
		expectedErrorSubstring: []string{"invalid configuration: no configuration has been provided"},
	}

	test.testConfirmUsable("not-here", t)
}

// TestValidateEmptyConfig 验证 Validate 对空配置报错。
func TestValidateEmptyConfig(t *testing.T) {
	config := clientcmdapi.NewConfig()
	test := configValidationTest{
		config:                 config,
		expectedErrorSubstring: []string{"invalid configuration: no configuration has been provided"},
	}

	test.testConfig(t)
}

// TestValidateMissingCurrentContextConfig 验证 current-context 指向不存在的 context 时报错。
func TestValidateMissingCurrentContextConfig(t *testing.T) {
	config := clientcmdapi.NewConfig()
	config.CurrentContext = "anything"
	test := configValidationTest{
		config:                 config,
		expectedErrorSubstring: []string{"context was not found for specified "},
	}

	test.testConfig(t)
}

// TestIsContextNotFound 验证错误分类辅助函数能从聚合错误中识别 context 缺失。
func TestIsContextNotFound(t *testing.T) {
	config := clientcmdapi.NewConfig()
	config.CurrentContext = "anything"

	err := Validate(*config)
	if !IsContextNotFound(err) {
		t.Errorf("Expected context not found, but got %v", err)
	}
	if !IsConfigurationInvalid(err) {
		t.Errorf("Expected configuration invalid, but got %v", err)
	}
}

// TestIsEmptyConfig 验证空配置被归类为 EmptyConfig，同时也属于 ConfigurationInvalid。
func TestIsEmptyConfig(t *testing.T) {
	config := clientcmdapi.NewConfig()

	err := Validate(*config)
	if !IsEmptyConfig(err) {
		t.Errorf("Expected context not found, but got %v", err)
	}
	if !IsConfigurationInvalid(err) {
		t.Errorf("Expected configuration invalid, but got %v", err)
	}
}

// TestIsConfigurationInvalid 验证空错误列表返回 nil、单个错误被直接返回、
// 多个错误才包装为聚合类型。
func TestIsConfigurationInvalid(t *testing.T) {
	if newErrConfigurationInvalid([]error{}) != nil {
		t.Errorf("unexpected error")
	}
	if newErrConfigurationInvalid([]error{ErrNoContext}) == ErrNoContext {
		t.Errorf("unexpected error")
	}
	if newErrConfigurationInvalid([]error{ErrNoContext, ErrNoContext}) == nil {
		t.Errorf("unexpected error")
	}
	if !IsConfigurationInvalid(newErrConfigurationInvalid([]error{ErrNoContext, ErrNoContext})) {
		t.Errorf("unexpected error")
	}
}

// TestValidateMissingReferencesConfig 验证 context 同时引用缺失的 user 与 cluster 时，
// 两个错误都会出现在聚合结果中，而不是只报第一个。
func TestValidateMissingReferencesConfig(t *testing.T) {
	config := clientcmdapi.NewConfig()
	config.CurrentContext = "anything"
	config.Contexts["anything"] = &clientcmdapi.Context{Cluster: "missing", AuthInfo: "missing"}
	test := configValidationTest{
		config:                 config,
		expectedErrorSubstring: []string{"user \"missing\" was not found for context \"anything\"", "cluster \"missing\" was not found for context \"anything\""},
	}

	test.testContext("anything", t)
	test.testConfig(t)
}

// TestValidateEmptyContext 验证 context 未指定 user/cluster 时的错误。
func TestValidateEmptyContext(t *testing.T) {
	config := clientcmdapi.NewConfig()
	config.CurrentContext = "anything"
	config.Contexts["anything"] = &clientcmdapi.Context{}
	test := configValidationTest{
		config:                 config,
		expectedErrorSubstring: []string{"user was not specified for context \"anything\"", "cluster was not specified for context \"anything\""},
	}

	test.testContext("anything", t)
	test.testConfig(t)
}

// TestValidateEmptyContextName 验证 context 名为空字符串时被拒绝。
func TestValidateEmptyContextName(t *testing.T) {
	config := clientcmdapi.NewConfig()
	config.CurrentContext = "anything"
	config.Contexts[""] = &clientcmdapi.Context{Cluster: "missing", AuthInfo: "missing"}
	test := configValidationTest{
		config:                 config,
		expectedErrorSubstring: []string{"empty context name", "is not allowed"},
	}

	test.testContext("", t)
	test.testConfig(t)
}

// TestValidateEmptyClusterInfo 验证 cluster 缺少 server 时报错。
func TestValidateEmptyClusterInfo(t *testing.T) {
	config := clientcmdapi.NewConfig()
	config.Clusters["empty"] = clientcmdapi.NewCluster()
	test := configValidationTest{
		config:                 config,
		expectedErrorSubstring: []string{"cluster has no server defined"},
	}

	test.testCluster("empty", t)
	test.testConfig(t)
}

// TestValidateClusterInfoErrEmptyCluster 验证空 cluster 只产生 ErrEmptyCluster 一个错误。
func TestValidateClusterInfoErrEmptyCluster(t *testing.T) {
	cluster := clientcmdapi.NewCluster()
	errs := validateClusterInfo("", *cluster)

	if len(errs) != 1 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if errs[0] != ErrEmptyCluster {
		t.Errorf("unexpected error: %v", errs[0])
	}
}

// TestValidateMissingCAFileClusterInfo 验证引用了不存在的 CA 文件时报错。
func TestValidateMissingCAFileClusterInfo(t *testing.T) {
	config := clientcmdapi.NewConfig()
	config.Clusters["missing ca"] = &clientcmdapi.Cluster{
		Server:               "anything",
		CertificateAuthority: "missing",
	}
	test := configValidationTest{
		config:                 config,
		expectedErrorSubstring: []string{"unable to read certificate-authority"},
	}

	test.testCluster("missing ca", t)
	test.testConfig(t)
}

// TestValidateCleanClusterInfo 验证仅含 server 的 cluster 合法。
func TestValidateCleanClusterInfo(t *testing.T) {
	config := clientcmdapi.NewConfig()
	config.Clusters["clean"] = &clientcmdapi.Cluster{
		Server: "anything",
	}
	test := configValidationTest{
		config: config,
	}

	test.testCluster("clean", t)
	test.testConfig(t)
}

func TestValidateCleanWithCAClusterInfo(t *testing.T) {
	tempFile, _ := os.CreateTemp("", "")
	defer utiltesting.CloseAndRemove(t, tempFile)

	config := clientcmdapi.NewConfig()
	config.Clusters["clean"] = &clientcmdapi.Cluster{
		Server:               "anything",
		CertificateAuthority: tempFile.Name(),
	}
	test := configValidationTest{
		config: config,
	}

	test.testCluster("clean", t)
	test.testConfig(t)
}

// TestValidateEmptyAuthInfo 验证空 AuthInfo 合法：它表示匿名访问，由鉴权层决定是否允许。
func TestValidateEmptyAuthInfo(t *testing.T) {
	config := clientcmdapi.NewConfig()
	config.AuthInfos["error"] = &clientcmdapi.AuthInfo{}
	test := configValidationTest{
		config: config,
	}

	test.testAuthInfo("error", t)
	test.testConfig(t)
}

// TestValidateCertFilesNotFoundAuthInfo 验证客户端证书/私钥文件缺失时的两个错误都被报告。
func TestValidateCertFilesNotFoundAuthInfo(t *testing.T) {
	config := clientcmdapi.NewConfig()
	config.AuthInfos["error"] = &clientcmdapi.AuthInfo{
		ClientCertificate: "missing",
		ClientKey:         "missing",
	}
	test := configValidationTest{
		config:                 config,
		expectedErrorSubstring: []string{"unable to read client-cert", "unable to read client-key"},
	}

	test.testAuthInfo("error", t)
	test.testConfig(t)
}

// TestValidateCertDataOverridesFiles 验证同时指定 data 与文件路径被视为歧义配置
// （与运行时“data 优先”的覆盖行为不同，验证阶段要求明确）。
func TestValidateCertDataOverridesFiles(t *testing.T) {
	tempFile, _ := os.CreateTemp("", "")
	defer utiltesting.CloseAndRemove(t, tempFile)

	config := clientcmdapi.NewConfig()
	config.AuthInfos["clean"] = &clientcmdapi.AuthInfo{
		ClientCertificate:     tempFile.Name(),
		ClientCertificateData: []byte("certdata"),
		ClientKey:             tempFile.Name(),
		ClientKeyData:         []byte("keydata"),
	}
	test := configValidationTest{
		config:                 config,
		expectedErrorSubstring: []string{"client-cert-data and client-cert are both specified", "client-key-data and client-key are both specified"},
	}

	test.testAuthInfo("clean", t)
	test.testConfig(t)
}

// TestValidateCleanCertFilesAuthInfo 验证存在的证书与私钥文件路径合法。
func TestValidateCleanCertFilesAuthInfo(t *testing.T) {
	tempFile, _ := os.CreateTemp("", "")
	defer utiltesting.CloseAndRemove(t, tempFile)

	config := clientcmdapi.NewConfig()
	config.AuthInfos["clean"] = &clientcmdapi.AuthInfo{
		ClientCertificate: tempFile.Name(),
		ClientKey:         tempFile.Name(),
	}
	test := configValidationTest{
		config: config,
	}

	test.testAuthInfo("clean", t)
	test.testConfig(t)
}

// TestValidateCleanTokenAuthInfo 验证仅使用 token 的 AuthInfo 合法。
func TestValidateCleanTokenAuthInfo(t *testing.T) {
	config := clientcmdapi.NewConfig()
	config.AuthInfos["clean"] = &clientcmdapi.AuthInfo{
		Token: "any-value",
	}
	test := configValidationTest{
		config: config,
	}

	test.testAuthInfo("clean", t)
	test.testConfig(t)
}

// TestValidateMultipleMethodsAuthInfo 验证同时配置 token 与 basic auth 会被拒绝，
// 因为无法确定应使用哪一种认证方式。
func TestValidateMultipleMethodsAuthInfo(t *testing.T) {
	config := clientcmdapi.NewConfig()
	config.AuthInfos["error"] = &clientcmdapi.AuthInfo{
		Token:    "token",
		Username: "username",
	}
	test := configValidationTest{
		config:                 config,
		expectedErrorSubstring: []string{"more than one authentication method", "token", "basicAuth"},
	}

	test.testAuthInfo("error", t)
	test.testConfig(t)
}

// TestValidateAuthInfoExec 验证完整的 exec 插件配置（命令、版本、参数、环境变量、
// interactiveMode）可以顺利通过校验。
func TestValidateAuthInfoExec(t *testing.T) {
	config := clientcmdapi.NewConfig()
	config.AuthInfos["user"] = &clientcmdapi.AuthInfo{
		Exec: &clientcmdapi.ExecConfig{
			Command:    "/bin/example",
			APIVersion: "clientauthentication.k8s.io/v1alpha1",
			Args:       []string{"hello", "world"},
			Env: []clientcmdapi.ExecEnvVar{
				{Name: "foo", Value: "bar"},
			},
			InteractiveMode: clientcmdapi.IfAvailableExecInteractiveMode,
		},
	}
	test := configValidationTest{
		config: config,
	}

	test.testAuthInfo("user", t)
	test.testConfig(t)
}

// TestValidateAuthInfoExecNoVersion 验证 exec 插件缺少 apiVersion 时报错。
func TestValidateAuthInfoExecNoVersion(t *testing.T) {
	config := clientcmdapi.NewConfig()
	config.AuthInfos["user"] = &clientcmdapi.AuthInfo{
		Exec: &clientcmdapi.ExecConfig{
			Command:         "/bin/example",
			InteractiveMode: clientcmdapi.IfAvailableExecInteractiveMode,
		},
	}
	test := configValidationTest{
		config: config,
		expectedErrorSubstring: []string{
			"apiVersion must be specified for user to use exec authentication plugin",
		},
	}

	test.testAuthInfo("user", t)
	test.testConfig(t)
}

// TestValidateAuthInfoExecNoCommand 验证 exec 插件缺少 command 时报错。
func TestValidateAuthInfoExecNoCommand(t *testing.T) {
	config := clientcmdapi.NewConfig()
	config.AuthInfos["user"] = &clientcmdapi.AuthInfo{
		Exec: &clientcmdapi.ExecConfig{
			APIVersion:      "clientauthentication.k8s.io/v1alpha1",
			InteractiveMode: clientcmdapi.IfAvailableExecInteractiveMode,
		},
	}
	test := configValidationTest{
		config: config,
		expectedErrorSubstring: []string{
			"command must be specified for user to use exec authentication plugin",
		},
	}

	test.testAuthInfo("user", t)
	test.testConfig(t)
}

// TestValidateAuthInfoExecWithAuthProvider 验证 exec 与 authProvider 不能同时配置。
func TestValidateAuthInfoExecWithAuthProvider(t *testing.T) {
	config := clientcmdapi.NewConfig()
	config.AuthInfos["user"] = &clientcmdapi.AuthInfo{
		AuthProvider: &clientcmdapi.AuthProviderConfig{
			Name: "oidc",
		},
		Exec: &clientcmdapi.ExecConfig{
			Command:         "/bin/example",
			APIVersion:      "clientauthentication.k8s.io/v1alpha1",
			InteractiveMode: clientcmdapi.IfAvailableExecInteractiveMode,
		},
	}
	test := configValidationTest{
		config: config,
		expectedErrorSubstring: []string{
			"authProvider cannot be provided in combination with an exec plugin for user",
		},
	}

	test.testAuthInfo("user", t)
	test.testConfig(t)
}

// TestValidateAuthInfoExecNoEnv 验证环境变量值为空是允许的（只有名字必填）。
func TestValidateAuthInfoExecNoEnv(t *testing.T) {
	config := clientcmdapi.NewConfig()
	config.AuthInfos["user"] = &clientcmdapi.AuthInfo{
		Exec: &clientcmdapi.ExecConfig{
			Command:    "/bin/example",
			APIVersion: "clientauthentication.k8s.io/v1alpha1",
			Env: []clientcmdapi.ExecEnvVar{
				{Name: "foo", Value: ""},
			},
			InteractiveMode: clientcmdapi.IfAvailableExecInteractiveMode,
		},
	}
	test := configValidationTest{
		config: config,
	}

	test.testAuthInfo("user", t)
	test.testConfig(t)
}

// TestValidateAuthInfoExecInteractiveModeMissing 验证 interactiveMode 为必填项，
// 强制插件作者显式声明其交互能力。
func TestValidateAuthInfoExecInteractiveModeMissing(t *testing.T) {
	config := clientcmdapi.NewConfig()
	config.AuthInfos["user"] = &clientcmdapi.AuthInfo{
		Exec: &clientcmdapi.ExecConfig{
			Command:    "/bin/example",
			APIVersion: "clientauthentication.k8s.io/v1alpha1",
		},
	}
	test := configValidationTest{
		config: config,
		expectedErrorSubstring: []string{
			"interactiveMode must be specified for user to use exec authentication plugin",
		},
	}

	test.testAuthInfo("user", t)
	test.testConfig(t)
}

// TestValidateAuthInfoExecInteractiveModeInvalid 验证非法 interactiveMode 取值被拒绝。
func TestValidateAuthInfoExecInteractiveModeInvalid(t *testing.T) {
	config := clientcmdapi.NewConfig()
	config.AuthInfos["user"] = &clientcmdapi.AuthInfo{
		Exec: &clientcmdapi.ExecConfig{
			Command:         "/bin/example",
			APIVersion:      "clientauthentication.k8s.io/v1alpha1",
			InteractiveMode: "invalid",
		},
	}
	test := configValidationTest{
		config: config,
		expectedErrorSubstring: []string{
			`invalid interactiveMode for user: "invalid"`,
		},
	}

	test.testAuthInfo("user", t)
	test.testConfig(t)
}

// TestValidateAuthInfoImpersonateUser 验证仅指定 impersonate user 的配置合法。
func TestValidateAuthInfoImpersonateUser(t *testing.T) {
	config := clientcmdapi.NewConfig()
	config.AuthInfos["user"] = &clientcmdapi.AuthInfo{
		Impersonate: "user",
	}
	test := configValidationTest{
		config: config,
	}
	test.testAuthInfo("user", t)
	test.testConfig(t)
}

// TestValidateAuthInfoImpersonateEverything 验证同时 impersonate user/uid/groups/extra 合法。
func TestValidateAuthInfoImpersonateEverything(t *testing.T) {
	config := clientcmdapi.NewConfig()
	config.AuthInfos["user"] = &clientcmdapi.AuthInfo{
		Impersonate:          "user",
		ImpersonateUID:       "abc123",
		ImpersonateGroups:    []string{"group-1", "group-2"},
		ImpersonateUserExtra: map[string][]string{"key": {"val1", "val2"}},
	}
	test := configValidationTest{
		config: config,
	}
	test.testAuthInfo("user", t)
	test.testConfig(t)
}

// TestValidateAuthInfoImpersonateGroupsWithoutUserInvalid 验证未指定 impersonate user
// 却请求 groups 是非法配置。
func TestValidateAuthInfoImpersonateGroupsWithoutUserInvalid(t *testing.T) {
	config := clientcmdapi.NewConfig()
	config.AuthInfos["user"] = &clientcmdapi.AuthInfo{
		ImpersonateGroups: []string{"group-1", "group-2"},
	}
	test := configValidationTest{
		config: config,
		expectedErrorSubstring: []string{
			`requesting uid, groups or user-extra for user without impersonating a user`,
		},
	}
	test.testAuthInfo("user", t)
	test.testConfig(t)
}

// TestValidateAuthInfoImpersonateExtraWithoutUserInvalid 验证未指定 impersonate user
// 却请求 user-extra 是非法配置。
func TestValidateAuthInfoImpersonateExtraWithoutUserInvalid(t *testing.T) {
	config := clientcmdapi.NewConfig()
	config.AuthInfos["user"] = &clientcmdapi.AuthInfo{
		ImpersonateUserExtra: map[string][]string{"key": {"val1", "val2"}},
	}
	test := configValidationTest{
		config: config,
		expectedErrorSubstring: []string{
			`requesting uid, groups or user-extra for user without impersonating a user`,
		},
	}
	test.testAuthInfo("user", t)
	test.testConfig(t)
}

// TestValidateAuthInfoImpersonateUIDWithoutUserInvalid 验证未指定 impersonate user
// 却请求 uid 是非法配置。
func TestValidateAuthInfoImpersonateUIDWithoutUserInvalid(t *testing.T) {
	config := clientcmdapi.NewConfig()
	config.AuthInfos["user"] = &clientcmdapi.AuthInfo{
		ImpersonateUID: "abc123",
	}
	test := configValidationTest{
		config: config,
		expectedErrorSubstring: []string{
			`requesting uid, groups or user-extra for user without impersonating a user`,
		},
	}
	test.testAuthInfo("user", t)
	test.testConfig(t)
}

// configValidationTest 把“配置 + 期望的错误子串”组合起来，
// 由各个 test* 方法套用到不同的校验入口，避免重复断言逻辑。
type configValidationTest struct {
	config                 *clientcmdapi.Config
	expectedErrorSubstring []string
}

// testContext 对单个 context 执行 validateContext，并检查错误是否包含期望子串。
func (c configValidationTest) testContext(contextName string, t *testing.T) {
	errs := validateContext(contextName, *c.config.Contexts[contextName], *c.config)

	if len(c.expectedErrorSubstring) != 0 {
		if len(errs) == 0 {
			t.Errorf("Expected error containing: %v", c.expectedErrorSubstring)
		}
		for _, curr := range c.expectedErrorSubstring {
			if len(errs) != 0 && !strings.Contains(utilerrors.NewAggregate(errs).Error(), curr) {
				t.Errorf("Expected error containing: %v, but got %v", c.expectedErrorSubstring, utilerrors.NewAggregate(errs))
			}
		}

	} else {
		if len(errs) != 0 {
			t.Errorf("Unexpected error: %v", utilerrors.NewAggregate(errs))
		}
	}
}

// testConfirmUsable 验证 ConfirmUsable 的结果；该入口只关注被选中的 context。
func (c configValidationTest) testConfirmUsable(contextName string, t *testing.T) {
	err := ConfirmUsable(*c.config, contextName)

	if len(c.expectedErrorSubstring) != 0 {
		if err == nil {
			t.Errorf("Expected error containing: %v", c.expectedErrorSubstring)
		} else {
			for _, curr := range c.expectedErrorSubstring {
				if err != nil && !strings.Contains(err.Error(), curr) {
					t.Errorf("Expected error containing: %v, but got %v", c.expectedErrorSubstring, err)
				}
			}
		}
	} else {
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	}
}

// testConfig 验证 Validate 的结果，并额外要求所有错误都实现 ConfigurationInvalid 分类。
func (c configValidationTest) testConfig(t *testing.T) {
	err := Validate(*c.config)

	if len(c.expectedErrorSubstring) != 0 {
		if err == nil {
			t.Errorf("Expected error containing: %v", c.expectedErrorSubstring)
		} else {
			for _, curr := range c.expectedErrorSubstring {
				if err != nil && !strings.Contains(err.Error(), curr) {
					t.Errorf("Expected error containing: %v, but got %v", c.expectedErrorSubstring, err)
				}
			}
			if !IsConfigurationInvalid(err) {
				t.Errorf("all errors should be configuration invalid: %v", err)
			}
		}
	} else {
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	}
}

// testCluster 对单个 cluster 执行 validateClusterInfo 并断言错误。
func (c configValidationTest) testCluster(clusterName string, t *testing.T) {
	errs := validateClusterInfo(clusterName, *c.config.Clusters[clusterName])

	if len(c.expectedErrorSubstring) != 0 {
		if len(errs) == 0 {
			t.Errorf("Expected error containing: %v", c.expectedErrorSubstring)
		}
		for _, curr := range c.expectedErrorSubstring {
			if len(errs) != 0 && !strings.Contains(utilerrors.NewAggregate(errs).Error(), curr) {
				t.Errorf("Expected error containing: %v, but got %v", c.expectedErrorSubstring, utilerrors.NewAggregate(errs))
			}
		}

	} else {
		if len(errs) != 0 {
			t.Errorf("Unexpected error: %v", utilerrors.NewAggregate(errs))
		}
	}
}

// testAuthInfo 对单个 AuthInfo 执行 validateAuthInfo 并断言错误。
func (c configValidationTest) testAuthInfo(authInfoName string, t *testing.T) {
	errs := validateAuthInfo(authInfoName, *c.config.AuthInfos[authInfoName])

	if len(c.expectedErrorSubstring) != 0 {
		if len(errs) == 0 {
			t.Errorf("Expected error containing: %v", c.expectedErrorSubstring)
		}
		for _, curr := range c.expectedErrorSubstring {
			if len(errs) != 0 && !strings.Contains(utilerrors.NewAggregate(errs).Error(), curr) {
				t.Errorf("Expected error containing: %v, but got %v", c.expectedErrorSubstring, utilerrors.NewAggregate(errs))
			}
		}

	} else {
		if len(errs) != 0 {
			t.Errorf("Unexpected error: %v", utilerrors.NewAggregate(errs))
		}
	}
}

// alwaysMatchingError 的 Is 恒为 true，用于验证 errConfigurationInvalid 会调用成员的 Is。
type alwaysMatchingError struct{}

func (_ alwaysMatchingError) Error() string {
	return "error"
}

func (_ alwaysMatchingError) Is(_ error) bool {
	return true
}

// someError 可比较（空结构体无字段），用于验证基于相等性的匹配。
type someError struct{ msg string }

func (se someError) Error() string {
	if se.msg != "" {
		return se.msg
	}
	return "err"
}

// TestErrConfigurationInvalidWithErrorsIs 验证聚合错误支持 errors.Is 的多种匹配方式：
// 成员自定义 Is、值相等、嵌套聚合以及被 %w 包裹的情况。
func TestErrConfigurationInvalidWithErrorsIs(t *testing.T) {
	testCases := []struct {
		name         string
		err          error
		matchAgainst error
		expectMatch  bool
	}{{
		name:         "no match",
		err:          errConfigurationInvalid{errors.New("my-error"), errors.New("my-other-error")},
		matchAgainst: fmt.Errorf("no entry %s", "here"),
	}, {
		name:         "match via .Is()",
		err:          errConfigurationInvalid{errors.New("forbidden"), alwaysMatchingError{}},
		matchAgainst: errors.New("unauthorized"),
		expectMatch:  true,
	}, {
		name:         "match via equality",
		err:          errConfigurationInvalid{errors.New("err"), someError{}},
		matchAgainst: someError{},
		expectMatch:  true,
	}, {
		name:         "match via nested aggregate",
		err:          errConfigurationInvalid{errors.New("closed today"), errConfigurationInvalid{errConfigurationInvalid{someError{}}}},
		matchAgainst: someError{},
		expectMatch:  true,
	}, {
		name:         "match via wrapped aggregate",
		err:          fmt.Errorf("wrap: %w", errConfigurationInvalid{errors.New("err"), someError{}}),
		matchAgainst: someError{},
		expectMatch:  true,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := errors.Is(tc.err, tc.matchAgainst)
			if result != tc.expectMatch {
				t.Errorf("expected match: %t, got match: %t", tc.expectMatch, result)
			}
		})
	}
}

// accessTrackingError 记录 Is 是否被调用，用于验证短路行为。
type accessTrackingError struct {
	wasAccessed bool
}

func (accessTrackingError) Error() string {
	return "err"
}

func (ate *accessTrackingError) Is(_ error) bool {
	ate.wasAccessed = true
	return true
}

var _ error = &accessTrackingError{}

// TestErrConfigurationInvalidWithErrorsIsShortCircuitsOnFirstMatch 验证 errors.Is
// 在第一个匹配后立即返回，不会继续访问后续成员。
func TestErrConfigurationInvalidWithErrorsIsShortCircuitsOnFirstMatch(t *testing.T) {
	errC := errConfigurationInvalid{&accessTrackingError{}, &accessTrackingError{}}
	_ = errors.Is(errC, &accessTrackingError{})

	var numAccessed int
	for _, err := range errC {
		if ate := err.(*accessTrackingError); ate.wasAccessed {
			numAccessed++
		}
	}
	if numAccessed != 1 {
		t.Errorf("expected exactly one error to get accessed, got %d", numAccessed)
	}
}
