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

package admission

import (
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apiserver/pkg/apis/apiserver"
	apiserverapiv1 "k8s.io/apiserver/pkg/apis/apiserver/v1"
	apiserverapiv1alpha1 "k8s.io/apiserver/pkg/apis/apiserver/v1alpha1"
)

// TestReadAdmissionConfiguration 验证准入配置文件的反序列化与路径修正。
//
// 重点覆盖三类行为：
//  1. 严格解码——重复字段与未知字段必须报错，防止拼写错误被静默忽略；
//  2. 相对路径以配置文件所在目录为基准修正，绝对路径保持不变；
//  3. 旧版扁平配置（imagePolicy 等）在启用对应插件时被映射为插件条目。
func TestReadAdmissionConfiguration(t *testing.T) {
	// 创建占位临时文件，用于存放每个用例的配置
	configFile, err := os.CreateTemp("", "admission-plugin-config")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.Remove(configFile.Name())
	if err = configFile.Close(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	configFileName := configFile.Name()
	// 下面的路径会被修正为相对于测试配置文件的位置
	imagePolicyWebhookFile, err := makeAbs("image-policy-webhook.json", os.TempDir())
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	// 各个测试场景
	testCases := map[string]struct {
		ConfigBody              string
		ExpectedAdmissionConfig *apiserver.AdmissionConfiguration
		PluginNames             []string
		ExpectedError           string
	}{
		"duplicate field configuration error": {
			ConfigBody: `{
"apiVersion": "apiserver.k8s.io/v1alpha1",
"kind": "AdmissionConfiguration",
"plugins": [
  {"name": "ImagePolicyWebhook-duplicate", "name": "ImagePolicyWebhook", "path": "image-policy-webhook.json"},
  {"name": "ResourceQuota"}
]}`,
			ExpectedError: "strict decoding error: duplicate field",
		},
		"unknown field configuration error": {
			ConfigBody: `{
"apiVersion": "apiserver.k8s.io/v1alpha1",
"kind": "AdmissionConfiguration",
"plugins": [
  {"foo": "bar", "name": "ImagePolicyWebhook", "path": "image-policy-webhook.json"},
  {"name": "ResourceQuota"}
]}`,
			ExpectedError: "strict decoding error: unknown field",
		},
		"v1alpha1 configuration - path fixup": {
			ConfigBody: `{
"apiVersion": "apiserver.k8s.io/v1alpha1",
"kind": "AdmissionConfiguration",
"plugins": [
  {"name": "ImagePolicyWebhook", "path": "image-policy-webhook.json"},
  {"name": "ResourceQuota"}
]}`,
			ExpectedAdmissionConfig: &apiserver.AdmissionConfiguration{
				Plugins: []apiserver.AdmissionPluginConfiguration{
					{
						Name: "ImagePolicyWebhook",
						Path: imagePolicyWebhookFile,
					},
					{
						Name: "ResourceQuota",
					},
				},
			},
			PluginNames: []string{},
		},
		"v1alpha1 configuration - abspath": {
			ConfigBody: `{
"apiVersion": "apiserver.k8s.io/v1alpha1",
"kind": "AdmissionConfiguration",
"plugins": [
  {"name": "ImagePolicyWebhook", "path": "/tmp/image-policy-webhook.json"},
  {"name": "ResourceQuota"}
]}`,
			ExpectedAdmissionConfig: &apiserver.AdmissionConfiguration{
				Plugins: []apiserver.AdmissionPluginConfiguration{
					{
						Name: "ImagePolicyWebhook",
						Path: "/tmp/image-policy-webhook.json",
					},
					{
						Name: "ResourceQuota",
					},
				},
			},
			PluginNames: []string{},
		},
		"v1 configuration - path fixup": {
			ConfigBody: `{
"apiVersion": "apiserver.config.k8s.io/v1",
"kind": "AdmissionConfiguration",
"plugins": [
  {"name": "ImagePolicyWebhook", "path": "image-policy-webhook.json"},
  {"name": "ResourceQuota"}
]}`,
			ExpectedAdmissionConfig: &apiserver.AdmissionConfiguration{
				Plugins: []apiserver.AdmissionPluginConfiguration{
					{
						Name: "ImagePolicyWebhook",
						Path: imagePolicyWebhookFile,
					},
					{
						Name: "ResourceQuota",
					},
				},
			},
			PluginNames: []string{},
		},
		"v1 configuration - abspath": {
			ConfigBody: `{
"apiVersion": "apiserver.config.k8s.io/v1",
"kind": "AdmissionConfiguration",
"plugins": [
  {"name": "ImagePolicyWebhook", "path": "/tmp/image-policy-webhook.json"},
  {"name": "ResourceQuota"}
]}`,
			ExpectedAdmissionConfig: &apiserver.AdmissionConfiguration{
				Plugins: []apiserver.AdmissionPluginConfiguration{
					{
						Name: "ImagePolicyWebhook",
						Path: "/tmp/image-policy-webhook.json",
					},
					{
						Name: "ResourceQuota",
					},
				},
			},
			PluginNames: []string{},
		},
		"legacy configuration with using legacy plugins": {
			ConfigBody: `{
"imagePolicy": {
  "kubeConfigFile": "/home/user/.kube/config",
  "allowTTL": 30,
  "denyTTL": 30,
  "retryBackoff": 500,
  "defaultAllow": true
},
"podNodeSelectorPluginConfig": {
  "clusterDefaultNodeSelector": ""
}  
}`,
			ExpectedAdmissionConfig: &apiserver.AdmissionConfiguration{
				Plugins: []apiserver.AdmissionPluginConfiguration{
					{
						Name: "ImagePolicyWebhook",
						Path: configFileName,
					},
					{
						Name: "PodNodeSelector",
						Path: configFileName,
					},
				},
			},
			PluginNames: []string{"ImagePolicyWebhook", "PodNodeSelector"},
		},
		"legacy configuration not using legacy plugins": {
			ConfigBody: `{
"imagePolicy": {
  "kubeConfigFile": "/home/user/.kube/config",
  "allowTTL": 30,
  "denyTTL": 30,
  "retryBackoff": 500,
  "defaultAllow": true
},
"podNodeSelectorPluginConfig": {
  "clusterDefaultNodeSelector": ""
}  
}`,
			ExpectedAdmissionConfig: &apiserver.AdmissionConfiguration{},
			PluginNames:             []string{"NamespaceLifecycle", "InitialResources"},
		},
	}

	scheme := runtime.NewScheme()
	require.NoError(t, apiserver.AddToScheme(scheme))
	require.NoError(t, apiserverapiv1alpha1.AddToScheme(scheme))
	require.NoError(t, apiserverapiv1.AddToScheme(scheme))

	for testName, testCase := range testCases {
		// 每个用例把配置写入同一个临时文件，避免跨用例残留。
		if err = os.WriteFile(configFileName, []byte(testCase.ConfigBody), 0644); err != nil {
			t.Fatalf("unexpected err writing temp file: %v", err)
		}
		config, err := ReadAdmissionConfiguration(testCase.PluginNames, configFileName, scheme)
		if testCase.ExpectedError != "" {
			if err != nil {
				assert.Contains(t, err.Error(), testCase.ExpectedError)
			} else {
				t.Fatalf("expected error %q but received none", testCase.ExpectedError)
			}
		} else if err != nil {
			t.Fatalf("unexpected err: %v", err)
		} else if !reflect.DeepEqual(config.(configProvider).config, testCase.ExpectedAdmissionConfig) {
			// 通过 configProvider 取回内部配置做深比较，确保转换后的结果符合预期。
			t.Fatalf("%s: Expected:\n\t%#v\nGot:\n\t%#v", testName, testCase.ExpectedAdmissionConfig, config.(configProvider).config)
		}

	}
}

// TestEmbeddedConfiguration 验证插件内嵌 configuration 字段原样透传。
//
// 内嵌配置不经过本包的 scheme 解码，而是以原始 JSON 交给插件自行解析；
// 因此带 apiVersion/kind 与不带两种形态都必须保持字节级语义（用 JSON 语义比较，
// 忽略空白差异）。这是插件自定义配置的兼容性基础。
func TestEmbeddedConfiguration(t *testing.T) {
	// 创建占位临时文件，用于存放每个用例的配置
	configFile, err := os.CreateTemp("", "admission-plugin-config")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.Remove(configFile.Name())
	if err = configFile.Close(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	configFileName := configFile.Name()

	testCases := map[string]struct {
		ConfigBody     string
		ExpectedConfig string
	}{
		"v1alpha1 versioned configuration": {
			ConfigBody: `{
				"apiVersion": "apiserver.k8s.io/v1alpha1",
				"kind": "AdmissionConfiguration",
				"plugins": [
				  {
					"name": "Foo",
					"configuration": {
					  "apiVersion": "foo.admission.k8s.io/v1alpha1",
					  "kind": "Configuration",
					  "foo": "bar"
					}
				  }
				]}`,
			ExpectedConfig: `{
			  "apiVersion": "foo.admission.k8s.io/v1alpha1",
			  "kind": "Configuration",
			  "foo": "bar"
			}`,
		},
		"v1alpha1 legacy configuration": {
			ConfigBody: `{
				"apiVersion": "apiserver.k8s.io/v1alpha1",
				"kind": "AdmissionConfiguration",
				"plugins": [
				  {
					"name": "Foo",
					"configuration": {
					  "foo": "bar"
					}
				  }
				]}`,
			ExpectedConfig: `{
			  "foo": "bar"
			}`,
		},
		"v1 versioned configuration": {
			ConfigBody: `{
				"apiVersion": "apiserver.config.k8s.io/v1",
				"kind": "AdmissionConfiguration",
				"plugins": [
				  {
					"name": "Foo",
					"configuration": {
					  "apiVersion": "foo.admission.k8s.io/v1alpha1",
					  "kind": "Configuration",
					  "foo": "bar"
					}
				  }
				]}`,
			ExpectedConfig: `{
			  "apiVersion": "foo.admission.k8s.io/v1alpha1",
			  "kind": "Configuration",
			  "foo": "bar"
			}`,
		},
		"v1 legacy configuration": {
			ConfigBody: `{
				"apiVersion": "apiserver.config.k8s.io/v1",
				"kind": "AdmissionConfiguration",
				"plugins": [
				  {
					"name": "Foo",
					"configuration": {
					  "foo": "bar"
					}
				  }
				]}`,
			ExpectedConfig: `{
			  "foo": "bar"
			}`,
		},
	}

	for desc, test := range testCases {
		scheme := runtime.NewScheme()
		require.NoError(t, apiserver.AddToScheme(scheme))
		require.NoError(t, apiserverapiv1alpha1.AddToScheme(scheme))
		require.NoError(t, apiserverapiv1.AddToScheme(scheme))

		if err = os.WriteFile(configFileName, []byte(test.ConfigBody), 0644); err != nil {
			t.Errorf("[%s] unexpected err writing temp file: %v", desc, err)
			continue
		}
		config, err := ReadAdmissionConfiguration([]string{"Foo"}, configFileName, scheme)
		if err != nil {
			t.Errorf("[%s] unexpected err: %v", desc, err)
			continue
		}
		r, err := config.ConfigFor("Foo")
		if err != nil {
			t.Errorf("[%s] Failed to get Foo config: %v", desc, err)
			continue
		}
		bs, err := io.ReadAll(r)
		if err != nil {
			t.Errorf("[%s] Failed to read Foo config data: %v", desc, err)
			continue
		}

		if !equalJSON(test.ExpectedConfig, string(bs)) {
			t.Errorf("Unexpected config: expected=%q got=%q", test.ExpectedConfig, string(bs))
		}
	}
}

// equalJSON 按 JSON 语义比较两段文本：键顺序与空白差异不影响结果。
func equalJSON(a, b string) bool {
	var x, y interface{}
	if err := json.Unmarshal([]byte(a), &x); err != nil {
		return false
	}
	if err := json.Unmarshal([]byte(b), &y); err != nil {
		return false
	}
	return reflect.DeepEqual(x, y)
}
