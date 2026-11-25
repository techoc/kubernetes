/*
Copyright 2020 The Kubernetes Authors.

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

package componentconfigs

import (
	"crypto/sha256"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/lithammer/dedent"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	clientsetfake "k8s.io/client-go/kubernetes/fake"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	kubeadmscheme "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/scheme"
	kubeadmapiv1 "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/v1beta4"
	outputapiv1alpha3 "k8s.io/kubernetes/cmd/kubeadm/app/apis/output/v1alpha3"
	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
	kubeadmutil "k8s.io/kubernetes/cmd/kubeadm/app/util"
)

// 本文件所有测试使用替代的 `known` 组件配置集合。
// 这里仅包含一个已知配置，即 kubeadm 自身的 ClusterConfiguration。
// ClusterConfiguration 通常不由本包管理，仅出于以下原因用于测试：
// - 它是版本化 API，由 kubeadm 维护者掌控。这样无需全面且持续了解其他组件的配置，
//   也能更充分地测试 componentconfigs 包。
// - 其他组件经常在不升级配置版本的情况下向配置添加字段。
//   这通常要求添加这些字段的 PR 同时修改 kubeadm 测试代码，增加开发者和审查者的工作。
//   当 kubeadm 迁出 k/k 后，在 k/k 中合并并添加字段的 PR 将无法修复 kubeadm 中的测试，
//   从而使 kubeadm 测试更偶发地出现故障。
// - 如果使用其他组件的配置测试所有通用功能，而该配置被弃用或停止在生产中支持，
//   就需要大规模重构测试，或仅为测试继续导入该配置。
//
// 因此，为在不降低测试覆盖率的情况下减少维护成本，这里引入一个小型框架和一组测试，
// 用单个 ClusterConfiguration 替代普通组件配置，测试本包中独立于组件配置的逻辑。

// clusterConfigHandler 是测试中使用的最新受支持 ClusterConfiguration 的处理实例。
var clusterConfigHandler = handler{
	GroupVersion: kubeadmapiv1.SchemeGroupVersion,
	AddToScheme:  kubeadmapiv1.AddToScheme,
	CreateEmpty: func() kubeadmapi.ComponentConfig {
		return &clusterConfig{
			configBase: configBase{
				GroupVersion: kubeadmapiv1.SchemeGroupVersion,
			},
		}
	},
	fromCluster: clusterConfigFromCluster,
}

func clusterConfigFromCluster(h *handler, clientset clientset.Interface, _ *kubeadmapi.ClusterConfiguration) (kubeadmapi.ComponentConfig, error) {
	return h.fromConfigMap(clientset, constants.KubeadmConfigConfigMap, constants.ClusterConfigurationConfigMapKey, true)
}

type clusterConfig struct {
	configBase
	config kubeadmapiv1.ClusterConfiguration
}

func (cc *clusterConfig) DeepCopy() kubeadmapi.ComponentConfig {
	result := &clusterConfig{}
	cc.configBase.DeepCopyInto(&result.configBase)
	cc.config.DeepCopyInto(&result.config)
	return result
}

func (cc *clusterConfig) Marshal() ([]byte, error) {
	return cc.configBase.Marshal(&cc.config)
}

func (cc *clusterConfig) Unmarshal(docmap kubeadmapi.DocumentMap) error {
	return cc.configBase.Unmarshal(docmap, &cc.config)
}

func (cc *clusterConfig) Get() interface{} {
	return &cc.config
}

func (cc *clusterConfig) Set(cfg interface{}) {
	cc.config = *cfg.(*kubeadmapiv1.ClusterConfiguration)
}

func (cc *clusterConfig) Default(_ *kubeadmapi.ClusterConfiguration, _ *kubeadmapi.APIEndpoint, _ *kubeadmapi.NodeRegistrationOptions) {
	cc.config.ClusterName = "foo"
	cc.config.KubernetesVersion = "bar"
}

func (cc *clusterConfig) Mutate() error {
	return nil
}

// fakeKnown 在本文件每个测试执行期间临时替换 known（位于 configset.go）。
var fakeKnown = []*handler{
	&clusterConfigHandler,
}

// fakeKnownContext 是承载伪组件配置上下文的函数。
// 注意：不支持并发执行测试！
func fakeKnownContext(f func()) {
	// 保存真实值。
	realKnown := known
	realScheme := Scheme
	realCodecs := Codecs

	// 将上下文替换为伪上下文。
	known = fakeKnown
	Scheme = kubeadmscheme.Scheme
	Codecs = kubeadmscheme.Codecs

	// 函数退出时恢复真实值。
	defer func() {
		known = realKnown
		Scheme = realScheme
		Codecs = realCodecs
	}()

	// 在伪上下文中调用 f。
	f()
}

// testClusterConfigMap 是创建测试 ConfigMap 并按需签名的简写函数。
// 生成的 ConfigMap 可由 clusterConfigFromCluster 加载。
func testClusterConfigMap(yaml string, signIt bool) *v1.ConfigMap {
	cm := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      constants.KubeadmConfigConfigMap,
			Namespace: metav1.NamespaceSystem,
		},
		Data: map[string]string{
			constants.ClusterConfigurationConfigMapKey: dedent.Dedent(yaml),
		},
	}

	if signIt {
		SignConfigMap(cm)
	}

	return cm
}

// oldClusterConfigVersion 在本文件测试中用作旧的不受支持版本。
const oldClusterConfigVersion = "v1alpha1"

var (
	// currentClusterConfigVersion 表示当前受支持的 ClusterConfiguration 版本。
	currentClusterConfigVersion = kubeadmapiv1.SchemeGroupVersion.Version

	// currentFooClusterConfig 是当前受支持的最小 ClusterConfiguration，
	// clusterName 使用已知值 `foo`。
	currentFooClusterConfig = fmt.Sprintf(`
		apiVersion: %s
		kind: ClusterConfiguration
		clusterName: foo
	`, kubeadmapiv1.SchemeGroupVersion)

	// oldFooClusterConfig 是不受支持的最小 ClusterConfiguration，
	// clusterName 使用已知值 `foo`。
	oldFooClusterConfig = fmt.Sprintf(`
		apiVersion: %s/%s
		kind: ClusterConfiguration
		clusterName: foo
	`, kubeadmapiv1.GroupName, oldClusterConfigVersion)

	// 这是可与 YAML 双向序列化的“最小”有效配置。
	// 由于采用相同的静态默认化逻辑，实际大小并不小。
	validUnmarshallableClusterConfig = struct {
		yaml string
		obj  kubeadmapiv1.ClusterConfiguration
	}{
		yaml: dedent.Dedent(fmt.Sprintf(`
			apiVersion: %s
			certificatesDir: /etc/kubernetes/pki
			clusterName: LeCluster
			encryptionAlgorithm: "RSA-2048"
			certificateValidityPeriod: "8760h0m0s"
			caCertificateValidityPeriod: "87600h0m0s"
			controllerManager: {}
			etcd:
			  local:
			    dataDir: /var/lib/etcd
			imageRepository: registry.k8s.io
			kind: ClusterConfiguration
			kubernetesVersion: 1.2.3
			networking:
			  dnsDomain: cluster.local
			  serviceSubnet: 10.96.0.0/12
			proxy: {}
			scheduler: {}
		`, kubeadmapiv1.SchemeGroupVersion.String())),
		obj: kubeadmapiv1.ClusterConfiguration{
			TypeMeta: metav1.TypeMeta{
				APIVersion: kubeadmapiv1.SchemeGroupVersion.String(),
				Kind:       "ClusterConfiguration",
			},
			ClusterName:       "LeCluster",
			KubernetesVersion: "1.2.3",
			CertificatesDir:   "/etc/kubernetes/pki",
			ImageRepository:   "registry.k8s.io",
			Networking: kubeadmapiv1.Networking{
				DNSDomain:     "cluster.local",
				ServiceSubnet: "10.96.0.0/12",
			},
			Etcd: kubeadmapiv1.Etcd{
				Local: &kubeadmapiv1.LocalEtcd{
					DataDir: "/var/lib/etcd",
				},
			},
			EncryptionAlgorithm: kubeadmapiv1.EncryptionAlgorithmRSA2048,
			CertificateValidityPeriod: &metav1.Duration{
				Duration: time.Hour * 8760,
			},
			CACertificateValidityPeriod: &metav1.Duration{
				Duration: time.Hour * 87600,
			},
		},
	}
)

func TestConfigBaseMarshal(t *testing.T) {
	fakeKnownContext(func() {
		cfg := &clusterConfig{
			configBase: configBase{
				GroupVersion: kubeadmapiv1.SchemeGroupVersion,
			},
			config: kubeadmapiv1.ClusterConfiguration{
				TypeMeta: metav1.TypeMeta{
					APIVersion: kubeadmapiv1.SchemeGroupVersion.String(),
					Kind:       "ClusterConfiguration",
				},
				ClusterName:       "LeCluster",
				KubernetesVersion: "1.2.3",
			},
		}

		b, err := cfg.Marshal()
		if err != nil {
			t.Fatalf("Marshal failed: %v", err)
		}

		got := strings.TrimSpace(string(b))
		expected := strings.TrimSpace(dedent.Dedent(fmt.Sprintf(`
			apiServer: {}
			apiVersion: %s
			clusterName: LeCluster
			controllerManager: {}
			dns: {}
			etcd: {}
			kind: ClusterConfiguration
			kubernetesVersion: 1.2.3
			networking: {}
			proxy: {}
			scheduler: {}
		`, kubeadmapiv1.SchemeGroupVersion.String())))

		if expected != got {
			t.Fatalf("Mismatch between expected and got:\nExpected:\n%s\n---\nGot:\n%s", expected, got)
		}
	})
}

func TestConfigBaseUnmarshal(t *testing.T) {
	fakeKnownContext(func() {
		expected := &clusterConfig{
			configBase: configBase{
				GroupVersion: kubeadmapiv1.SchemeGroupVersion,
			},
			config: validUnmarshallableClusterConfig.obj,
		}

		gvkmap, err := kubeadmutil.SplitConfigDocuments([]byte(validUnmarshallableClusterConfig.yaml))
		if err != nil {
			t.Fatalf("unexpected failure of SplitConfigDocuments: %v", err)
		}

		got := &clusterConfig{
			configBase: configBase{
				GroupVersion: kubeadmapiv1.SchemeGroupVersion,
			},
		}
		if err = got.Unmarshal(gvkmap); err != nil {
			t.Fatalf("unexpected failure of Unmarshal: %v", err)
		}

		if diff := cmp.Diff(expected.config, got.config); diff != "" {
			t.Fatalf("Unexpected diff (-expected,+got):\n%s", diff)
		}
	})
}

func TestGeneratedConfigFromCluster(t *testing.T) {
	fakeKnownContext(func() {
		testYAML := dedent.Dedent(fmt.Sprintf(`
			apiVersion: %s
			kind: ClusterConfiguration
		`, kubeadmapiv1.SchemeGroupVersion.String()))
		testYAMLHash := fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(testYAML)))
		// 字符串 "The quick brown fox jumps over the lazy dog" 的 SHA256 摘要。
		const mismatchHash = "sha256:d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592"
		tests := []struct {
			name         string
			hash         string
			userSupplied bool
		}{
			{
				name: "Matching hash means generated config",
				hash: testYAMLHash,
			},
			{
				name:         "Mismatching hash means user supplied config",
				hash:         mismatchHash,
				userSupplied: true,
			},
			{
				name:         "No hash means user supplied config",
				userSupplied: true,
			},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				configMap := testClusterConfigMap(testYAML, false)
				if test.hash != "" {
					configMap.Annotations = map[string]string{
						constants.ComponentConfigHashAnnotationKey: test.hash,
					}
				}

				client := clientsetfake.NewSimpleClientset(configMap)
				cfg, err := clusterConfigHandler.FromCluster(client, testClusterCfg())
				if err != nil {
					t.Fatalf("unexpected failure of FromCluster: %v", err)
				}

				got := cfg.IsUserSupplied()
				if got != test.userSupplied {
					t.Fatalf("mismatch between expected and got:\n\tExpected: %t\n\tGot: %t", test.userSupplied, got)
				}
			})
		}
	})
}

// runClusterConfigFromTest 保存 handler.From* 函数的通用测试数据和求值逻辑。
func runClusterConfigFromTest(t *testing.T, perform func(t *testing.T, in string) (kubeadmapi.ComponentConfig, error)) {
	fakeKnownContext(func() {
		tests := []struct {
			name      string
			in        string
			out       *clusterConfig
			expectErr bool
		}{
			{
				name: "Empty document map should return nothing successfully",
			},
			{
				name: "Non-empty document map without the proper API group returns nothing successfully",
				in: dedent.Dedent(`
					apiVersion: api.example.com/v1
					kind: Configuration
				`),
			},
			{
				name: "Old config version returns an error",
				in: dedent.Dedent(`
					apiVersion: kubeadm.k8s.io/v1alpha1
					kind: ClusterConfiguration
				`),
				expectErr: true,
			},
			{
				name: "Unknown kind returns an error",
				in: dedent.Dedent(fmt.Sprintf(`
					apiVersion: %s
					kind: Configuration
				`, kubeadmapiv1.SchemeGroupVersion.String())),
				expectErr: true,
			},
			{
				name: "Valid config gets loaded",
				in:   validUnmarshallableClusterConfig.yaml,
				out: &clusterConfig{
					configBase: configBase{
						GroupVersion: clusterConfigHandler.GroupVersion,
						userSupplied: true,
					},
					config: validUnmarshallableClusterConfig.obj,
				},
			},
			{
				name: "Valid config gets loaded even if coupled with an extra document",
				in:   "apiVersion: api.example.com/v1\nkind: Configuration\n---\n" + validUnmarshallableClusterConfig.yaml,
				out: &clusterConfig{
					configBase: configBase{
						GroupVersion: clusterConfigHandler.GroupVersion,
						userSupplied: true,
					},
					config: validUnmarshallableClusterConfig.obj,
				},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				componentCfg, err := perform(t, test.in)
				if err != nil {
					if !test.expectErr {
						t.Errorf("unexpected failure: %v", err)
					}
				} else {
					if test.expectErr {
						t.Error("unexpected success")
					} else {
						if componentCfg == nil {
							if test.out != nil {
								t.Error("unexpected nil result")
							}
						} else {
							if got, ok := componentCfg.(*clusterConfig); !ok {
								t.Error("different result type")
							} else {
								if test.out == nil {
									t.Errorf("unexpected result: %v", got)
								} else {
									if !reflect.DeepEqual(test.out, got) {
										t.Errorf("mismatch between expected and got:\nExpected:\n%v\n---\nGot:\n%v", test.out, got)
									}
								}
							}
						}
					}
				}
			})
		}
	})
}

func TestLoadingFromDocumentMap(t *testing.T) {
	runClusterConfigFromTest(t, func(t *testing.T, in string) (kubeadmapi.ComponentConfig, error) {
		gvkmap, err := kubeadmutil.SplitConfigDocuments([]byte(in))
		if err != nil {
			t.Fatalf("unexpected failure of SplitConfigDocuments: %v", err)
		}

		return clusterConfigHandler.FromDocumentMap(gvkmap)
	})
}

func TestLoadingFromCluster(t *testing.T) {
	runClusterConfigFromTest(t, func(t *testing.T, in string) (kubeadmapi.ComponentConfig, error) {
		client := clientsetfake.NewSimpleClientset(
			testClusterConfigMap(in, false),
		)

		return clusterConfigHandler.FromCluster(client, testClusterCfg())
	})
}

func TestGetVersionStates(t *testing.T) {
	fakeKnownContext(func() {
		versionStateCurrent := outputapiv1alpha3.ComponentConfigVersionState{
			Group:            kubeadmapiv1.GroupName,
			CurrentVersion:   currentClusterConfigVersion,
			PreferredVersion: currentClusterConfigVersion,
		}

		cases := []struct {
			desc        string
			obj         runtime.Object
			expectedErr bool
			expected    outputapiv1alpha3.ComponentConfigVersionState
		}{
			{
				desc:     "appropriate cluster object",
				obj:      testClusterConfigMap(currentFooClusterConfig, false),
				expected: versionStateCurrent,
			},
			{
				desc:        "old config returns an error",
				obj:         testClusterConfigMap(oldFooClusterConfig, false),
				expectedErr: true,
			},
			{
				desc:     "appropriate signed cluster object",
				obj:      testClusterConfigMap(currentFooClusterConfig, true),
				expected: versionStateCurrent,
			},
			{
				desc: "old signed config",
				obj:  testClusterConfigMap(oldFooClusterConfig, true),
				expected: outputapiv1alpha3.ComponentConfigVersionState{
					Group:            kubeadmapiv1.GroupName,
					CurrentVersion:   "", // 将该配置视为缺失。
					PreferredVersion: currentClusterConfigVersion,
				},
			},
		}

		for _, test := range cases {
			t.Run(test.desc, func(t *testing.T) {
				client := clientsetfake.NewSimpleClientset(test.obj)

				clusterCfg := testClusterCfg()

				got, err := GetVersionStates(clusterCfg, client)
				if err != nil && !test.expectedErr {
					t.Errorf("unexpected error: %v", err)
				}
				if err == nil {
					if test.expectedErr {
						t.Errorf("expected error not found: %v", test.expectedErr)
					}
					if len(got) != 1 {
						t.Errorf("got %d, but expected only a single result: %v", len(got), got)
					} else if got[0] != test.expected {
						t.Errorf("unexpected result:\n\texpected: %v\n\tgot: %v", test.expected, got[0])
					}
				}
			})
		}
	})
}
