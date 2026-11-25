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

package exec

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientauthenticationv1 "k8s.io/client-go/pkg/apis/clientauthentication/v1"
	clientauthenticationv1beta1 "k8s.io/client-go/pkg/apis/clientauthentication/v1beta1"
	"k8s.io/client-go/rest"
)

// restInfo 保存测试断言所关心的 rest.Client 字段。
//
// 仅抽取与 exec 插件相关信息相关的字段，避免比较整个 rest.Config。
type restInfo struct {
	host            string
	tlsClientConfig rest.TLSClientConfig
	proxyURL        string
}

// TestLoadExecCredential 表驱动验证 LoadExecCredential：从插件输出的 JSON 解析出
// ExecCredential 并转换为 rest.Config，同时覆盖正常、字段缺失与各类错误输入。
func TestLoadExecCredential(t *testing.T) {
	t.Parallel()

	// 用例覆盖：v1/v1beta1 正常路径、未设置 Config、非法 ProxyURL、缺少 Cluster、
	// 非 ExecCredential 类型以及无法解析的数据。
	tests := []struct {
		name               string
		data               []byte
		wantExecCredential runtime.Object
		wantRESTInfo       restInfo
		wantErrorPrefix    string
	}{
		{
			name: "v1 happy path",
			data: marshal(t, clientauthenticationv1.SchemeGroupVersion, &clientauthenticationv1.ExecCredential{
				Spec: clientauthenticationv1.ExecCredentialSpec{
					Cluster: &clientauthenticationv1.Cluster{
						Server:                   "https://some-server/some/path",
						TLSServerName:            "some-server-name",
						InsecureSkipTLSVerify:    true,
						CertificateAuthorityData: []byte("some-ca-data"),
						ProxyURL:                 "https://some-proxy-url:12345",
						Config: runtime.RawExtension{
							Raw: []byte(`{"apiVersion":"group/v1","kind":"PluginConfig","spec":{"names":["marshmallow","zelda"]}}`),
						},
					},
				},
			}),
			wantExecCredential: &clientauthenticationv1.ExecCredential{
				TypeMeta: metav1.TypeMeta{
					Kind:       "ExecCredential",
					APIVersion: clientauthenticationv1.SchemeGroupVersion.String(),
				},
				Spec: clientauthenticationv1.ExecCredentialSpec{
					Cluster: &clientauthenticationv1.Cluster{
						Server:                   "https://some-server/some/path",
						TLSServerName:            "some-server-name",
						InsecureSkipTLSVerify:    true,
						CertificateAuthorityData: []byte("some-ca-data"),
						ProxyURL:                 "https://some-proxy-url:12345",
						Config: runtime.RawExtension{
							Raw: []byte(`{"apiVersion":"group/v1","kind":"PluginConfig","spec":{"names":["marshmallow","zelda"]}}`),
						},
					},
				},
			},
			wantRESTInfo: restInfo{
				host: "https://some-server/some/path",
				tlsClientConfig: rest.TLSClientConfig{
					Insecure:   true,
					ServerName: "some-server-name",
					CAData:     []byte("some-ca-data"),
				},
				proxyURL: "https://some-proxy-url:12345",
			},
		},
		{
			name: "v1beta1 happy path",
			data: marshal(t, clientauthenticationv1beta1.SchemeGroupVersion, &clientauthenticationv1beta1.ExecCredential{
				Spec: clientauthenticationv1beta1.ExecCredentialSpec{
					Cluster: &clientauthenticationv1beta1.Cluster{
						Server:                   "https://some-server/some/path",
						TLSServerName:            "some-server-name",
						InsecureSkipTLSVerify:    true,
						CertificateAuthorityData: []byte("some-ca-data"),
						ProxyURL:                 "https://some-proxy-url:12345",
						Config: runtime.RawExtension{
							Raw: []byte(`{"apiVersion":"group/v1","kind":"PluginConfig","spec":{"names":["marshmallow","zelda"]}}`),
						},
					},
				},
			}),
			wantExecCredential: &clientauthenticationv1beta1.ExecCredential{
				TypeMeta: metav1.TypeMeta{
					Kind:       "ExecCredential",
					APIVersion: clientauthenticationv1beta1.SchemeGroupVersion.String(),
				},
				Spec: clientauthenticationv1beta1.ExecCredentialSpec{
					Cluster: &clientauthenticationv1beta1.Cluster{
						Server:                   "https://some-server/some/path",
						TLSServerName:            "some-server-name",
						InsecureSkipTLSVerify:    true,
						CertificateAuthorityData: []byte("some-ca-data"),
						ProxyURL:                 "https://some-proxy-url:12345",
						Config: runtime.RawExtension{
							Raw: []byte(`{"apiVersion":"group/v1","kind":"PluginConfig","spec":{"names":["marshmallow","zelda"]}}`),
						},
					},
				},
			},
			wantRESTInfo: restInfo{
				host: "https://some-server/some/path",
				tlsClientConfig: rest.TLSClientConfig{
					Insecure:   true,
					ServerName: "some-server-name",
					CAData:     []byte("some-ca-data"),
				},
				proxyURL: "https://some-proxy-url:12345",
			},
		},
		{
			name: "v1 nil config",
			data: marshal(t, clientauthenticationv1.SchemeGroupVersion, &clientauthenticationv1.ExecCredential{
				Spec: clientauthenticationv1.ExecCredentialSpec{
					Cluster: &clientauthenticationv1.Cluster{
						Server:                   "https://some-server/some/path",
						TLSServerName:            "some-server-name",
						InsecureSkipTLSVerify:    true,
						CertificateAuthorityData: []byte("some-ca-data"),
						ProxyURL:                 "https://some-proxy-url:12345",
					},
				},
			}),
			wantExecCredential: &clientauthenticationv1.ExecCredential{
				TypeMeta: metav1.TypeMeta{
					Kind:       "ExecCredential",
					APIVersion: clientauthenticationv1.SchemeGroupVersion.String(),
				},
				Spec: clientauthenticationv1.ExecCredentialSpec{
					Cluster: &clientauthenticationv1.Cluster{
						Server:                   "https://some-server/some/path",
						TLSServerName:            "some-server-name",
						InsecureSkipTLSVerify:    true,
						CertificateAuthorityData: []byte("some-ca-data"),
						ProxyURL:                 "https://some-proxy-url:12345",
					},
				},
			},
			wantRESTInfo: restInfo{
				host: "https://some-server/some/path",
				tlsClientConfig: rest.TLSClientConfig{
					Insecure:   true,
					ServerName: "some-server-name",
					CAData:     []byte("some-ca-data"),
				},
				proxyURL: "https://some-proxy-url:12345",
			},
		},
		{
			name: "v1beta1 nil config",
			data: marshal(t, clientauthenticationv1beta1.SchemeGroupVersion, &clientauthenticationv1beta1.ExecCredential{
				Spec: clientauthenticationv1beta1.ExecCredentialSpec{
					Cluster: &clientauthenticationv1beta1.Cluster{
						Server:                   "https://some-server/some/path",
						TLSServerName:            "some-server-name",
						InsecureSkipTLSVerify:    true,
						CertificateAuthorityData: []byte("some-ca-data"),
						ProxyURL:                 "https://some-proxy-url:12345",
					},
				},
			}),
			wantExecCredential: &clientauthenticationv1beta1.ExecCredential{
				TypeMeta: metav1.TypeMeta{
					Kind:       "ExecCredential",
					APIVersion: clientauthenticationv1beta1.SchemeGroupVersion.String(),
				},
				Spec: clientauthenticationv1beta1.ExecCredentialSpec{
					Cluster: &clientauthenticationv1beta1.Cluster{
						Server:                   "https://some-server/some/path",
						TLSServerName:            "some-server-name",
						InsecureSkipTLSVerify:    true,
						CertificateAuthorityData: []byte("some-ca-data"),
						ProxyURL:                 "https://some-proxy-url:12345",
					},
				},
			},
			wantRESTInfo: restInfo{
				host: "https://some-server/some/path",
				tlsClientConfig: rest.TLSClientConfig{
					Insecure:   true,
					ServerName: "some-server-name",
					CAData:     []byte("some-ca-data"),
				},
				proxyURL: "https://some-proxy-url:12345",
			},
		},
		{
			name: "v1 invalid cluster",
			data: marshal(t, clientauthenticationv1.SchemeGroupVersion, &clientauthenticationv1.ExecCredential{
				Spec: clientauthenticationv1.ExecCredentialSpec{
					Cluster: &clientauthenticationv1.Cluster{
						ProxyURL: "invalid- url\n",
					},
				},
			}),
			wantErrorPrefix: "cannot create rest.Config",
		},
		{
			name: "v1beta1 invalid cluster",
			data: marshal(t, clientauthenticationv1beta1.SchemeGroupVersion, &clientauthenticationv1beta1.ExecCredential{
				Spec: clientauthenticationv1beta1.ExecCredentialSpec{
					Cluster: &clientauthenticationv1beta1.Cluster{
						ProxyURL: "invalid- url\n",
					},
				},
			}),
			wantErrorPrefix: "cannot create rest.Config",
		},
		{
			name:            "v1 nil cluster",
			data:            marshal(t, clientauthenticationv1.SchemeGroupVersion, &clientauthenticationv1.ExecCredential{}),
			wantErrorPrefix: "ExecCredential does not contain cluster information",
		},
		{
			name:            "v1beta1 nil cluster",
			data:            marshal(t, clientauthenticationv1beta1.SchemeGroupVersion, &clientauthenticationv1beta1.ExecCredential{}),
			wantErrorPrefix: "ExecCredential does not contain cluster information",
		},
		{
			name:            "invalid object kind",
			data:            marshal(t, metav1.SchemeGroupVersion, &metav1.Status{}),
			wantErrorPrefix: "invalid group/kind: wanted ExecCredential.client.authentication.k8s.io, got Status",
		},
		{
			name:            "bad data",
			data:            []byte("bad data"),
			wantErrorPrefix: "decode: ",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			// 错误分支只校验错误前缀，避免测试与具体措辞耦合。
			execCredential, restConfig, err := LoadExecCredential(test.data)
			if test.wantErrorPrefix != "" {
				if err == nil {
					t.Error("wanted error, got success")
				} else if !strings.HasPrefix(err.Error(), test.wantErrorPrefix) {
					t.Errorf("wanted '%s', got '%s'", test.wantErrorPrefix, err.Error())
				}
			} else if err != nil {
				t.Error(err)
			} else {
				// 分别比较序列化对象、Host、TLS 配置与解析后的代理 URL。
				if diff := cmp.Diff(test.wantExecCredential, execCredential); diff != "" {
					t.Error(diff)
				}

				if diff := cmp.Diff(test.wantRESTInfo.host, restConfig.Host); diff != "" {
					t.Error(diff)
				}
				if diff := cmp.Diff(test.wantRESTInfo.tlsClientConfig, restConfig.TLSClientConfig); diff != "" {
					t.Error(diff)
				}

				proxyURL, err := restConfig.Proxy(nil)
				if err != nil {
					t.Fatal(err)
				}
				if diff := cmp.Diff(test.wantRESTInfo.proxyURL, proxyURL.String()); diff != "" {
					t.Error(diff)
				}
			}
		})
	}
}

// marshal 按给定 GroupVersion 序列化对象，用于构造插件 stdout 的字节内容。
func marshal(t *testing.T, gv schema.GroupVersion, obj runtime.Object) []byte {
	t.Helper()

	data, err := runtime.Encode(codecs.LegacyCodec(gv), obj)
	if err != nil {
		t.Fatal(err)
	}

	return data
}
