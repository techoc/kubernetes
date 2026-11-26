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

package exec

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	mathrand "math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/pkg/apis/clientauthentication"
	"k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/transport"
	testingclock "k8s.io/utils/clock/testing"
)

// 测试用的一对 PEM 证书/私钥；init 中解析一次供所有用例复用。
var (
	certData = []byte(`-----BEGIN CERTIFICATE-----
MIIC6jCCAdSgAwIBAgIBCzALBgkqhkiG9w0BAQswIzEhMB8GA1UEAwwYMTAuMTMu
MTI5LjEwNkAxNDIxMzU5MDU4MB4XDTE1MDExNTIyMDEzMVoXDTE2MDExNTIyMDEz
MlowGzEZMBcGA1UEAxMQb3BlbnNoaWZ0LWNsaWVudDCCASIwDQYJKoZIhvcNAQEB
BQADggEPADCCAQoCggEBAKtdhz0+uCLXw5cSYns9rU/XifFSpb/x24WDdrm72S/v
b9BPYsAStiP148buylr1SOuNi8sTAZmlVDDIpIVwMLff+o2rKYDicn9fjbrTxTOj
lI4pHJBH+JU3AJ0tbajupioh70jwFS0oYpwtneg2zcnE2Z4l6mhrj2okrc5Q1/X2
I2HChtIU4JYTisObtin10QKJX01CLfYXJLa8upWzKZ4/GOcHG+eAV3jXWoXidtjb
1Usw70amoTZ6mIVCkiu1QwCoa8+ycojGfZhvqMsAp1536ZcCul+Na+AbCv4zKS7F
kQQaImVrXdUiFansIoofGlw/JNuoKK6ssVpS5Ic3pgcCAwEAAaM1MDMwDgYDVR0P
AQH/BAQDAgCgMBMGA1UdJQQMMAoGCCsGAQUFBwMCMAwGA1UdEwEB/wQCMAAwCwYJ
KoZIhvcNAQELA4IBAQCKLREH7bXtXtZ+8vI6cjD7W3QikiArGqbl36bAhhWsJLp/
p/ndKz39iFNaiZ3GlwIURWOOKx3y3GA0x9m8FR+Llthf0EQ8sUjnwaknWs0Y6DQ3
jjPFZOpV3KPCFrdMJ3++E3MgwFC/Ih/N2ebFX9EcV9Vcc6oVWMdwT0fsrhu683rq
6GSR/3iVX1G/pmOiuaR0fNUaCyCfYrnI4zHBDgSfnlm3vIvN2lrsR/DQBakNL8DJ
HBgKxMGeUPoneBv+c8DMXIL0EhaFXRlBv9QW45/GiAIOuyFJ0i6hCtGZpJjq4OpQ
BRjCI+izPzFTjsxD4aORE+WOkyWFCGPWKfNejfw0
-----END CERTIFICATE-----`)
	keyData = []byte(`-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAq12HPT64ItfDlxJiez2tT9eJ8VKlv/HbhYN2ubvZL+9v0E9i
wBK2I/Xjxu7KWvVI642LyxMBmaVUMMikhXAwt9/6jaspgOJyf1+NutPFM6OUjikc
kEf4lTcAnS1tqO6mKiHvSPAVLShinC2d6DbNycTZniXqaGuPaiStzlDX9fYjYcKG
0hTglhOKw5u2KfXRAolfTUIt9hcktry6lbMpnj8Y5wcb54BXeNdaheJ22NvVSzDv
RqahNnqYhUKSK7VDAKhrz7JyiMZ9mG+oywCnXnfplwK6X41r4BsK/jMpLsWRBBoi
ZWtd1SIVqewiih8aXD8k26gorqyxWlLkhzemBwIDAQABAoIBAD2XYRs3JrGHQUpU
FkdbVKZkvrSY0vAZOqBTLuH0zUv4UATb8487anGkWBjRDLQCgxH+jucPTrztekQK
aW94clo0S3aNtV4YhbSYIHWs1a0It0UdK6ID7CmdWkAj6s0T8W8lQT7C46mWYVLm
5mFnCTHi6aB42jZrqmEpC7sivWwuU0xqj3Ml8kkxQCGmyc9JjmCB4OrFFC8NNt6M
ObvQkUI6Z3nO4phTbpxkE1/9dT0MmPIF7GhHVzJMS+EyyRYUDllZ0wvVSOM3qZT0
JMUaBerkNwm9foKJ1+dv2nMKZZbJajv7suUDCfU44mVeaEO+4kmTKSGCGjjTBGkr
7L1ySDECgYEA5ElIMhpdBzIivCuBIH8LlUeuzd93pqssO1G2Xg0jHtfM4tz7fyeI
cr90dc8gpli24dkSxzLeg3Tn3wIj/Bu64m2TpZPZEIlukYvgdgArmRIPQVxerYey
OkrfTNkxU1HXsYjLCdGcGXs5lmb+K/kuTcFxaMOs7jZi7La+jEONwf8CgYEAwCs/
rUOOA0klDsWWisbivOiNPII79c9McZCNBqncCBfMUoiGe8uWDEO4TFHN60vFuVk9
8PkwpCfvaBUX+ajvbafIfHxsnfk1M04WLGCeqQ/ym5Q4sQoQOcC1b1y9qc/xEWfg
nIUuia0ukYRpl7qQa3tNg+BNFyjypW8zukUAC/kCgYB1/Kojuxx5q5/oQVPrx73k
2bevD+B3c+DYh9MJqSCNwFtUpYIWpggPxoQan4LwdsmO0PKzocb/ilyNFj4i/vII
NToqSc/WjDFpaDIKyuu9oWfhECye45NqLWhb/6VOuu4QA/Nsj7luMhIBehnEAHW+
GkzTKM8oD1PxpEG3nPKXYQKBgQC6AuMPRt3XBl1NkCrpSBy/uObFlFaP2Enpf39S
3OZ0Gv0XQrnSaL1kP8TMcz68rMrGX8DaWYsgytstR4W+jyy7WvZwsUu+GjTJ5aMG
77uEcEBpIi9CBzivfn7hPccE8ZgqPf+n4i6q66yxBJflW5xhvafJqDtW2LcPNbW/
bvzdmQKBgExALRUXpq+5dbmkdXBHtvXdRDZ6rVmrnjy4nI5bPw+1GqQqk6uAR6B/
F6NmLCQOO4PDG/cuatNHIr2FrwTmGdEL6ObLUGWn9Oer9gJhHVqqsY5I4sEPo4XX
stR0Yiw0buV6DL/moUO0HIM9Bjh96HJp+LxiIS6UCdIhMPp5HoQa
-----END RSA PRIVATE KEY-----`)
	validCert *tls.Certificate
)

func init() {
	// 提前解析叶子证书，便于用例直接比较 credentials.cert。
	cert, err := tls.X509KeyPair(certData, keyData)
	if err != nil {
		panic(err)
	}
	cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		panic(err)
	}
	validCert = &cert
}

// TestCacheKey 验证 authenticator 缓存键的相等/区分语义：命令、参数、环境变量、
// cluster 信息与 interactive/stdin 状态任一不同都应产生不同 key；完全相同的配置
// 必须命中同一 key。
func TestCacheKey(t *testing.T) {
	c1 := &api.ExecConfig{
		Command: "foo-bar",
		Args:    []string{"1", "2"},
		Env: []api.ExecEnvVar{
			{Name: "3", Value: "4"},
			{Name: "5", Value: "6"},
			{Name: "7", Value: "8"},
		},
		APIVersion:         "client.authentication.k8s.io/v1beta1",
		ProvideClusterInfo: true,
	}
	c1c := &clientauthentication.Cluster{
		Server:                   "foo",
		TLSServerName:            "bar",
		CertificateAuthorityData: []byte("baz"),
		Config: &runtime.Unknown{
			TypeMeta: runtime.TypeMeta{
				APIVersion: "",
				Kind:       "",
			},
			Raw:             []byte(`{"apiVersion":"group/v1","kind":"PluginConfig","spec":{"audience":"snorlax"}}`),
			ContentEncoding: "",
			ContentType:     "application/json",
		},
	}

	c2 := &api.ExecConfig{
		Command: "foo-bar",
		Args:    []string{"1", "2"},
		Env: []api.ExecEnvVar{
			{Name: "3", Value: "4"},
			{Name: "5", Value: "6"},
			{Name: "7", Value: "8"},
		},
		APIVersion:         "client.authentication.k8s.io/v1beta1",
		ProvideClusterInfo: true,
	}
	c2c := &clientauthentication.Cluster{
		Server:                   "foo",
		TLSServerName:            "bar",
		CertificateAuthorityData: []byte("baz"),
		Config: &runtime.Unknown{
			TypeMeta: runtime.TypeMeta{
				APIVersion: "",
				Kind:       "",
			},
			Raw:             []byte(`{"apiVersion":"group/v1","kind":"PluginConfig","spec":{"audience":"snorlax"}}`),
			ContentEncoding: "",
			ContentType:     "application/json",
		},
	}

	c3 := &api.ExecConfig{
		Command: "foo-bar",
		Args:    []string{"1", "2"},
		Env: []api.ExecEnvVar{
			{Name: "3", Value: "4"},
			{Name: "5", Value: "6"},
		},
		APIVersion: "client.authentication.k8s.io/v1beta1",
	}
	c3c := &clientauthentication.Cluster{
		Server:                   "foo",
		TLSServerName:            "bar",
		CertificateAuthorityData: []byte("baz"),
		Config: &runtime.Unknown{
			TypeMeta: runtime.TypeMeta{
				APIVersion: "",
				Kind:       "",
			},
			Raw:             []byte(`{"apiVersion":"group/v1","kind":"PluginConfig","spec":{"audience":"snorlax"}}`),
			ContentEncoding: "",
			ContentType:     "application/json",
		},
	}

	c4 := &api.ExecConfig{
		Command: "foo-bar",
		Args:    []string{"1", "2"},
		Env: []api.ExecEnvVar{
			{Name: "3", Value: "4"},
			{Name: "5", Value: "6"},
		},
		APIVersion: "client.authentication.k8s.io/v1beta1",
	}
	c4c := &clientauthentication.Cluster{
		Server:                   "foo",
		TLSServerName:            "bar",
		CertificateAuthorityData: []byte("baz"),
		Config: &runtime.Unknown{
			TypeMeta: runtime.TypeMeta{
				APIVersion: "",
				Kind:       "",
			},
			Raw:             []byte(`{"apiVersion":"group/v1","kind":"PluginConfig","spec":{"audience":"panda"}}`),
			ContentEncoding: "",
			ContentType:     "application/json",
		},
	}

	// c5/c5c 应与 c4/c4c 相同，除了 c5 的 ProvideClusterInfo 设为 true。
	c5 := &api.ExecConfig{
		Command: "foo-bar",
		Args:    []string{"1", "2"},
		Env: []api.ExecEnvVar{
			{Name: "3", Value: "4"},
			{Name: "5", Value: "6"},
		},
		APIVersion:         "client.authentication.k8s.io/v1beta1",
		ProvideClusterInfo: true,
	}
	c5c := &clientauthentication.Cluster{
		Server:                   "foo",
		TLSServerName:            "bar",
		CertificateAuthorityData: []byte("baz"),
		Config: &runtime.Unknown{
			TypeMeta: runtime.TypeMeta{
				APIVersion: "",
				Kind:       "",
			},
			Raw:             []byte(`{"apiVersion":"group/v1","kind":"PluginConfig","spec":{"audience":"panda"}}`),
			ContentEncoding: "",
			ContentType:     "application/json",
		},
	}

	// c6 应与 c4 相同，除了 c6 的 cluster 为 nil。
	c6 := &api.ExecConfig{
		Command: "foo-bar",
		Args:    []string{"1", "2"},
		Env: []api.ExecEnvVar{
			{Name: "3", Value: "4"},
			{Name: "5", Value: "6"},
		},
		APIVersion: "client.authentication.k8s.io/v1betaa1",
	}

	// c7 应与 c6 相同，除了 c7 的 stdin 标记为不可用。
	c7 := &api.ExecConfig{
		Command: "foo-bar",
		Args:    []string{"1", "2"},
		Env: []api.ExecEnvVar{
			{Name: "3", Value: "4"},
			{Name: "5", Value: "6"},
		},
		APIVersion:       "client.authentication.k8s.io/v1beta1",
		StdinUnavailable: true,
	}

	key1 := cacheKey(c1, c1c)
	key2 := cacheKey(c2, c2c)
	key3 := cacheKey(c3, c3c)
	key4 := cacheKey(c4, c4c)
	key5 := cacheKey(c5, c5c)
	key6 := cacheKey(c6, nil)
	key7 := cacheKey(c7, nil)
	if key1 != key2 {
		t.Error("key1 and key2 didn't match")
	}
	if key1 == key3 {
		t.Error("key1 and key3 matched")
	}
	if key2 == key3 {
		t.Error("key2 and key3 matched")
	}
	if key3 == key4 {
		t.Error("key3 and key4 matched")
	}
	if key4 == key5 {
		t.Error("key3 and key4 matched")
	}
	if key6 == key4 {
		t.Error("key6 and key4 matched")
	}
	if key6 == key7 {
		t.Error("key6 and key7 matched")
	}
}

// compJSON 比较两段 JSON 的语义内容（忽略空白与缩进），失败时打印格式化结果。
func compJSON(t *testing.T, got, want []byte) {
	t.Helper()
	gotJSON := &bytes.Buffer{}
	wantJSON := &bytes.Buffer{}

	if err := json.Indent(gotJSON, got, "", "  "); err != nil {
		t.Errorf("got invalid JSON: %v", err)
	}
	if err := json.Indent(wantJSON, want, "", "  "); err != nil {
		t.Errorf("want invalid JSON: %v", err)
	}
	g := strings.TrimSpace(gotJSON.String())
	w := strings.TrimSpace(wantJSON.String())
	if g != w {
		t.Errorf("wanted %q, got %q", w, g)
	}
}

// TestRefreshCreds 以表驱动方式驱动 test-plugin.sh（通过 TEST_OUTPUT/
// TEST_EXIT_CODE 控制输出），覆盖各 API 版本的凭据解析、TLS 凭据校验、
// interactive 语义、过期时间以及各类错误路径。
func TestRefreshCreds(t *testing.T) {
	tests := []struct {
		name             string
		config           api.ExecConfig
		stdinUnavailable bool
		exitCode         int
		cluster          *clientauthentication.Cluster
		output           string
		isTerminal       bool
		wantInput        string
		wantCreds        credentials
		wantExpiry       time.Time
		wantErr          bool
		wantErrSubstr    string
	}{
		{
			name: "beta-with-TLS-credentials",
			config: api.ExecConfig{
				APIVersion:      "client.authentication.k8s.io/v1beta1",
				InteractiveMode: api.IfAvailableExecInteractiveMode,
			},
			wantInput: `{
				"kind":"ExecCredential",
				"apiVersion":"client.authentication.k8s.io/v1beta1",
				"spec": {
					"interactive": false
				}
			}`,
			output: fmt.Sprintf(`{
				"kind": "ExecCredential",
				"apiVersion": "client.authentication.k8s.io/v1beta1",
				"status": {
					"clientKeyData": %q,
					"clientCertificateData": %q
				}
			}`, keyData, certData),
			wantCreds: credentials{cert: validCert},
		},
		{
			name: "beta-with-bad-TLS-credentials",
			config: api.ExecConfig{
				APIVersion:      "client.authentication.k8s.io/v1beta1",
				InteractiveMode: api.IfAvailableExecInteractiveMode,
			},
			output: `{
				"kind": "ExecCredential",
				"apiVersion": "client.authentication.k8s.io/v1beta1",
				"status": {
					"clientKeyData": "foo",
					"clientCertificateData": "bar"
				}
			}`,
			wantErr: true,
		},
		{
			name: "beta-cert-but-no-key",
			config: api.ExecConfig{
				APIVersion:      "client.authentication.k8s.io/v1beta1",
				InteractiveMode: api.IfAvailableExecInteractiveMode,
			},
			output: fmt.Sprintf(`{
				"kind": "ExecCredential",
				"apiVersion": "client.authentication.k8s.io/v1beta1",
				"status": {
					"clientCertificateData": %q
				}
			}`, certData),
			wantErr: true,
		},
		{
			name: "beta-basic-request",
			config: api.ExecConfig{
				APIVersion:      "client.authentication.k8s.io/v1beta1",
				InteractiveMode: api.IfAvailableExecInteractiveMode,
			},
			wantInput: `{
				"kind": "ExecCredential",
				"apiVersion": "client.authentication.k8s.io/v1beta1",
				"spec": {
					"interactive": false
				}
			}`,
			output: `{
				"kind": "ExecCredential",
				"apiVersion": "client.authentication.k8s.io/v1beta1",
				"status": {
					"token": "foo-bar"
				}
			}`,
			wantCreds: credentials{token: "foo-bar"},
		},
		{
			name: "beta-basic-request-with-never-interactive-mode",
			config: api.ExecConfig{
				APIVersion:      "client.authentication.k8s.io/v1beta1",
				InteractiveMode: api.NeverExecInteractiveMode,
			},
			wantInput: `{
				"kind": "ExecCredential",
				"apiVersion": "client.authentication.k8s.io/v1beta1",
				"spec": {
					"interactive": false
				}
			}`,
			output: `{
				"kind": "ExecCredential",
				"apiVersion": "client.authentication.k8s.io/v1beta1",
				"status": {
					"token": "foo-bar"
				}
			}`,
			wantCreds: credentials{token: "foo-bar"},
		},
		{
			name: "beta-basic-request-with-never-interactive-mode-and-stdin-unavailable",
			config: api.ExecConfig{
				APIVersion:       "client.authentication.k8s.io/v1beta1",
				InteractiveMode:  api.NeverExecInteractiveMode,
				StdinUnavailable: true,
			},
			wantInput: `{
				"kind": "ExecCredential",
				"apiVersion": "client.authentication.k8s.io/v1beta1",
				"spec": {
					"interactive": false
				}
			}`,
			output: `{
				"kind": "ExecCredential",
				"apiVersion": "client.authentication.k8s.io/v1beta1",
				"status": {
					"token": "foo-bar"
				}
			}`,
			wantCreds: credentials{token: "foo-bar"},
		},
		{
			name: "beta-basic-request-with-if-available-interactive-mode",
			config: api.ExecConfig{
				APIVersion:      "client.authentication.k8s.io/v1beta1",
				InteractiveMode: api.IfAvailableExecInteractiveMode,
			},
			wantInput: `{
				"kind": "ExecCredential",
				"apiVersion": "client.authentication.k8s.io/v1beta1",
				"spec": {
					"interactive": false
				}
			}`,
			output: `{
				"kind": "ExecCredential",
				"apiVersion": "client.authentication.k8s.io/v1beta1",
				"status": {
					"token": "foo-bar"
				}
			}`,
			wantCreds: credentials{token: "foo-bar"},
		},
		{
			name: "beta-basic-request-with-if-available-interactive-mode-and-stdin-unavailable",
			config: api.ExecConfig{
				APIVersion:       "client.authentication.k8s.io/v1beta1",
				InteractiveMode:  api.IfAvailableExecInteractiveMode,
				StdinUnavailable: true,
			},
			wantInput: `{
				"kind": "ExecCredential",
				"apiVersion": "client.authentication.k8s.io/v1beta1",
				"spec": {
					"interactive": false
				}
			}`,
			output: `{
				"kind": "ExecCredential",
				"apiVersion": "client.authentication.k8s.io/v1beta1",
				"status": {
					"token": "foo-bar"
				}
			}`,
			wantCreds: credentials{token: "foo-bar"},
		},
		{
			name: "beta-basic-request-with-if-available-interactive-mode-and-terminal",
			config: api.ExecConfig{
				APIVersion:      "client.authentication.k8s.io/v1beta1",
				InteractiveMode: api.IfAvailableExecInteractiveMode,
			},
			isTerminal: true,
			wantInput: `{
				"kind": "ExecCredential",
				"apiVersion": "client.authentication.k8s.io/v1beta1",
				"spec": {
					"interactive": true
				}
			}`,
			output: `{
				"kind": "ExecCredential",
				"apiVersion": "client.authentication.k8s.io/v1beta1",
				"status": {
					"token": "foo-bar"
				}
			}`,
			wantCreds: credentials{token: "foo-bar"},
		},
		{
			name: "beta-basic-request-with-if-available-interactive-mode-and-terminal-and-stdin-unavailable",
			config: api.ExecConfig{
				APIVersion:       "client.authentication.k8s.io/v1beta1",
				InteractiveMode:  api.IfAvailableExecInteractiveMode,
				StdinUnavailable: true,
			},
			isTerminal: true,
			wantInput: `{
				"kind": "ExecCredential",
				"apiVersion": "client.authentication.k8s.io/v1beta1",
				"spec": {
					"interactive": false
				}
			}`,
			output: `{
				"kind": "ExecCredential",
				"apiVersion": "client.authentication.k8s.io/v1beta1",
				"status": {
					"token": "foo-bar"
				}
			}`,
			wantCreds: credentials{token: "foo-bar"},
		},
		{
			name: "beta-basic-request-with-always-interactive-mode",
			config: api.ExecConfig{
				APIVersion:      "client.authentication.k8s.io/v1beta1",
				InteractiveMode: api.AlwaysExecInteractiveMode,
			},
			wantErr:       true,
			wantErrSubstr: "exec plugin cannot support interactive mode: standard input is not a terminal",
		},
		{
			name: "beta-basic-request-with-always-interactive-mode-and-terminal-and-stdin-unavailable",
			config: api.ExecConfig{
				APIVersion:       "client.authentication.k8s.io/v1beta1",
				InteractiveMode:  api.AlwaysExecInteractiveMode,
				StdinUnavailable: true,
			},
			isTerminal:    true,
			wantErr:       true,
			wantErrSubstr: "exec plugin cannot support interactive mode: standard input is unavailable",
		},
		{
			name: "beta-basic-request-with-always-interactive-mode-and-terminal-and-stdin-unavailable-with-message",
			config: api.ExecConfig{
				APIVersion:              "client.authentication.k8s.io/v1beta1",
				InteractiveMode:         api.AlwaysExecInteractiveMode,
				StdinUnavailable:        true,
				StdinUnavailableMessage: "some message",
			},
			isTerminal:    true,
			wantErr:       true,
			wantErrSubstr: "exec plugin cannot support interactive mode: standard input is unavailable: some message",
		},
		{
			name: "beta-basic-request-with-always-interactive-mode-and-terminal",
			config: api.ExecConfig{
				APIVersion:      "client.authentication.k8s.io/v1beta1",
				InteractiveMode: api.AlwaysExecInteractiveMode,
			},
			isTerminal: true,
			wantInput: `{
				"kind": "ExecCredential",
				"apiVersion": "client.authentication.k8s.io/v1beta1",
				"spec": {
					"interactive": true
				}
			}`,
			output: `{
				"kind": "ExecCredential",
				"apiVersion": "client.authentication.k8s.io/v1beta1",
				"status": {
					"token": "foo-bar"
				}
			}`,
			wantCreds: credentials{token: "foo-bar"},
		},
		{
			name: "beta-expiry",
			config: api.ExecConfig{
				APIVersion:      "client.authentication.k8s.io/v1beta1",
				InteractiveMode: api.IfAvailableExecInteractiveMode,
			},
			wantInput: `{
				"kind": "ExecCredential",
				"apiVersion": "client.authentication.k8s.io/v1beta1",
				"spec": {
					"interactive": false
				}
			}`,
			output: `{
				"kind": "ExecCredential",
				"apiVersion": "client.authentication.k8s.io/v1beta1",
				"status": {
					"token": "foo-bar",
					"expirationTimestamp": "2006-01-02T15:04:05Z"
				}
			}`,
			wantExpiry: time.Date(2006, 01, 02, 15, 04, 05, 0, time.UTC),
			wantCreds:  credentials{token: "foo-bar"},
		},
		{
			name: "beta-no-group-version",
			config: api.ExecConfig{
				APIVersion:      "client.authentication.k8s.io/v1beta1",
				InteractiveMode: api.IfAvailableExecInteractiveMode,
			},
			output: `{
				"kind": "ExecCredential",
				"status": {
					"token": "foo-bar"
				}
			}`,
			wantErr: true,
		},
		{
			name: "beta-no-status",
			config: api.ExecConfig{
				APIVersion:      "client.authentication.k8s.io/v1beta1",
				InteractiveMode: api.IfAvailableExecInteractiveMode,
			},
			output: `{
				"kind": "ExecCredential",
				"apiVersion":"client.authentication.k8s.io/v1beta1"
			}`,
			wantErr: true,
		},
		{
			name: "beta-no-token",
			config: api.ExecConfig{
				APIVersion:      "client.authentication.k8s.io/v1beta1",
				InteractiveMode: api.IfAvailableExecInteractiveMode,
			},
			output: `{
				"kind": "ExecCredential",
				"apiVersion":"client.authentication.k8s.io/v1beta1",
				"status": {}
			}`,
			wantErr: true,
		},
		{
			name: "unknown-binary",
			config: api.ExecConfig{
				APIVersion:      "client.authentication.k8s.io/v1beta1",
				Command:         "does not exist",
				InstallHint:     "some install hint",
				InteractiveMode: api.IfAvailableExecInteractiveMode,
			},
			wantErr:       true,
			wantErrSubstr: "some install hint",
		},
		{
			name: "binary-fails",
			config: api.ExecConfig{
				APIVersion:      "client.authentication.k8s.io/v1beta1",
				InteractiveMode: api.IfAvailableExecInteractiveMode,
			},
			exitCode:      73,
			wantErr:       true,
			wantErrSubstr: "73",
		},
		{
			name: "beta-with-cluster-and-provide-cluster-info-is-serialized",
			config: api.ExecConfig{
				APIVersion:         "client.authentication.k8s.io/v1beta1",
				ProvideClusterInfo: true,
				InteractiveMode:    api.IfAvailableExecInteractiveMode,
			},
			cluster: &clientauthentication.Cluster{
				Server:                   "foo",
				TLSServerName:            "bar",
				CertificateAuthorityData: []byte("baz"),
				Config: &runtime.Unknown{
					TypeMeta: runtime.TypeMeta{
						APIVersion: "",
						Kind:       "",
					},
					Raw:             []byte(`{"apiVersion":"group/v1","kind":"PluginConfig","spec":{"audience":"snorlax"}}`),
					ContentEncoding: "",
					ContentType:     "application/json",
				},
			},
			wantInput: `{
				"kind":"ExecCredential",
				"apiVersion":"client.authentication.k8s.io/v1beta1",
				"spec": {
					"cluster": {
						"server": "foo",
						"tls-server-name": "bar",
						"certificate-authority-data": "YmF6",
						"config": {
							"apiVersion": "group/v1",
							"kind": "PluginConfig",
							"spec": {
								"audience": "snorlax"
							}
						}
					},
					"interactive": false
				}
			}`,
			output: `{
				"kind": "ExecCredential",
				"apiVersion": "client.authentication.k8s.io/v1beta1",
				"status": {
					"token": "foo-bar"
				}
			}`,
			wantCreds: credentials{token: "foo-bar"},
		},
		{
			name: "beta-with-cluster-and-without-provide-cluster-info-is-not-serialized",
			config: api.ExecConfig{
				APIVersion:      "client.authentication.k8s.io/v1beta1",
				InteractiveMode: api.IfAvailableExecInteractiveMode,
			},
			cluster: &clientauthentication.Cluster{
				Server:                   "foo",
				TLSServerName:            "bar",
				CertificateAuthorityData: []byte("baz"),
				Config: &runtime.Unknown{
					TypeMeta: runtime.TypeMeta{
						APIVersion: "",
						Kind:       "",
					},
					Raw:             []byte(`{"apiVersion":"group/v1","kind":"PluginConfig","spec":{"audience":"snorlax"}}`),
					ContentEncoding: "",
					ContentType:     "application/json",
				},
			},
			wantInput: `{
				"kind":"ExecCredential",
				"apiVersion":"client.authentication.k8s.io/v1beta1",
				"spec": {
					"interactive": false
				}
			}`,
			output: `{
				"kind": "ExecCredential",
				"apiVersion": "client.authentication.k8s.io/v1beta1",
				"status": {
					"token": "foo-bar"
				}
			}`,
			wantCreds: credentials{token: "foo-bar"},
		},
		{
			name: "v1-basic-request",
			config: api.ExecConfig{
				APIVersion:      "client.authentication.k8s.io/v1",
				InteractiveMode: api.IfAvailableExecInteractiveMode,
			},
			wantInput: `{
				"kind": "ExecCredential",
				"apiVersion": "client.authentication.k8s.io/v1",
				"spec": {
					"interactive": false
				}
			}`,
			output: `{
				"kind": "ExecCredential",
				"apiVersion": "client.authentication.k8s.io/v1",
				"status": {
					"token": "foo-bar"
				}
			}`,
			wantCreds: credentials{token: "foo-bar"},
		},
		{
			name: "v1-with-missing-interactive-mode",
			config: api.ExecConfig{
				APIVersion: "client.authentication.k8s.io/v1",
			},
			wantErr:       true,
			wantErrSubstr: `exec plugin cannot support interactive mode: unknown interactiveMode: ""`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := test.config

			if c.Command == "" {
				c.Command = "./testdata/test-plugin.sh"
				c.Env = append(c.Env, api.ExecEnvVar{
					Name:  "TEST_OUTPUT",
					Value: test.output,
				})
				c.Env = append(c.Env, api.ExecEnvVar{
					Name:  "TEST_EXIT_CODE",
					Value: strconv.Itoa(test.exitCode),
				})
			}

			a, err := newAuthenticator(newCache(), func(_ int) bool { return test.isTerminal }, &c, test.cluster)
			if err != nil {
				t.Fatal(err)
			}

			stderr := &bytes.Buffer{}
			a.stderr = stderr
			a.environ = func() []string { return nil }

			if err := a.refreshCredsLocked(); err != nil {
				if !test.wantErr {
					t.Errorf("get token %v", err)
				} else if !strings.Contains(err.Error(), test.wantErrSubstr) {
					t.Errorf("expected error with substring '%v' got '%v'", test.wantErrSubstr, err.Error())
				}
				return
			}
			if test.wantErr {
				t.Fatal("expected error getting token")
			}

			if !reflect.DeepEqual(a.cachedCreds, &test.wantCreds) {
				t.Errorf("expected credentials %+v got %+v", &test.wantCreds, a.cachedCreds)
			}

			if !a.exp.Equal(test.wantExpiry) {
				t.Errorf("expected expiry %v got %v", test.wantExpiry, a.exp)
			}

			if test.wantInput == "" {
				if got := strings.TrimSpace(stderr.String()); got != "" {
					t.Errorf("expected no input parameters, got %q", got)
				}
				return
			}

			compJSON(t, stderr.Bytes(), []byte(test.wantInput))
		})
	}
}

// pluginPolicyTest 描述一次凭据插件策略测试的全部输入与期望。
type pluginPolicyTest struct {
	name             string
	wantErr          bool
	wantErrSubstr    string
	config           *api.ExecConfig
	pluginExists     bool
	entryExists      bool
	usePluginAbsPath bool
	useEntryAbsPath  bool
	allowlistLength  int
	policyType       api.PolicyType
	allowlist        []api.AllowlistEntry
}

// dualBool 同时表示“插件侧”和“allowlist 条目侧”的布尔维度。
type dualBool struct{ plugin, entry bool }

// pluginPolicyTestMatrix 定义需要组合遍历的维度，以及判定期望错误的规则。
type pluginPolicyTestMatrix struct {
	exists           []dualBool
	absolute         []dualBool
	allowlistLengths []int
	policies         []api.PolicyType

	// 判断给定测试配置是否应产生错误的逻辑。
	shouldErrFunc func(tt *pluginPolicyTest) (bool, string)
}

// allPermutations 遍历 plugin/entry 是否存在（或是否为绝对路径）的四种组合。
var allPermutations = []dualBool{
	{plugin: false, entry: false},
	{plugin: false, entry: true},
	{plugin: true, entry: false},
	{plugin: true, entry: true},
}

// TestPluginPolicy 测试各种插件策略的行为，以及
// 以及各种策略的有效性。
func TestPluginPolicy(t *testing.T) {
	// 这里内联展开，使判断哪些配置应通过的逻辑清晰可见。
	// 哪些测试配置应通过、哪些应失败。
	shouldErrFunc := func(test *pluginPolicyTest) (bool, string) {
		switch test.policyType {
		case "", api.PluginPolicyAllowAll:
			if test.allowlist != nil { // invalid
				return true, "allowlist is non-nil"
			}

			if test.pluginExists {
				return false, ""
			}

			return true, "not found"
		case api.PluginPolicyDenyAll:
			if test.allowlist != nil { // invalid
				return true, "allowlist is non-nil"
			}

			return true, `policy set to "DenyAll"`
		case api.PluginPolicyAllowlist:
			if test.allowlist == nil { // invalid
				return true, "allowlist is unspecified"
			}

			if len(test.allowlist) == 0 { // invalid
				return true, "allowlist is empty; use \"DenyAll\" policy instead"
			}

			switch {
			case test.pluginExists && test.entryExists:
				return false, ""
			case test.pluginExists && !test.entryExists:
				return true, "is not permitted by the credential plugin allowlist"
			case !test.pluginExists && test.entryExists:
				// 错误信息会随路径是相对还是绝对而变化。
				// 关键是这里会产生错误。
				return true, ""
			case !test.pluginExists && !test.entryExists:
				// 错误信息会随路径是相对还是绝对而变化。
				// 关键是这里会产生错误。
				return true, ""
			}

			panic("unreachable")
		}

		return true, "unknown plugin policy"
	}

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("could not get working directory: %s", err)
	}

	testdataDir := filepath.Join(wd, "testdata")

	path := os.Getenv("PATH")
	defer func() {
		if err = os.Setenv("PATH", path); err != nil {
			t.Fatal(err)
		}
	}()

	matrix := pluginPolicyTestMatrix{
		shouldErrFunc:    shouldErrFunc,
		exists:           allPermutations,
		absolute:         allPermutations,
		allowlistLengths: []int{-1 /*nil*/, 0, 1, 3},
		policies: []api.PolicyType{
			"",
			api.PluginPolicyAllowAll,
			api.PluginPolicyDenyAll,
			api.PluginPolicyAllowlist,
			api.PolicyType("HIGHLYILLEGAL"),
		},
	}

	tests := matrix.makeTests(t, testdataDir, path)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := test.config
			a, err := newAuthenticator(newCache(), func(_ int) bool { return false }, c, &clientauthentication.Cluster{})
			if err != nil {
				if !test.wantErr {
					t.Fatalf("unexpected validation error: %v", err)
				} else if !strings.Contains(err.Error(), test.wantErrSubstr) {
					t.Fatalf("expected error with substring '%v' got '%v'", test.wantErrSubstr, err.Error())
				}

				return
			}

			stderr := &bytes.Buffer{}
			a.stderr = stderr
			a.environ = func() []string { return nil }

			if err := a.refreshCredsLocked(); err != nil {
				if !test.wantErr {
					t.Fatalf("unexpected allows plugin error: %v", err)
				} else if !strings.Contains(err.Error(), test.wantErrSubstr) {
					t.Fatalf("expected error with substring '%v' got '%v'", test.wantErrSubstr, err.Error())
				}
				return
			}

			if test.wantErr {
				t.Fatal("expected allowlist error, but error was nil")
			}
		})
	}
}

func (m *pluginPolicyTestMatrix) makeTests(t *testing.T, testdataDir, path string) []pluginPolicyTest {
	// 把 testdata 目录置于 PATH 前端，使 test-plugin.sh 同时可按 basename 和绝对路径解析。
	const existingPluginInPATHBasename = "test-plugin.sh"
	existingPluginInPATHAbsolutePath := filepath.Join(testdataDir, existingPluginInPATHBasename)

	err := os.Setenv("PATH", fmt.Sprintf("%s:%s", testdataDir, path))
	if err != nil {
		t.Fatalf("error setting PATH: %s", err)
	}

	resolved, err := exec.LookPath(existingPluginInPATHBasename)
	if err != nil {
		t.Fatal(err)
	}
	if existingPluginInPATHAbsolutePath != resolved {
		t.Fatalf("test plugin basename resolved incorrectly: did not resolve to %s", existingPluginInPATHAbsolutePath)
	}

	resolvedAbs, err := exec.LookPath(existingPluginInPATHAbsolutePath)
	if err != nil {
		t.Fatal(err)
	}
	if existingPluginInPATHAbsolutePath != resolvedAbs {
		t.Fatalf("test plugin absolute path resolved incorrectly: did not resolve to %s", existingPluginInPATHAbsolutePath)
	}

	tests := make([]pluginPolicyTest, 0, len(m.exists)*2+len(m.absolute)*2+len(m.allowlistLengths)+len(m.policies))

	var tt *pluginPolicyTest
	for _, exists := range m.exists {
		tt = new(pluginPolicyTest)

		tt.setExists(exists)
		for _, absolute := range m.absolute {
			tt.setAbsolute(absolute)

			for _, length := range m.allowlistLengths {
				tt.setAllowlist(length, existingPluginInPATHAbsolutePath, existingPluginInPATHBasename)

				for _, p := range m.policies {
					tt.policyType = p
					tt.setName()
					tt.setExecConfig(existingPluginInPATHAbsolutePath, existingPluginInPATHBasename)
					tt.setErrDetails(m)

					tests = append(tests, *tt)
				}
			}
		}
	}

	return tests
}

// setErrDetails 根据矩阵的判定函数填充期望结果。
func (tt *pluginPolicyTest) setErrDetails(m *pluginPolicyTestMatrix) {
	tt.wantErr, tt.wantErrSubstr = m.shouldErrFunc(tt)
}

// setAbsolute 设置插件命令与 allowlist 条目是否使用绝对路径。
func (tt *pluginPolicyTest) setAbsolute(absolute dualBool) {
	tt.usePluginAbsPath = absolute.plugin
	tt.useEntryAbsPath = absolute.entry
}

// setExists 设置插件与 allowlist 条目在该路径上是否真实存在。
func (tt *pluginPolicyTest) setExists(exists dualBool) {
	tt.pluginExists = exists.plugin
	tt.entryExists = exists.entry
}

// setAllowlist 按长度构造 allowlist：-1 表示 nil，0 表示空，>=1 时首项指向测试插件，
// 其余填入无关条目，并打乱顺序以验证匹配与顺序无关。
func (tt *pluginPolicyTest) setAllowlist(l int, existingPluginInPATHAbsolutePath string, existingPluginInPATHBasename string) {
	tt.allowlistLength = l
	if tt.allowlistLength >= 0 {
		tt.allowlist = make([]api.AllowlistEntry, 0, tt.allowlistLength)
	}

	if tt.allowlistLength >= 1 {
		entry := tt.makeAllowlistEntry(existingPluginInPATHAbsolutePath, existingPluginInPATHBasename)
		tt.allowlist = append(tt.allowlist, entry)
	}

	for i := 1; i < tt.allowlistLength; i++ {
		tt.allowlist = append(tt.allowlist, api.AllowlistEntry{Command: fmt.Sprintf("foo-%d", i)})
	}

	// 打乱允许列表顺序，确保顺序不影响结果。
	for i := range tt.allowlist {
		j := mathrand.IntN(i + 1)
		tt.allowlist[i], tt.allowlist[j] = tt.allowlist[j], tt.allowlist[i]
	}
}

// makeAllowlistEntry 根据“是否存在/是否绝对路径”构造一条 allowlist 条目，
// 覆盖命中与不命中的组合。
func (tt *pluginPolicyTest) makeAllowlistEntry(existingPluginInPATHAbsolutePath string, existingPluginInPATHBasename string) api.AllowlistEntry {
	var entry api.AllowlistEntry

	switch {
	case tt.entryExists && tt.useEntryAbsPath:
		entry.Command = existingPluginInPATHAbsolutePath
	case tt.entryExists && !tt.useEntryAbsPath:
		entry.Command = existingPluginInPATHBasename
	case !tt.entryExists && tt.useEntryAbsPath:
		entry.Command = "/this/path/does/not/exist"
	case !tt.entryExists && !tt.useEntryAbsPath:
		entry.Command = "does not exist"
	}

	return entry
}

// setExecConfig 依据当前维度生成 ExecConfig；只有真正指向 test-plugin.sh 时才
// 注入测试输出环境变量，避免对不存在的命令产生误导性输出。
func (tt *pluginPolicyTest) setExecConfig(existingPluginInPATHAbsolutePath string, existingPluginInPATHBasename string) {
	var cmd string

	switch {
	case tt.pluginExists && tt.usePluginAbsPath:
		cmd = existingPluginInPATHAbsolutePath
	case tt.pluginExists && !tt.usePluginAbsPath:
		cmd = existingPluginInPATHBasename
	case !tt.pluginExists && tt.usePluginAbsPath:
		cmd = "/this/path/does/not/exist"
	case !tt.pluginExists && !tt.usePluginAbsPath:
		cmd = "does not exist"
	default: // verifiably unreachable
		cmd = "does not exist"
	}

	config := api.ExecConfig{}
	if strings.HasSuffix(cmd, "test-plugin.sh") {
		config.Env = append(config.Env, api.ExecEnvVar{
			Name: "TEST_OUTPUT",
			Value: `{
				"kind": "ExecCredential",
				"apiVersion": "client.authentication.k8s.io/v1",
				"status": {
					"token": "foo-bar"
				}
			}`,
		})
		config.Env = append(config.Env, api.ExecEnvVar{
			Name:  "TEST_EXIT_CODE",
			Value: strconv.Itoa(0),
		})
	}

	config.APIVersion = "client.authentication.k8s.io/v1"
	config.InteractiveMode = api.IfAvailableExecInteractiveMode
	config.Command = cmd
	config.PluginPolicy.PolicyType = tt.policyType
	config.PluginPolicy.Allowlist = tt.allowlist

	tt.config = &config
}

// makeExistsString 生成 test 名称中的存在性描述片段。
func makeExistsString(s string, t bool) string {
	ne := "nonexistent"
	if t {
		ne = "existing"
	}

	return fmt.Sprintf("%s-%s-path", ne, s)
}

// makePathString 生成 test 名称中的路径形式描述片段。
func makePathString(s string, t bool) string {
	ba := "basename"
	if t {
		ba = "absolute-path"
	}

	return fmt.Sprintf("%s-%s", ba, s)
}

// setName 把各维度拼成可读且唯一的子测试名（用路径分隔符形成层级）。
func (tt *pluginPolicyTest) setName() {
	p := string(tt.policyType)
	if string(tt.policyType) == "" {
		p = "unspecified"
	}
	var lengthStr string
	switch tt.allowlistLength {
	case -1:
		lengthStr = "nil-allowlist"
	case 0:
		lengthStr = "empty-allowlist"
	case 1:
		lengthStr = "single-entry-allowlist"
	default:
		lengthStr = "multiple-entry-allowlist"
	}
	pluginExistsString := makeExistsString("plugin", tt.pluginExists)
	entryExistsString := makeExistsString("entry", tt.entryExists)
	pluginPathString := makePathString("plugin", tt.usePluginAbsPath)
	entryPathString := makePathString("entry", tt.useEntryAbsPath)
	policyString := fmt.Sprintf("with-%s-policy", strings.ToLower(p))

	tt.name = filepath.Join(
		policyString,
		lengthStr,
		pluginExistsString,
		entryExistsString,
		pluginPathString,
		entryPathString,
	)
}

// TestRoundTripper 端到端验证 exec 凭据在 HTTP transport 中的行为：
// 首次获取 token 后缓存复用；服务端 401 触发重新执行；token 过期后自动刷新，
// 不依赖 401。
func TestRoundTripper(t *testing.T) {
	wantToken := ""

	n := time.Now()
	now := func() time.Time { return n }

	env := []string{""}
	environ := func() []string {
		s := make([]string, len(env))
		copy(s, env)
		return s
	}

	setOutput := func(s string) {
		env[0] = "TEST_OUTPUT=" + s
	}

	handler := func(w http.ResponseWriter, r *http.Request) {
		gotToken := ""
		parts := strings.Split(r.Header.Get("Authorization"), " ")
		if len(parts) > 1 && strings.EqualFold(parts[0], "bearer") {
			gotToken = parts[1]
		}

		if wantToken != gotToken {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		fmt.Fprintln(w, "ok")
	}
	server := httptest.NewServer(http.HandlerFunc(handler))

	c := api.ExecConfig{
		Command:         "./testdata/test-plugin.sh",
		APIVersion:      "client.authentication.k8s.io/v1beta1",
		InteractiveMode: api.IfAvailableExecInteractiveMode,
	}
	a, err := newAuthenticator(newCache(), func(_ int) bool { return false }, &c, nil)
	if err != nil {
		t.Fatal(err)
	}
	a.environ = environ
	a.now = now
	a.stderr = io.Discard

	tc := &transport.Config{}
	if err := a.UpdateTransportConfig(tc); err != nil {
		t.Fatal(err)
	}
	client := http.Client{
		Transport: tc.WrapTransport(http.DefaultTransport),
	}

	get := func(t *testing.T, statusCode int) {
		t.Helper()
		resp, err := client.Get(server.URL)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != statusCode {
			t.Errorf("wanted status %d got %d", statusCode, resp.StatusCode)
		}
	}

	setOutput(`{
		"kind": "ExecCredential",
		"apiVersion": "client.authentication.k8s.io/v1beta1",
		"status": {
			"token": "token1"
		}
	}`)
	wantToken = "token1"
	get(t, http.StatusOK)

	setOutput(`{
		"kind": "ExecCredential",
		"apiVersion": "client.authentication.k8s.io/v1beta1",
		"status": {
			"token": "token2"
		}
	}`)
	// 前一个 token 应被缓存。
	get(t, http.StatusOK)

	wantToken = "token2"
	// token 仍被缓存，遇到 unauthorized 后触发轮换。
	get(t, http.StatusUnauthorized)
	// 后续请求使用轮换后的 token。
	get(t, http.StatusOK)

	setOutput(`{
		"kind": "ExecCredential",
		"apiVersion": "client.authentication.k8s.io/v1beta1",
		"status": {
			"token": "token3",
			"expirationTimestamp": "` + now().Add(time.Hour).Format(time.RFC3339Nano) + `"
		}
	}`)
	wantToken = "token3"
	// token 仍被缓存，遇到 unauthorized 后轮换为带过期时间的 token。
	get(t, http.StatusUnauthorized)
	get(t, http.StatusOK)

	// 时间前进 2 小时，"token3" 现已过期。
	n = n.Add(time.Hour * 2)
	setOutput(`{
		"kind": "ExecCredential",
		"apiVersion": "client.authentication.k8s.io/v1beta1",
		"status": {
			"token": "token4",
			"expirationTimestamp": "` + now().Add(time.Hour).Format(time.RFC3339Nano) + `"
		}
	}`)
	wantToken = "token4"
	// 旧 token 已过期，应自动刷新而不触发 401。
	get(t, http.StatusOK)
}

// TestAuthorizationHeaderPresentCancelsExecAction 验证 transport 配置中已存在
// 其他认证方式（bearer、basic、客户端证书）时，exec 插件不应被激活，
// 避免覆盖用户显式配置的凭据。
func TestAuthorizationHeaderPresentCancelsExecAction(t *testing.T) {
	tests := []struct {
		name               string
		setTransportConfig func(*transport.Config)
	}{
		{
			name: "bearer token",
			setTransportConfig: func(config *transport.Config) {
				config.BearerToken = "token1f"
			},
		},
		{
			name: "basic auth",
			setTransportConfig: func(config *transport.Config) {
				config.Username = "marshmallow"
				config.Password = "zelda"
			},
		},
		{
			name: "cert auth",
			setTransportConfig: func(config *transport.Config) {
				config.TLS.CertData = []byte("some-cert-data")
				config.TLS.KeyData = []byte("some-key-data")
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			a, err := newAuthenticator(newCache(), func(_ int) bool { return false }, &api.ExecConfig{
				Command:    "./testdata/test-plugin.sh",
				APIVersion: "client.authentication.k8s.io/v1beta1",
			}, nil)
			if err != nil {
				t.Fatal(err)
			}

			// UpdateTransportConfig 在已有 TLS 证书回调且没有 bearer token 时返回错误；
			// 有 bearer token 时则以其为准。
			cert := func() (*tls.Certificate, error) {
				return nil, nil
			}
			tc := &transport.Config{TLS: transport.TLSConfig{Insecure: true, GetCertHolder: &transport.GetCertHolder{GetCert: cert}}}
			test.setTransportConfig(tc)

			if err := a.UpdateTransportConfig(tc); err != nil {
				t.Error("Expected presence of bearer token in config to cancel exec action")
			}
		})
	}
}

// TestTLSCredentials 验证 exec 插件返回的客户端证书会被用于 mTLS；证书过期
// 后重新执行插件，新证书若不被服务端信任则握手失败，换回受信任证书后恢复。
func TestTLSCredentials(t *testing.T) {
	now := time.Now()

	certPool := x509.NewCertPool()
	cert, key := genClientCert(t)
	if !certPool.AppendCertsFromPEM(cert) {
		t.Fatal("failed to add client cert to CertPool")
	}

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	}))
	server.TLS = &tls.Config{
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  certPool,
	}
	server.StartTLS()
	defer server.Close()

	a, err := newAuthenticator(newCache(), func(_ int) bool { return false }, &api.ExecConfig{
		Command:         "./testdata/test-plugin.sh",
		APIVersion:      "client.authentication.k8s.io/v1beta1",
		InteractiveMode: api.IfAvailableExecInteractiveMode,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var output *clientauthentication.ExecCredential
	a.environ = func() []string {
		data, err := runtime.Encode(codecs.LegacyCodec(a.group), output)
		if err != nil {
			t.Fatal(err)
		}
		return []string{"TEST_OUTPUT=" + string(data)}
	}
	a.now = func() time.Time { return now }
	a.stderr = io.Discard

	// 不关心服务端证书，本测试只针对客户端证书。
	tc := &transport.Config{TLS: transport.TLSConfig{Insecure: true}}
	if err := a.UpdateTransportConfig(tc); err != nil {
		t.Fatal(err)
	}

	get := func(t *testing.T, desc string, wantErr bool) {
		t.Run(desc, func(t *testing.T) {
			tlsCfg, err := transport.TLSConfigFor(tc)
			if err != nil {
				t.Fatal("TLSConfigFor:", err)
			}
			client := http.Client{
				Transport: &http.Transport{TLSClientConfig: tlsCfg},
			}
			resp, err := client.Get(server.URL)
			switch {
			case err != nil && !wantErr:
				t.Errorf("got client.Get error: %q, want nil", err)
			case err == nil && wantErr:
				t.Error("got nil client.Get error, want non-nil")
			}
			if err == nil {
				resp.Body.Close()
			}
		})
	}

	output = &clientauthentication.ExecCredential{
		Status: &clientauthentication.ExecCredentialStatus{
			ClientCertificateData: string(cert),
			ClientKeyData:         string(key),
			ExpirationTimestamp:   &v1.Time{Time: now.Add(time.Hour)},
		},
	}
	get(t, "valid TLS cert", false)

	// 推进时间以强制重新执行插件。
	nCert, nKey := genClientCert(t)
	now = now.Add(time.Hour * 2)
	output = &clientauthentication.ExecCredential{
		Status: &clientauthentication.ExecCredentialStatus{
			ClientCertificateData: string(nCert),
			ClientKeyData:         string(nKey),
			ExpirationTimestamp:   &v1.Time{Time: now.Add(time.Hour)},
		},
	}
	get(t, "untrusted TLS cert", true)

	now = now.Add(time.Hour * 2)
	output = &clientauthentication.ExecCredential{
		Status: &clientauthentication.ExecCredentialStatus{
			ClientCertificateData: string(cert),
			ClientKeyData:         string(key),
			ExpirationTimestamp:   &v1.Time{Time: now.Add(time.Hour)},
		},
	}
	get(t, "valid TLS cert again", false)
}

// TestConcurrentUpdateTransportConfig 并发反复调用 UpdateTransportConfig，
// 配合 -race 验证内部状态（缓存与配置更新）无数据竞争。
func TestConcurrentUpdateTransportConfig(t *testing.T) {
	n := time.Now()
	now := func() time.Time { return n }

	env := []string{""}
	environ := func() []string {
		s := make([]string, len(env))
		copy(s, env)
		return s
	}

	c := api.ExecConfig{
		Command:    "./testdata/test-plugin.sh",
		APIVersion: "client.authentication.k8s.io/v1beta1",
	}
	a, err := newAuthenticator(newCache(), func(_ int) bool { return false }, &c, nil)
	if err != nil {
		t.Fatal(err)
	}
	a.environ = environ
	a.now = now
	a.stderr = io.Discard

	stopCh := make(chan struct{})
	defer close(stopCh)

	numConcurrent := 2

	for i := 0; i < numConcurrent; i++ {
		go func() {
			for {
				tc := &transport.Config{}
				a.UpdateTransportConfig(tc)

				select {
				case <-stopCh:
					return
				default:
					continue
				}
			}
		}()
	}
	time.Sleep(2 * time.Second)
}

// TestInstallHintRateLimit 验证 installHint 仅在阈值内展示，并在时间窗口过后重置，
// 防止插件持续缺失时反复打印安装提示。
func TestInstallHintRateLimit(t *testing.T) {
	tests := []struct {
		name string

		threshold int
		interval  time.Duration

		calls          int
		perCallAdvance time.Duration

		wantInstallHint int
	}{
		{
			name:            "print-up-to-threshold",
			threshold:       2,
			interval:        time.Second,
			calls:           10,
			wantInstallHint: 2,
		},
		{
			name:            "after-interval-threshold-resets",
			threshold:       2,
			interval:        time.Second * 5,
			calls:           10,
			perCallAdvance:  time.Second,
			wantInstallHint: 4,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := api.ExecConfig{
				Command:         "does not exist",
				APIVersion:      "client.authentication.k8s.io/v1beta1",
				InstallHint:     "some install hint",
				InteractiveMode: api.IfAvailableExecInteractiveMode,
			}
			a, err := newAuthenticator(newCache(), func(_ int) bool { return false }, &c, nil)
			if err != nil {
				t.Fatal(err)
			}

			a.sometimes.threshold = test.threshold
			a.sometimes.interval = test.interval

			clock := testingclock.NewFakeClock(time.Now())
			a.sometimes.clock = clock

			count := 0
			for i := 0; i < test.calls; i++ {
				err := a.refreshCredsLocked()
				if strings.Contains(err.Error(), c.InstallHint) {
					count++
				}

				clock.SetTime(clock.Now().Add(test.perCallAdvance))
			}

			if test.wantInstallHint != count {
				t.Errorf(
					"%s: expected install hint %d times got %d",
					test.name,
					test.wantInstallHint,
					count,
				)
			}
		})
	}
}

// genClientCert 生成测试用 x509 证书，以 PEM 编码返回证书与私钥，有效期 24 小时。
func genClientCert(t *testing.T) ([]byte, []byte) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	keyRaw, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject:      pkix.Name{Organization: []string{"Acme Co"}},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(24 * time.Hour),

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	certRaw, err := x509.CreateCertificate(rand.Reader, cert, cert, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certRaw}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyRaw})
}
