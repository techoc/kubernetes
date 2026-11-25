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

package auth

// 本文件测试发往 apiserver 的 HTTP 请求的认证（以及后续的鉴权）。
// 不使用 pkg/client/... 中的客户端，因为认证和鉴权必须对任意 HTTP 客户端都生效。

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	authenticationv1 "k8s.io/api/authentication/v1"
	authenticationv1beta1 "k8s.io/api/authentication/v1beta1"
	certificatesv1 "k8s.io/api/certificates/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/wait"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	auditv1 "k8s.io/apiserver/pkg/apis/audit/v1"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/group"
	"k8s.io/apiserver/pkg/authentication/request/bearertoken"
	"k8s.io/apiserver/pkg/authentication/serviceaccount"
	"k8s.io/apiserver/pkg/authentication/token/cache"
	"k8s.io/apiserver/pkg/authentication/token/tokenfile"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	unionauthz "k8s.io/apiserver/pkg/authorization/union"
	genericrequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/server"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	webhookutil "k8s.io/apiserver/pkg/util/webhook"
	"k8s.io/apiserver/plugin/pkg/authenticator/token/webhook"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	v1 "k8s.io/client-go/tools/clientcmd/api/v1"
	resttransport "k8s.io/client-go/transport"
	utiltesting "k8s.io/client-go/util/testing"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/cmd/kube-apiserver/app/options"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/pkg/apis/autoscaling"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/extensions"
	"k8s.io/kubernetes/pkg/controlplane"
	"k8s.io/kubernetes/test/integration"
	"k8s.io/kubernetes/test/integration/authutil"
	"k8s.io/kubernetes/test/integration/framework"
	testutils "k8s.io/kubernetes/test/utils"
	"k8s.io/kubernetes/test/utils/ktesting"
)

const (
	AliceToken   string = "abc123" // 用户名: alice，存在于 token 文件中。
	BobToken     string = "xyz987" // 用户名: bob，存在于 token 文件中。
	UnknownToken string = "qwerty" // 不在 token 文件中。
)

func getTestWebhookTokenAuth(serverURL string, customDial utilnet.DialFunc) (authenticator.Request, error) {
	kubecfgFile, err := os.CreateTemp("", "webhook-kubecfg")
	if err != nil {
		return nil, err
	}
	defer utiltesting.CloseAndRemove(&testing.T{}, kubecfgFile)
	config := v1.Config{
		Clusters: []v1.NamedCluster{
			{
				Cluster: v1.Cluster{Server: serverURL},
			},
		},
	}
	if err := json.NewEncoder(kubecfgFile).Encode(config); err != nil {
		return nil, err
	}

	retryBackoff := wait.Backoff{
		Duration: 500 * time.Millisecond,
		Factor:   1.5,
		Jitter:   0.2,
		Steps:    5,
	}

	clientConfig, err := webhookutil.LoadKubeconfig(kubecfgFile.Name(), customDial)
	if err != nil {
		return nil, err
	}

	webhookTokenAuth, err := webhook.New(clientConfig, "v1beta1", nil, retryBackoff)
	if err != nil {
		return nil, err
	}
	return bearertoken.New(cache.New(webhookTokenAuth, false, 2*time.Minute, 2*time.Minute)), nil
}

func getTestWebhookTokenAuthCustomDialer(serverURL string) (authenticator.Request, error) {
	customDial := http.DefaultTransport.(*http.Transport).DialContext

	return getTestWebhookTokenAuth(serverURL, customDial)
}

func path(resource, namespace, name string) string {
	return pathWithPrefix("", resource, namespace, name)
}

func pathWithPrefix(prefix, resource, namespace, name string) string {
	path := "/api/v1"
	if prefix != "" {
		path = path + "/" + prefix
	}
	if namespace != "" {
		path = path + "/namespaces/" + namespace
	}
	// 资源名统一为小写。
	resource = strings.ToLower(resource)
	if resource != "" {
		path = path + "/" + resource
	}
	if name != "" {
		path = path + "/" + name
	}
	return path
}

func pathWithSubResource(resource, namespace, name, subresource string) string {
	path := pathWithPrefix("", resource, namespace, name)
	if subresource != "" {
		path = path + "/" + subresource
	}
	return path
}

func timeoutPath(resource, namespace, name string) string {
	return addTimeoutFlag(path(resource, namespace, name))
}

// 后续测试请求使用的请求体。
var aPod = `
{
  "kind": "Pod",
  "apiVersion": "v1",
  "metadata": {
    "name": "a"%s
  },
  "spec": {
    "containers": [
      {
        "name": "foo",
        "image": "bar/foo"
      }
    ]
  }
}
`
var aRC = `
{
  "kind": "ReplicationController",
  "apiVersion": "v1",
  "metadata": {
    "name": "a",
    "labels": {
      "name": "a"
    }%s
  },
  "spec": {
    "replicas": 2,
    "selector": {
      "name": "a"
    },
    "template": {
      "metadata": {
        "labels": {
          "name": "a"
        }
      },
      "spec": {
        "containers": [
          {
            "name": "foo",
            "image": "bar/foo"
          }
        ]
      }
    }
  }
}
`
var aService = `
{
  "kind": "Service",
  "apiVersion": "v1",
  "metadata": {
    "name": "a",
    "labels": {
      "name": "a"
    }%s
  },
  "spec": {
    "ports": [
      {
        "protocol": "TCP",
        "port": 8000
      }
    ],
    "selector": {
      "name": "a"
    },
    "clusterIP": "10.0.0.100"
  }
}
`
var aNode = `
{
  "kind": "Node",
  "apiVersion": "v1",
  "metadata": {
    "name": "a"%s
  },
  "spec": {
    "externalID": "external"
  }
}
`

func aEvent(namespace string) string {
	return `
{
  "kind": "Event",
  "apiVersion": "v1",
  "metadata": {
    "name": "a"%s
  },
  "involvedObject": {
    "kind": "Pod",
    "namespace": "` + namespace + `",
    "name": "a",
    "apiVersion": "v1"
  }
}
`
}

var aBinding = `
{
  "kind": "Binding",
  "apiVersion": "v1",
  "metadata": {
    "name": "a"%s
  },
  "target": {
    "name": "10.10.10.10"
  }
}
`

var emptyEndpoints = `
{
  "kind": "Endpoints",
  "apiVersion": "v1",
  "metadata": {
    "name": "a"%s
  }
}
`

var aEndpoints = `
{
  "kind": "Endpoints",
  "apiVersion": "v1",
  "metadata": {
    "name": "a"%s
  },
  "subsets": [
    {
      "addresses": [
        {
          "ip": "10.10.1.1"
        }
      ],
      "ports": [
        {
          "port": 1909,
          "protocol": "TCP"
        }
      ]
    }
  ]
}
`

var deleteNow = `
{
  "kind": "DeleteOptions",
  "apiVersion": "v1",
  "gracePeriodSeconds": 0%s
}
`

// addTimeoutFlag 设置 timeout 参数，确保依赖它的 GET 请求在 POST 完成之后才执行。
func addTimeoutFlag(URLString string) string {
	u, _ := url.Parse(URLString)
	values := u.Query()
	values.Set("timeout", "60s")
	u.RawQuery = values.Encode()
	return u.String()
}

type testRequest struct {
	verb        string
	URL         string
	body        string
	statusCodes map[int]bool // 允许的状态码。
}

func getTestRequests(namespace string) []testRequest {
	requests := []testRequest{
		// Pod 常规操作。
		{"GET", path("pods", "", ""), "", integration.Code200},
		{"GET", path("pods", namespace, ""), "", integration.Code200},
		{"POST", timeoutPath("pods", namespace, ""), aPod, integration.Code201},
		{"PUT", timeoutPath("pods", namespace, "a"), aPod, integration.Code200},
		{"GET", path("pods", namespace, "a"), "", integration.Code200},
		// Pod 尚未调度到 Node，/exec 的 GET 和 POST 应返回 400。
		{"GET", path("pods", namespace, "a") + "/exec", "", integration.Code400},
		{"POST", path("pods", namespace, "a") + "/exec", "", integration.Code400},
		// /exec 不支持 PUT，应返回 405。
		{"PUT", path("pods", namespace, "a") + "/exec", "", integration.Code405},
		// Pod 尚未调度到 Node，/portforward 的 GET 和 POST 应返回 400。
		{"GET", path("pods", namespace, "a") + "/portforward", "", integration.Code400},
		{"POST", path("pods", namespace, "a") + "/portforward", "", integration.Code400},
		// /portforward 不支持 PUT，应返回 405。
		{"PUT", path("pods", namespace, "a") + "/portforward", "", integration.Code405},
		{"PATCH", path("pods", namespace, "a"), "{%v}", integration.Code200},
		{"DELETE", timeoutPath("pods", namespace, "a"), deleteNow, integration.Code200},

		// 非标准方法（预期不可用，但应先完成鉴权，再在校验阶段失败）。
		{"OPTIONS", path("pods", namespace, ""), "", integration.Code405},
		{"OPTIONS", path("pods", namespace, "a"), "", integration.Code405},
		{"HEAD", path("pods", namespace, ""), "", integration.Code405},
		{"HEAD", path("pods", namespace, "a"), "", integration.Code405},
		{"TRACE", path("pods", namespace, ""), "", integration.Code405},
		{"TRACE", path("pods", namespace, "a"), "", integration.Code405},
		{"NOSUCHVERB", path("pods", namespace, ""), "", integration.Code405},

		// Service 常规操作。
		{"GET", path("services", "", ""), "", integration.Code200},
		{"GET", path("services", namespace, ""), "", integration.Code200},
		{"POST", timeoutPath("services", namespace, ""), aService, integration.Code201},
		// 手动为 Service 创建 Endpoints（正常由 endpoint controller 完成，但本测试未运行它）。
		{"POST", timeoutPath("endpoints", namespace, ""), emptyEndpoints, integration.Code201},
		// Endpoints 的 subset 为空时，访问 proxy 应返回 503。
		{"GET", pathWithSubResource("services", namespace, "a", "proxy") + "/", "", integration.Code503},
		{"PUT", timeoutPath("services", namespace, "a"), aService, integration.Code200},
		{"GET", path("services", namespace, "a"), "", integration.Code200},
		{"DELETE", timeoutPath("endpoints", namespace, "a"), "", integration.Code200},
		{"DELETE", timeoutPath("services", namespace, "a"), "", integration.Code200},

		// ReplicationController 常规操作。
		{"GET", path("replicationControllers", "", ""), "", integration.Code200},
		{"GET", path("replicationControllers", namespace, ""), "", integration.Code200},
		{"POST", timeoutPath("replicationControllers", namespace, ""), aRC, integration.Code201},
		{"PUT", timeoutPath("replicationControllers", namespace, "a"), aRC, integration.Code200},
		{"GET", path("replicationControllers", namespace, "a"), "", integration.Code200},
		{"DELETE", timeoutPath("replicationControllers", namespace, "a"), "", integration.Code200},

		// Endpoints 常规操作。
		{"GET", path("endpoints", "", ""), "", integration.Code200},
		{"GET", path("endpoints", namespace, ""), "", integration.Code200},
		{"POST", timeoutPath("endpoints", namespace, ""), aEndpoints, integration.Code201},
		{"PUT", timeoutPath("endpoints", namespace, "a"), aEndpoints, integration.Code200},
		{"GET", path("endpoints", namespace, "a"), "", integration.Code200},
		{"DELETE", timeoutPath("endpoints", namespace, "a"), "", integration.Code200},

		// Node 常规操作。
		{"GET", path("nodes", "", ""), "", integration.Code200},
		{"POST", timeoutPath("nodes", "", ""), aNode, integration.Code201},
		{"PUT", timeoutPath("nodes", "", "a"), aNode, integration.Code200},
		{"GET", path("nodes", "", "a"), "", integration.Code200},
		{"DELETE", timeoutPath("nodes", "", "a"), "", integration.Code200},

		// Event 常规操作。
		{"GET", path("events", "", ""), "", integration.Code200},
		{"GET", path("events", namespace, ""), "", integration.Code200},
		{"POST", timeoutPath("events", namespace, ""), aEvent(namespace), integration.Code201},
		{"PUT", timeoutPath("events", namespace, "a"), aEvent(namespace), integration.Code200},
		{"GET", path("events", namespace, "a"), "", integration.Code200},
		{"DELETE", timeoutPath("events", namespace, "a"), "", integration.Code200},

		// Binding 常规操作。
		{"GET", path("bindings", namespace, ""), "", integration.Code405},
		{"POST", timeoutPath("pods", namespace, ""), aPod, integration.Code201}, // 先创建 Pod，否则绑定会返回 404。
		{"POST", timeoutPath("bindings", namespace, ""), aBinding, integration.Code201},
		{"PUT", timeoutPath("bindings", namespace, "a"), aBinding, integration.Code404},
		{"GET", path("bindings", namespace, "a"), "", integration.Code404}, // 不存在 Binding 实例。
		{"DELETE", timeoutPath("bindings", namespace, "a"), "", integration.Code404},

		// 不存在的对象类型。
		{"GET", path("foo", "", ""), "", integration.Code404},
		{"POST", path("foo", namespace, ""), `{"foo": "foo"}`, integration.Code404},
		{"PUT", path("foo", namespace, "a"), `{"foo": "foo"}`, integration.Code404},
		{"GET", path("foo", namespace, "a"), "", integration.Code404},
		{"DELETE", timeoutPath("foo", namespace, ""), "", integration.Code404},

		// Node 上的特殊动词。
		{"GET", pathWithSubResource("nodes", namespace, "a", "proxy"), "", integration.Code404},
		{"GET", pathWithPrefix("redirect", "nodes", namespace, "a"), "", integration.Code404},
		// TODO: 测试 .../watch/...，它在测试超时前不会结束。
		// TODO: 找到创建 Node 的方法，使 proxy/redirect 能成功。

		// 非对象 endpoint。
		{"GET", "/", "", integration.Code200},
		{"GET", "/api", "", integration.Code200},
		{"GET", "/healthz", "", integration.Code200},
		{"GET", "/version", "", integration.Code200},
		{"GET", "/invalidURL", "", integration.Code404},
	}
	return requests
}

// TestAuthMode* 系列测试大量 URL，检查在不同模式下是否被 FORBIDDEN。
// 除鉴权外不做详细的行为校验，也不是 fuzz 测试。
//
// TODO(etune): 为 REST API 编写 fuzz 测试。
func TestAuthModeAlwaysAllow(t *testing.T) {
	tCtx := ktesting.Init(t)
	kubeClient, kubeConfig, tearDownFn := framework.StartTestServer(tCtx, t, framework.TestServerSetup{
		ModifyServerRunOptions: func(opts *options.ServerRunOptions) {
			// 当前未运行 serviceaccount controller，需禁用 ServiceAccount admission 插件。
			opts.Admission.GenericAdmission.DisablePlugins = []string{"ServiceAccount"}
			opts.Authorization.Modes = []string{"AlwaysAllow"}
		},
	})
	defer tearDownFn()

	ns := framework.CreateNamespaceOrDie(kubeClient, "auth-always-allow", t)
	defer framework.DeleteNamespaceOrDie(kubeClient, ns, t)

	transport, err := rest.TransportFor(kubeConfig)
	if err != nil {
		t.Fatal(err)
	}
	previousResourceVersion := make(map[string]float64)

	for _, r := range getTestRequests(ns.Name) {
		var bodyStr string
		if r.body != "" {
			sub := ""
			if r.verb == "PUT" {
				// 更新操作需带上之前的 resourceVersion。
				if resVersion := previousResourceVersion[getPreviousResourceVersionKey(r.URL, "")]; resVersion != 0 {
					sub += fmt.Sprintf(",\r\n\"resourceVersion\": \"%v\"", resVersion)
				}
				sub += fmt.Sprintf(",\r\n\"namespace\": %q", ns.Name)
			}
			bodyStr = fmt.Sprintf(r.body, sub)
		}
		r.body = bodyStr
		bodyBytes := bytes.NewReader([]byte(bodyStr))
		req, err := http.NewRequest(r.verb, kubeConfig.Host+r.URL, bodyBytes)
		if err != nil {
			t.Logf("case %v", r)
			t.Fatalf("unexpected error: %v", err)
		}
		if r.verb == "PATCH" {
			req.Header.Set("Content-Type", "application/merge-patch+json")
		}
		func() {
			resp, err := transport.RoundTrip(req)
			if err != nil {
				t.Logf("case %v", r)
				t.Fatalf("unexpected error: %v", err)
			}
			defer resp.Body.Close()
			b, _ := io.ReadAll(resp.Body)
			if _, ok := r.statusCodes[resp.StatusCode]; !ok {
				t.Logf("case %v", r)
				t.Errorf("Expected status one of %v, but got %v", r.statusCodes, resp.StatusCode)
				t.Errorf("Body: %v", string(b))
			} else {
				if r.verb == "POST" {
					// 创建成功后提取 resourceVersion，供后续更新使用。
					id, currentResourceVersion, err := parseResourceVersion(b)
					if err == nil {
						key := getPreviousResourceVersionKey(r.URL, id)
						previousResourceVersion[key] = currentResourceVersion
					} else {
						t.Logf("error in trying to extract resource version: %s", err)
					}
				}
			}
		}()
	}
}

func parseResourceVersion(response []byte) (string, float64, error) {
	var resultBodyMap map[string]interface{}
	err := json.Unmarshal(response, &resultBodyMap)
	if err != nil {
		return "", 0, fmt.Errorf("unexpected error unmarshaling resultBody: %v", err)
	}
	metadata, ok := resultBodyMap["metadata"].(map[string]interface{})
	if !ok {
		return "", 0, fmt.Errorf("unexpected error, metadata not found in JSON response: %v", string(response))
	}
	id, ok := metadata["name"].(string)
	if !ok {
		return "", 0, fmt.Errorf("unexpected error, id not found in JSON response: %v", string(response))
	}
	resourceVersionString, ok := metadata["resourceVersion"].(string)
	if !ok {
		return "", 0, fmt.Errorf("unexpected error, resourceVersion not found in JSON response: %v", string(response))
	}
	resourceVersion, err := strconv.ParseFloat(resourceVersionString, 64)
	if err != nil {
		return "", 0, fmt.Errorf("unexpected error, could not parse resourceVersion as float64, err: %s. JSON response: %v", err, string(response))
	}
	return id, resourceVersion, nil
}

func getPreviousResourceVersionKey(url, id string) string {
	baseURL := strings.Split(url, "?")[0]
	key := baseURL
	if id != "" {
		key = fmt.Sprintf("%s/%v", baseURL, id)
	}
	return key
}

func TestAuthModeAlwaysDeny(t *testing.T) {
	tCtx := ktesting.Init(t)
	kubeClient, kubeConfig, tearDownFn := framework.StartTestServer(tCtx, t, framework.TestServerSetup{
		ModifyServerRunOptions: func(opts *options.ServerRunOptions) {
			// 当前未运行 serviceaccount controller，需禁用 ServiceAccount admission 插件。
			opts.Admission.GenericAdmission.DisablePlugins = []string{"ServiceAccount"}
			opts.Authorization.Modes = []string{"AlwaysDeny"}
			opts.Authentication.TokenFile.TokenFile = "testdata/tokens.csv"
		},
	})
	defer tearDownFn()

	ns := framework.CreateNamespaceOrDie(kubeClient, "auth-always-deny", t)
	defer framework.DeleteNamespaceOrDie(kubeClient, ns, t)

	transport, err := rest.TransportFor(kubeConfig)
	if err != nil {
		t.Fatal(err)
	}
	transport = resttransport.NewBearerAuthRoundTripper(AliceToken, transport)

	for _, r := range getTestRequests(ns.Name) {
		bodyBytes := bytes.NewReader([]byte(r.body))
		req, err := http.NewRequest(r.verb, kubeConfig.Host+r.URL, bodyBytes)
		if err != nil {
			t.Logf("case %v", r)
			t.Fatalf("unexpected error: %v", err)
		}
		func() {
			resp, err := transport.RoundTrip(req)
			if err != nil {
				t.Logf("case %v", r)
				t.Fatalf("unexpected error: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusForbidden {
				t.Logf("case %v", r)
				t.Errorf("Expected status Forbidden but got status %v", resp.Status)
			}
		}()
	}
}

// TestAliceNotForbiddenOrUnauthorized 验证认证系统已知且被授权所有操作的用户不会被拒绝。
func TestAliceNotForbiddenOrUnauthorized(t *testing.T) {
	tCtx := ktesting.Init(t)
	kubeClient, kubeConfig, tearDownFn := framework.StartTestServer(tCtx, t, framework.TestServerSetup{
		ModifyServerRunOptions: func(opts *options.ServerRunOptions) {
			// 当前未运行 serviceaccount controller，需禁用 ServiceAccount admission 插件。
			opts.Admission.GenericAdmission.DisablePlugins = []string{"ServiceAccount"}
			opts.Authentication.TokenFile.TokenFile = "testdata/tokens.csv"
			opts.Authorization.Modes = []string{"ABAC"}
			opts.Authorization.PolicyFile = "testdata/allowalice.jsonl"
		},
	})
	defer tearDownFn()

	ns := framework.CreateNamespaceOrDie(kubeClient, "auth-alice-not-forbidden", t)
	defer framework.DeleteNamespaceOrDie(kubeClient, ns, t)

	previousResourceVersion := make(map[string]float64)
	transport, err := rest.TransportFor(kubeConfig)
	if err != nil {
		t.Fatal(err)
	}

	for _, r := range getTestRequests(ns.Name) {
		token := AliceToken
		var bodyStr string
		if r.body != "" {
			sub := ""
			if r.verb == "PUT" {
				// 更新操作需带上之前的 resourceVersion。
				if resVersion := previousResourceVersion[getPreviousResourceVersionKey(r.URL, "")]; resVersion != 0 {
					sub += fmt.Sprintf(",\r\n\"resourceVersion\": \"%v\"", resVersion)
				}
				sub += fmt.Sprintf(",\r\n\"namespace\": %q", ns.Name)
			}
			bodyStr = fmt.Sprintf(r.body, sub)
		}
		r.body = bodyStr
		bodyBytes := bytes.NewReader([]byte(bodyStr))
		req, err := http.NewRequest(r.verb, kubeConfig.Host+r.URL, bodyBytes)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
		if r.verb == "PATCH" {
			req.Header.Set("Content-Type", "application/merge-patch+json")
		}

		func() {
			resp, err := transport.RoundTrip(req)
			if err != nil {
				t.Logf("case %v", r)
				t.Fatalf("unexpected error: %v", err)
			}
			defer resp.Body.Close()
			b, _ := io.ReadAll(resp.Body)
			if _, ok := r.statusCodes[resp.StatusCode]; !ok {
				t.Logf("case %v", r)
				t.Errorf("Expected status one of %v, but got %v", r.statusCodes, resp.StatusCode)
				t.Errorf("Body: %v", string(b))
			} else {
				if r.verb == "POST" {
					// 创建成功后提取 resourceVersion，供后续更新使用。
					id, currentResourceVersion, err := parseResourceVersion(b)
					if err == nil {
						key := getPreviousResourceVersionKey(r.URL, id)
						previousResourceVersion[key] = currentResourceVersion
					}
				}
			}

		}()
	}
}

// TestBobIsForbidden 验证认证系统已知但未被授权任何操作的用户会收到 Forbidden。
func TestBobIsForbidden(t *testing.T) {
	tCtx := ktesting.Init(t)
	kubeClient, kubeConfig, tearDownFn := framework.StartTestServer(tCtx, t, framework.TestServerSetup{
		ModifyServerRunOptions: func(opts *options.ServerRunOptions) {
			// 当前未运行 serviceaccount controller，需禁用 ServiceAccount admission 插件。
			opts.Admission.GenericAdmission.DisablePlugins = []string{"ServiceAccount"}
			opts.Authentication.TokenFile.TokenFile = "testdata/tokens.csv"
			opts.Authorization.Modes = []string{"ABAC"}
			opts.Authorization.PolicyFile = "testdata/allowalice.jsonl"
		},
	})
	defer tearDownFn()

	ns := framework.CreateNamespaceOrDie(kubeClient, "auth-bob-forbidden", t)
	defer framework.DeleteNamespaceOrDie(kubeClient, ns, t)

	transport, err := rest.TransportFor(kubeConfig)
	if err != nil {
		t.Fatal(err)
	}

	for _, r := range getTestRequests(ns.Name) {
		token := BobToken
		bodyBytes := bytes.NewReader([]byte(r.body))
		req, err := http.NewRequest(r.verb, kubeConfig.Host+r.URL, bodyBytes)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

		func() {
			resp, err := transport.RoundTrip(req)
			if err != nil {
				t.Logf("case %v", r)
				t.Fatalf("unexpected error: %v", err)
			}
			defer resp.Body.Close()
			// 期望 bob 的所有操作都返回 Forbidden。
			if resp.StatusCode != http.StatusForbidden {
				t.Logf("case %v", r)
				t.Errorf("Expected not status Forbidden, but got %s", resp.Status)
			}
		}()
	}
}

// TestUnknownUserIsUnauthorized 验证认证系统未知的用户会收到 Unauthorized。
// 本场景也安装了鉴权模块，但请求预期到不了鉴权阶段。
func TestUnknownUserIsUnauthorized(t *testing.T) {
	tCtx := ktesting.Init(t)
	kubeClient, kubeConfig, tearDownFn := framework.StartTestServer(tCtx, t, framework.TestServerSetup{
		ModifyServerRunOptions: func(opts *options.ServerRunOptions) {
			// 当前未运行 serviceaccount controller，需禁用 ServiceAccount admission 插件。
			opts.Admission.GenericAdmission.DisablePlugins = []string{"ServiceAccount"}
			opts.Authentication.TokenFile.TokenFile = "testdata/tokens.csv"
			opts.Authorization.Modes = []string{"ABAC"}
			opts.Authorization.PolicyFile = "testdata/allowalice.jsonl"
		},
	})
	defer tearDownFn()

	ns := framework.CreateNamespaceOrDie(kubeClient, "auth-unknown-unauthorized", t)
	defer framework.DeleteNamespaceOrDie(kubeClient, ns, t)

	transport, err := rest.TransportFor(kubeConfig)
	if err != nil {
		t.Fatal(err)
	}

	for _, r := range getTestRequests(ns.Name) {
		token := UnknownToken
		bodyBytes := bytes.NewReader([]byte(r.body))
		req, err := http.NewRequest(r.verb, kubeConfig.Host+r.URL, bodyBytes)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
		func() {
			resp, err := transport.RoundTrip(req)
			if err != nil {
				t.Logf("case %v", r)
				t.Fatalf("unexpected error: %v", err)
			}
			defer resp.Body.Close()
			// 期望未认证用户的所有请求都返回 Unauthorized。
			if resp.StatusCode != http.StatusUnauthorized {
				t.Logf("case %v", r)
				t.Errorf("Expected status %v, but got %v", http.StatusUnauthorized, resp.StatusCode)
				b, _ := io.ReadAll(resp.Body)
				t.Errorf("Body: %v", string(b))
			}
		}()
	}
}

type impersonateAuthorizer struct{}

// alice 不能模拟任意用户；bob 除模拟他人外什么也做不了。
func (impersonateAuthorizer) Authorize(ctx context.Context, a authorizer.Attributes) (authorizer.Decision, string, error) {
	// alice 可以模拟 service account，并执行其他操作。
	if a.GetUser() != nil && a.GetUser().GetName() == "alice" && a.GetVerb() == "impersonate" && a.GetResource() == "serviceaccounts" {
		return authorizer.DecisionAllow, "", nil
	}
	if a.GetUser() != nil && a.GetUser().GetName() == "alice" && a.GetVerb() != "impersonate" {
		return authorizer.DecisionAllow, "", nil
	}
	// bob 可以模拟任意用户，但仅限于此。
	if a.GetUser() != nil && a.GetUser().GetName() == "bob" && a.GetVerb() == "impersonate" {
		return authorizer.DecisionAllow, "", nil
	}
	if a.GetUser() != nil && a.GetUser().GetName() == "bob" && a.GetVerb() != "impersonate" {
		return authorizer.DecisionDeny, "", nil
	}
	// service account 可以做任何操作。
	if a.GetUser() != nil && strings.HasPrefix(a.GetUser().GetName(), serviceaccount.ServiceAccountUsernamePrefix) {
		return authorizer.DecisionAllow, "", nil
	}

	return authorizer.DecisionNoOpinion, "I can't allow that.  Go ask alice.", nil
}

// ConditionsAwareAuthorize 不支持 conditions，仅转换 Authorize 的判定结果。
func (i impersonateAuthorizer) ConditionsAwareAuthorize(ctx context.Context, a authorizer.Attributes) authorizer.ConditionsAwareDecision {
	return authorizer.ConditionsAwareDecisionFromParts(i.Authorize(ctx, a))
}

// EvaluateConditions 当前 authorizer 不支持。
func (impersonateAuthorizer) EvaluateConditions(_ context.Context, _ authorizer.ConditionsAwareDecision, _ authorizer.ConditionsData) (authorizer.Decision, string, error) {
	return authorizer.DecisionDeny, "", authorizer.ErrorConditionEvaluationNotSupported
}

func TestImpersonateIsForbidden(t *testing.T) {
	tCtx := ktesting.Init(t)
	kubeClient, kubeConfig, tearDownFn := framework.StartTestServer(tCtx, t, framework.TestServerSetup{
		ModifyServerRunOptions: func(opts *options.ServerRunOptions) {
			// 当前未运行 serviceaccount controller，需禁用 ServiceAccount admission 插件。
			opts.Admission.GenericAdmission.DisablePlugins = []string{"ServiceAccount"}
			opts.Authentication.TokenFile.TokenFile = "testdata/tokens.csv"
		},
		ModifyServerConfig: func(config *controlplane.Config) {
			// 在鉴权链最前面加入针对 alice 和 bob 定制规则的模拟鉴权器。
			authz, err := unionauthz.New(
				unionauthz.NamedAuthorizer{AuthorizerName: "impersonate", Authorizer: impersonateAuthorizer{}},
				unionauthz.NamedAuthorizer{AuthorizerName: "default", Authorizer: config.ControlPlane.Generic.Authorization.Authorizer},
			)
			if err != nil {
				t.Fatalf("unionauthz.New: %v", err)
			}
			config.ControlPlane.Generic.Authorization.Authorizer = authz
		},
	})
	defer tearDownFn()

	ns := framework.CreateNamespaceOrDie(kubeClient, "auth-impersonate-forbidden", t)
	defer framework.DeleteNamespaceOrDie(kubeClient, ns, t)

	transport, err := rest.TransportFor(kubeConfig)
	if err != nil {
		t.Fatal(err)
	}

	// bob 不能以自身身份执行操作。
	for _, r := range getTestRequests(ns.Name) {
		token := BobToken
		bodyBytes := bytes.NewReader([]byte(r.body))
		req, err := http.NewRequest(r.verb, kubeConfig.Host+r.URL, bodyBytes)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

		func() {
			resp, err := transport.RoundTrip(req)
			if err != nil {
				t.Logf("case %v", r)
				t.Fatalf("unexpected error: %v", err)
			}
			defer resp.Body.Close()
			// 期望 bob 的所有操作都返回 Forbidden。
			if resp.StatusCode != http.StatusForbidden {
				t.Logf("case %v", r)
				t.Errorf("Expected status Forbidden, but got %s", resp.Status)
			}
		}()
	}

	// bob 可以模拟 alice 执行其他操作。
	for _, r := range getTestRequests(ns.Name) {
		token := BobToken
		bodyBytes := bytes.NewReader([]byte(r.body))
		req, err := http.NewRequest(r.verb, kubeConfig.Host+r.URL, bodyBytes)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
		req.Header.Set("Impersonate-User", "alice")
		func() {
			resp, err := transport.RoundTrip(req)
			if err != nil {
				t.Logf("case %v", r)
				t.Fatalf("unexpected error: %v", err)
			}
			defer resp.Body.Close()
			// 期望所有请求通过鉴权，不关心实际操作结果。
			if resp.StatusCode == http.StatusForbidden {
				t.Logf("case %v", r)
				t.Errorf("Expected status not %v, but got %v", http.StatusForbidden, resp.StatusCode)
			}
		}()
	}

	// alice 不能模拟 bob。
	for _, r := range getTestRequests(ns.Name) {
		token := AliceToken
		bodyBytes := bytes.NewReader([]byte(r.body))
		req, err := http.NewRequest(r.verb, kubeConfig.Host+r.URL, bodyBytes)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
		req.Header.Set("Impersonate-User", "bob")

		func() {
			resp, err := transport.RoundTrip(req)
			if err != nil {
				t.Logf("case %v", r)
				t.Fatalf("unexpected error: %v", err)
			}
			defer resp.Body.Close()
			// 期望 bob 的所有操作都返回 Forbidden。
			if resp.StatusCode != http.StatusForbidden {
				t.Logf("case %v", r)
				t.Errorf("Expected not status Forbidden, but got %s", resp.Status)
			}
		}()
	}

	// bob 可以模拟 service account。
	for _, r := range getTestRequests(ns.Name) {
		token := BobToken
		bodyBytes := bytes.NewReader([]byte(r.body))
		req, err := http.NewRequest(r.verb, kubeConfig.Host+r.URL, bodyBytes)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
		req.Header.Set("Impersonate-User", serviceaccount.MakeUsername("default", "default"))
		func() {
			resp, err := transport.RoundTrip(req)
			if err != nil {
				t.Logf("case %v", r)
				t.Fatalf("unexpected error: %v", err)
			}
			defer resp.Body.Close()
			// 期望所有请求通过鉴权，不关心实际操作结果。
			if resp.StatusCode == http.StatusForbidden {
				t.Logf("case %v", r)
				t.Errorf("Expected status not %v, but got %v", http.StatusForbidden, resp.StatusCode)
			}
		}()
	}

}

func TestImpersonateWithUID(t *testing.T) {
	auditPolicyFile, auditLogFile := setupImpersonationAuditFiles(t)
	server := kubeapiservertesting.StartTestServerOrDie(
		t,
		nil,
		[]string{
			"--authorization-mode=RBAC",
			"--anonymous-auth",
			"--audit-policy-file", auditPolicyFile,
			"--audit-log-path", auditLogFile,
			"--audit-log-version", "audit.k8s.io/v1",
			"--audit-log-mode", "blocking",
		},
		framework.SharedEtcd(),
	)
	t.Cleanup(server.TearDownFn)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	t.Run("impersonation with uid header", func(t *testing.T) {
		adminClient := clientset.NewForConfigOrDie(server.ClientConfig)

		authutil.GrantUserAuthorization(t, ctx, adminClient, "alice",
			rbacv1.PolicyRule{
				Verbs:     []string{"create"},
				APIGroups: []string{"certificates.k8s.io"},
				Resources: []string{"certificatesigningrequests"},
			},
		)

		req := csrPEM(t)

		clientConfig := rest.CopyConfig(server.ClientConfig)
		clientConfig.Impersonate = rest.ImpersonationConfig{
			UserName: "alice",
			UID:      "1234",
		}

		client := clientset.NewForConfigOrDie(clientConfig)
		createdCsr, err := client.CertificatesV1().CertificateSigningRequests().Create(
			ctx,
			&certificatesv1.CertificateSigningRequest{
				Spec: certificatesv1.CertificateSigningRequestSpec{
					SignerName: "kubernetes.io/kube-apiserver-client",
					Request:    req,
					Usages:     []certificatesv1.KeyUsage{"client auth"},
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: "impersonated-csr",
				},
			},
			metav1.CreateOptions{},
		)
		if err != nil {
			t.Fatalf("Unexpected error creating Certificate Signing Request: %v", err)
		}

		// 返回的 spec 中应保留所有原始字段，并带上被模拟用户的信息。
		expectedCsrSpec := certificatesv1.CertificateSigningRequestSpec{
			Groups:     []string{"system:authenticated"},
			SignerName: "kubernetes.io/kube-apiserver-client",
			Request:    req,
			Usages:     []certificatesv1.KeyUsage{"client auth"},
			Username:   "alice",
			UID:        "1234",
		}
		actualCsrSpec := createdCsr.Spec

		if diff := cmp.Diff(expectedCsrSpec, actualCsrSpec); diff != "" {
			t.Fatalf("CSR spec was different than expected, -got, +want:\n %s", diff)
		}

		withUID := allowedImpersonationEvent("create", http.StatusCreated, "alice", "system:authenticated", "certificatesigningrequests", new("impersonate:user-info"))
		withUID.ImpersonatedUID = "1234"
		assertImpersonationAuditEventsNoLatency(t, auditLogFile, user.APIServerUser, withUID)
	})

	t.Run("impersonation with only UID fails", func(t *testing.T) {
		clientConfig := rest.CopyConfig(server.ClientConfig)
		clientConfig.Impersonate = rest.ImpersonationConfig{
			UID: "1234",
		}

		client := clientset.NewForConfigOrDie(clientConfig)
		_, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})

		if !errors.IsBadRequest(err) {
			t.Fatalf("expected bad request, got %T %v", err, err)
		}
		if diff := cmp.Diff(
			`requested &user.DefaultInfo{Name:"", UID:"1234", Groups:[]string(nil), Extra:map[string][]string(nil)} without impersonating a user name`,
			err.Error(),
		); diff != "" {
			t.Fatalf("bad request different than expected, -got, +want:\n %s", diff)
		}
	})

	t.Run("impersonating UID without authorization fails", func(t *testing.T) {
		truncateAuditLog(t, auditLogFile)
		adminClient := clientset.NewForConfigOrDie(server.ClientConfig)

		authutil.GrantUserAuthorization(t, ctx, adminClient, "system:anonymous",
			rbacv1.PolicyRule{
				Verbs:         []string{"impersonate:user-info"},
				APIGroups:     []string{"authentication.k8s.io"},
				Resources:     []string{"users"},
				ResourceNames: []string{"some-user-anonymous-can-impersonate"},
			},
		)
		authutil.GrantUserAuthorization(t, ctx, adminClient, "system:anonymous",
			rbacv1.PolicyRule{
				Verbs:     []string{"impersonate-on:user-info:list"},
				APIGroups: []string{""},
				Resources: []string{"nodes"},
			},
		)

		clientConfig := rest.AnonymousClientConfig(server.ClientConfig)
		clientConfig.Impersonate = rest.ImpersonationConfig{
			UserName: "some-user-anonymous-can-impersonate",
			UID:      "1234",
		}

		client := clientset.NewForConfigOrDie(clientConfig)
		_, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})

		if !errors.IsForbidden(err) {
			t.Fatalf("expected forbidden error, got %T %v", err, err)
		}
		if diff := cmp.Diff(
			`uids.authentication.k8s.io "1234" is forbidden: `+
				`User "system:anonymous" cannot impersonate:user-info resource "uids" in API group "authentication.k8s.io" at the cluster scope`,
			err.Error(),
		); diff != "" {
			t.Fatalf("forbidden error different than expected, -got, +want:\n %s", diff)
		}

		assertImpersonationAuditEventsNoLatency(t, auditLogFile, "system:anonymous",
			deniedImpersonationEvent("list", `uids.authentication.k8s.io "1234" is forbidden: User "system:anonymous" cannot impersonate:user-info resource "uids" in API group "authentication.k8s.io" at the cluster scope`, "nodes"),
		)
	})
}

// TestConstrainedImpersonation 测试受约束的 impersonation 功能：
// 只有具备相应权限的用户才能代表他人执行操作。
// 覆盖场景：普通用户模拟另一用户、模拟 Node、service account 模拟其所在 Node，
// 以及功能开启时回退到 legacy impersonation 的行为。
func TestConstrainedImpersonation(t *testing.T) {
	superUser := "admin/system:masters"

	authenticator := group.NewAuthenticatedGroupAdder(bearertoken.New(tokenfile.New(map[string]*user.DefaultInfo{
		superUser: {Name: "admin", Groups: []string{"system:masters"}},
		"bob":     {Name: "bob"},
		"alice":   {Name: "alice"},
		"node1":   {Name: "system:node:node1", Groups: []string{user.NodesGroup}},
		"serviceaccount1": {Name: "system:serviceaccount:default:sa1", Extra: map[string][]string{
			"authentication.kubernetes.io/node-name": {"node1"},
		}},
		"serviceaccount2": {Name: "system:serviceaccount:default:sa2", Extra: map[string][]string{
			"authentication.kubernetes.io/node-name": {"node2"},
		}},
	})))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ConstrainedImpersonation, true)
	var auditLogFile string
	_, kubeConfig, tearDownFn := framework.StartTestServer(ctx, t, framework.TestServerSetup{
		ModifyServerRunOptions: func(opts *options.ServerRunOptions) {
			opts.Authorization.Modes = []string{"RBAC"}
			auditLogFile = setupImpersonationAudit(t, opts)
		},
		ModifyServerConfig: func(config *controlplane.Config) {
			config.ControlPlane.Generic.Authentication.Authenticator = authenticator
			config.ControlPlane.Generic.RequestInfoResolver = &slowImpersonationRequests{
				delegate: server.NewRequestInfoResolver(config.ControlPlane.Generic),
			}
		},
	})
	t.Cleanup(tearDownFn)

	superuserClient, _ := clientsetForToken(superUser, kubeConfig)

	// 预先授予被模拟用户所需权限。
	authutil.GrantUserAuthorization(t, ctx, superuserClient, "system:node:node1", rbacv1.PolicyRule{
		Verbs:     []string{"list"},
		APIGroups: []string{""},
		Resources: []string{"pods"},
	})
	authutil.GrantUserAuthorization(t, ctx, superuserClient, "alice", rbacv1.PolicyRule{
		Verbs:     []string{"list"},
		APIGroups: []string{""},
		Resources: []string{"pods"},
	})

	t.Run("bob impersonating alice", func(t *testing.T) {
		resetAllMetrics(t, ctx, superuserClient)
		truncateAuditLog(t, auditLogFile)

		impersonatorClientConfig := rest.CopyConfig(kubeConfig)
		impersonatorClientConfig.BearerToken = "bob"
		impersonatorClientConfig.Impersonate = rest.ImpersonationConfig{
			UserName: "alice",
		}

		client := clientset.NewForConfigOrDie(impersonatorClientConfig)
		_, err := client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
		if !errors.IsForbidden(err) {
			t.Fatalf("expected forbidden error, got %T %v", err, err)
		}

		if diff := cmp.Diff(
			`pods is forbidden: User "bob" cannot impersonate-on:user-info:list resource "pods" in API group "" at the cluster scope`,
			err.Error(),
		); diff != "" {
			t.Fatalf("forbidden error different than expected, -got, +want:\n %s", diff)
		}

		// 仅授予 impersonation:user-info 权限后，bob 仍不能 list Pod。
		authutil.GrantUserAuthorization(t, ctx, superuserClient, "bob", rbacv1.PolicyRule{
			Verbs:     []string{"impersonate:user-info"},
			APIGroups: []string{"authentication.k8s.io"},
			Resources: []string{"users"},
		})

		_, err = client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
		if !errors.IsForbidden(err) {
			t.Fatalf("expected forbidden error, got %T %v", err, err)
		}

		if diff := cmp.Diff(
			`pods is forbidden: User "bob" cannot impersonate-on:user-info:list resource "pods" in API group "" at the cluster scope`,
			err.Error(),
		); diff != "" {
			t.Fatalf("forbidden error different than expected, -got, +want:\n %s", diff)
		}

		// 再授予 impersonate-on:list 权限后，bob 可以 list 但不能 watch Pod。
		authutil.GrantUserAuthorization(t, ctx, superuserClient, "bob", rbacv1.PolicyRule{
			Verbs:     []string{"impersonate-on:user-info:list"},
			APIGroups: []string{""},
			Resources: []string{"pods"},
		})

		_, err = client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
		if err != nil {
			t.Fatalf("expected no error, got %T %v", err, err)
		}

		_, err = client.CoreV1().Pods(metav1.NamespaceAll).Watch(ctx, metav1.ListOptions{})
		if !errors.IsForbidden(err) {
			t.Fatalf("expected forbidden error, got %T %v", err, err)
		}

		assertImpersonationMetrics(t, ctx, superuserClient, []string{
			`apiserver_impersonation_attempts_duration_seconds_count{decision="allowed",mode="user-info"} 1`,
			`apiserver_impersonation_attempts_duration_seconds_count{decision="denied",mode=""} 3`,
			`apiserver_impersonation_attempts_duration_seconds_sum{decision="allowed",mode="user-info"} FP`,
			`apiserver_impersonation_attempts_duration_seconds_sum{decision="denied",mode=""} FP`,
			`apiserver_impersonation_attempts_total{decision="allowed",mode="user-info"} 1`,
			`apiserver_impersonation_attempts_total{decision="denied",mode=""} 3`,
			`apiserver_impersonation_authorization_attempts_total{decision="allowed",mode="user-info"} 2`,
			`apiserver_impersonation_authorization_attempts_total{decision="denied",mode="legacy"} 3`,
			`apiserver_impersonation_authorization_attempts_total{decision="denied",mode="user-info"} 3`,
			`apiserver_impersonation_authorization_attempts_duration_seconds_count{decision="allowed",mode="user-info"} 2`,
			`apiserver_impersonation_authorization_attempts_duration_seconds_count{decision="denied",mode="legacy"} 3`,
			`apiserver_impersonation_authorization_attempts_duration_seconds_count{decision="denied",mode="user-info"} 3`,
			`apiserver_impersonation_authorization_attempts_duration_seconds_sum{decision="allowed",mode="user-info"} FP`,
			`apiserver_impersonation_authorization_attempts_duration_seconds_sum{decision="denied",mode="legacy"} FP`,
			`apiserver_impersonation_authorization_attempts_duration_seconds_sum{decision="denied",mode="user-info"} FP`,
		})
		assertImpersonationAuditEvents(t, auditLogFile, "bob",
			deniedImpersonationEvent("list", `pods is forbidden: User "bob" cannot impersonate-on:user-info:list resource "pods" in API group "" at the cluster scope`, "pods"),
			deniedImpersonationEvent("list", `pods is forbidden: User "bob" cannot impersonate-on:user-info:list resource "pods" in API group "" at the cluster scope`, "pods"),
			allowedImpersonationEvent("list", http.StatusOK, "alice", "system:authenticated", "pods", new("impersonate:user-info")),
			deniedImpersonationEvent("watch", `pods is forbidden: User "bob" cannot impersonate-on:user-info:watch resource "pods" in API group "" at the cluster scope`, "pods"),
		)
	})

	t.Run("bob impersonating a node", func(t *testing.T) {
		resetAllMetrics(t, ctx, superuserClient)
		truncateAuditLog(t, auditLogFile)

		impersonatorClientConfig := rest.CopyConfig(kubeConfig)
		impersonatorClientConfig.BearerToken = "bob"
		impersonatorClientConfig.Impersonate = rest.ImpersonationConfig{
			UserName: "system:node:node1",
		}

		client := clientset.NewForConfigOrDie(impersonatorClientConfig)
		_, err := client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
		if !errors.IsForbidden(err) {
			t.Fatalf("expected forbidden error, got %T %v", err, err)
		}

		if diff := cmp.Diff(
			`pods is forbidden: User "bob" cannot impersonate-on:arbitrary-node:list resource "pods" in API group "" at the cluster scope`,
			err.Error(),
		); diff != "" {
			t.Fatalf("forbidden error different than expected, -got, +want:\n %s", diff)
		}

		// 即使授予上述权限，bob 仍不能 list Pod，因为还缺少模拟 Node 的权限。
		authutil.GrantUserAuthorization(t, ctx, superuserClient, "bob", rbacv1.PolicyRule{
			Verbs:     []string{"impersonate:user-info"},
			APIGroups: []string{"authentication.k8s.io"},
			Resources: []string{"users"},
		})
		authutil.GrantUserAuthorization(t, ctx, superuserClient, "bob", rbacv1.PolicyRule{
			Verbs:     []string{"impersonate-on:arbitrary-node:list"},
			APIGroups: []string{""},
			Resources: []string{"pods"},
		})

		_, err = client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
		if !errors.IsForbidden(err) {
			t.Fatalf("expected forbidden error, got %T %v", err, err)
		}

		if diff := cmp.Diff(
			`nodes.authentication.k8s.io "node1" is forbidden: User "bob" cannot impersonate:arbitrary-node resource "nodes" in API group "authentication.k8s.io" at the cluster scope`,
			err.Error(),
		); diff != "" {
			t.Fatalf("forbidden error different than expected, -got, +want:\n %s", diff)
		}

		authutil.GrantUserAuthorization(t, ctx, superuserClient, "bob", rbacv1.PolicyRule{
			Verbs:     []string{"impersonate:arbitrary-node"},
			APIGroups: []string{"authentication.k8s.io"},
			Resources: []string{"nodes"},
		})

		_, err = client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
		if err != nil {
			t.Fatalf("expected no error, got %T %v", err, err)
		}

		assertImpersonationMetrics(t, ctx, superuserClient, []string{
			`apiserver_impersonation_attempts_duration_seconds_count{decision="allowed",mode="arbitrary-node"} 1`,
			`apiserver_impersonation_attempts_duration_seconds_count{decision="denied",mode=""} 2`,
			`apiserver_impersonation_attempts_duration_seconds_sum{decision="allowed",mode="arbitrary-node"} FP`,
			`apiserver_impersonation_attempts_duration_seconds_sum{decision="denied",mode=""} FP`,
			`apiserver_impersonation_attempts_total{decision="allowed",mode="arbitrary-node"} 1`,
			`apiserver_impersonation_attempts_total{decision="denied",mode=""} 2`,
			`apiserver_impersonation_authorization_attempts_total{decision="allowed",mode="arbitrary-node"} 3`,
			`apiserver_impersonation_authorization_attempts_total{decision="denied",mode="arbitrary-node"} 2`,
			`apiserver_impersonation_authorization_attempts_total{decision="denied",mode="legacy"} 2`,
			`apiserver_impersonation_authorization_attempts_duration_seconds_count{decision="allowed",mode="arbitrary-node"} 3`,
			`apiserver_impersonation_authorization_attempts_duration_seconds_count{decision="denied",mode="arbitrary-node"} 2`,
			`apiserver_impersonation_authorization_attempts_duration_seconds_count{decision="denied",mode="legacy"} 2`,
			`apiserver_impersonation_authorization_attempts_duration_seconds_sum{decision="allowed",mode="arbitrary-node"} FP`,
			`apiserver_impersonation_authorization_attempts_duration_seconds_sum{decision="denied",mode="arbitrary-node"} FP`,
			`apiserver_impersonation_authorization_attempts_duration_seconds_sum{decision="denied",mode="legacy"} FP`,
		})
		assertImpersonationAuditEvents(t, auditLogFile, "bob",
			deniedImpersonationEvent("list", `pods is forbidden: User "bob" cannot impersonate-on:arbitrary-node:list resource "pods" in API group "" at the cluster scope`, "pods"),
			deniedImpersonationEvent("list", `nodes.authentication.k8s.io "node1" is forbidden: User "bob" cannot impersonate:arbitrary-node resource "nodes" in API group "authentication.k8s.io" at the cluster scope`, "pods"),
			allowedImpersonationEvent("list", http.StatusOK, "system:node:node1", "system:authenticated,system:nodes", "pods", new("impersonate:arbitrary-node")),
		)
	})

	t.Run("impersonating scheduled node", func(t *testing.T) {
		resetAllMetrics(t, ctx, superuserClient)
		truncateAuditLog(t, auditLogFile)

		impersonatorClientConfig := rest.CopyConfig(kubeConfig)
		impersonatorClientConfig.BearerToken = "serviceaccount2"
		impersonatorClientConfig.Impersonate = rest.ImpersonationConfig{
			UserName: "system:node:node1",
		}

		client := clientset.NewForConfigOrDie(impersonatorClientConfig)

		// 即使授予权限，位于 node2 的 sa 仍不能模拟 node1 来 list Pod。
		authutil.GrantUserAuthorization(t, ctx, superuserClient, "system:serviceaccount:default:sa2", rbacv1.PolicyRule{
			Verbs:     []string{"impersonate:associated-node"},
			APIGroups: []string{"authentication.k8s.io"},
			Resources: []string{"nodes"},
		})
		authutil.GrantUserAuthorization(t, ctx, superuserClient, "system:serviceaccount:default:sa2", rbacv1.PolicyRule{
			Verbs:     []string{"impersonate-on:associated-node:list"},
			APIGroups: []string{""},
			Resources: []string{"pods"},
		})

		_, err := client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
		if !errors.IsForbidden(err) {
			t.Fatalf("expected forbidden error, got %T %v", err, err)
		}

		if diff := cmp.Diff(
			`pods is forbidden: User "system:serviceaccount:default:sa2" cannot impersonate-on:arbitrary-node:list resource "pods" in API group "" at the cluster scope`,
			err.Error(),
		); diff != "" {
			t.Fatalf("forbidden error different than expected, -got, +want:\n %s", diff)
		}

		// 改用位于 node1 的 service account1。
		impersonatorClientConfig.BearerToken = "serviceaccount1"

		client = clientset.NewForConfigOrDie(impersonatorClientConfig)
		authutil.GrantUserAuthorization(t, ctx, superuserClient, "system:serviceaccount:default:sa1", rbacv1.PolicyRule{
			Verbs:     []string{"impersonate:associated-node"},
			APIGroups: []string{"authentication.k8s.io"},
			Resources: []string{"nodes"},
		})
		authutil.GrantUserAuthorization(t, ctx, superuserClient, "system:serviceaccount:default:sa1", rbacv1.PolicyRule{
			Verbs:     []string{"impersonate-on:associated-node:list"},
			APIGroups: []string{""},
			Resources: []string{"pods"},
		})

		_, err = client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
		if err != nil {
			t.Fatalf("expected no error, got %T %v", err, err)
		}

		assertImpersonationMetrics(t, ctx, superuserClient, []string{
			`apiserver_impersonation_attempts_duration_seconds_count{decision="allowed",mode="associated-node"} 1`,
			`apiserver_impersonation_attempts_duration_seconds_count{decision="denied",mode=""} 1`,
			`apiserver_impersonation_attempts_duration_seconds_sum{decision="allowed",mode="associated-node"} FP`,
			`apiserver_impersonation_attempts_duration_seconds_sum{decision="denied",mode=""} FP`,
			`apiserver_impersonation_attempts_total{decision="allowed",mode="associated-node"} 1`,
			`apiserver_impersonation_attempts_total{decision="denied",mode=""} 1`,
			`apiserver_impersonation_authorization_attempts_total{decision="allowed",mode="associated-node"} 2`,
			`apiserver_impersonation_authorization_attempts_total{decision="denied",mode="arbitrary-node"} 1`,
			`apiserver_impersonation_authorization_attempts_total{decision="denied",mode="legacy"} 1`,
			`apiserver_impersonation_authorization_attempts_duration_seconds_count{decision="allowed",mode="associated-node"} 2`,
			`apiserver_impersonation_authorization_attempts_duration_seconds_count{decision="denied",mode="arbitrary-node"} 1`,
			`apiserver_impersonation_authorization_attempts_duration_seconds_count{decision="denied",mode="legacy"} 1`,
			`apiserver_impersonation_authorization_attempts_duration_seconds_sum{decision="allowed",mode="associated-node"} FP`,
			`apiserver_impersonation_authorization_attempts_duration_seconds_sum{decision="denied",mode="arbitrary-node"} FP`,
			`apiserver_impersonation_authorization_attempts_duration_seconds_sum{decision="denied",mode="legacy"} FP`,
		})
		assertImpersonationAuditEvents(t, auditLogFile, "system:serviceaccount:default:sa2",
			deniedImpersonationEvent("list", `pods is forbidden: User "system:serviceaccount:default:sa2" cannot impersonate-on:arbitrary-node:list resource "pods" in API group "" at the cluster scope`, "pods"),
		)
		assertImpersonationAuditEvents(t, auditLogFile, "system:serviceaccount:default:sa1",
			allowedImpersonationEvent("list", http.StatusOK, "system:node:node1", "system:authenticated,system:nodes", "pods", new("impersonate:associated-node")),
		)
	})

	t.Run("fallback to legacy impersonation", func(t *testing.T) {
		resetAllMetrics(t, ctx, superuserClient)
		truncateAuditLog(t, auditLogFile)

		impersonatorClientConfig := rest.CopyConfig(kubeConfig)
		impersonatorClientConfig.BearerToken = "bob"
		impersonatorClientConfig.Impersonate = rest.ImpersonationConfig{
			UserName: "alice",
		}

		client := clientset.NewForConfigOrDie(impersonatorClientConfig)

		// 有了 legacy impersonation 动词权限后，bob 可以模拟 alice。
		authutil.GrantUserAuthorization(t, ctx, superuserClient, "bob", rbacv1.PolicyRule{
			Verbs:     []string{"impersonate"},
			APIGroups: []string{""},
			Resources: []string{"users"},
		})

		_, err := client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
			LabelSelector: "app=panda", // 使用不同的 label selector，使缓存 key 与前面的测试不同。
		})
		if err != nil {
			t.Fatalf("expected no error, got %T %v", err, err)
		}

		assertImpersonationMetrics(t, ctx, superuserClient, []string{
			`apiserver_impersonation_attempts_duration_seconds_count{decision="allowed",mode="legacy"} 1`,
			`apiserver_impersonation_attempts_duration_seconds_sum{decision="allowed",mode="legacy"} FP`,
			`apiserver_impersonation_attempts_total{decision="allowed",mode="legacy"} 1`,
			`apiserver_impersonation_authorization_attempts_total{decision="allowed",mode="legacy"} 1`,
			`apiserver_impersonation_authorization_attempts_total{decision="denied",mode="user-info"} 1`,
			`apiserver_impersonation_authorization_attempts_duration_seconds_count{decision="allowed",mode="legacy"} 1`,
			`apiserver_impersonation_authorization_attempts_duration_seconds_count{decision="denied",mode="user-info"} 1`,
			`apiserver_impersonation_authorization_attempts_duration_seconds_sum{decision="allowed",mode="legacy"} FP`,
			`apiserver_impersonation_authorization_attempts_duration_seconds_sum{decision="denied",mode="user-info"} FP`,
		})
		assertImpersonationAuditEvents(t, auditLogFile, "bob",
			allowedImpersonationEvent("list", http.StatusOK, "alice", "system:authenticated", "pods", nil),
		)
	})
}

func resetAllMetrics(t *testing.T, ctx context.Context, client clientset.Interface) {
	t.Helper()

	if err := client.CoreV1().RESTClient().Delete().AbsPath("/metrics").Do(ctx).Error(); err != nil {
		t.Fatalf("failed to reset metrics: %v", err)
	}
}

func assertImpersonationMetrics(t *testing.T, ctx context.Context, client clientset.Interface, wantMetricStrings []string) {
	t.Helper()

	rc := client.CoreV1().RESTClient()

	body, err := rc.Get().AbsPath("/metrics").DoRaw(ctx)
	if err != nil {
		t.Fatalf("failed to fetch metrics: %v", err)
	}

	var gotMetricStrings []string
	trimFP := regexp.MustCompile(`(} )[\de.+-]+(.*)`)
	for line := range strings.SplitSeq(string(body), "\n") {
		if !strings.HasPrefix(line, "apiserver_impersonation_") {
			continue
		}
		// 跳过 histogram bucket 行，避免断言过于冗长。
		if strings.Contains(line, "_bucket{") {
			continue
		}
		if strings.Contains(line, "_seconds_sum") {
			line = trimFP.ReplaceAllString(line, "${1}FP")
		}
		gotMetricStrings = append(gotMetricStrings, line)
	}
	slices.Sort(gotMetricStrings)
	slices.Sort(wantMetricStrings)

	if diff := cmp.Diff(wantMetricStrings, gotMetricStrings); diff != "" {
		t.Errorf("unexpected impersonation metrics diff (-want +got): %s", diff)
	}
}

const impersonationAuditPolicy = `
apiVersion: audit.k8s.io/v1
kind: Policy
omitStages:
  - "RequestReceived"
rules:
  - level: Metadata
`

func setupImpersonationAuditFiles(t *testing.T) (policyFilePath, logFilePath string) {
	t.Helper()

	dir := t.TempDir()

	policyFilePath = filepath.Join(dir, "audit-policy.yaml")
	if err := os.WriteFile(policyFilePath, []byte(impersonationAuditPolicy), 0644); err != nil {
		t.Fatalf("failed to write audit policy: %v", err)
	}

	logFilePath = filepath.Join(dir, "audit.log")

	return policyFilePath, logFilePath
}

func setupImpersonationAudit(t *testing.T, opts *options.ServerRunOptions) string {
	t.Helper()

	policyFilePath, logFilePath := setupImpersonationAuditFiles(t)
	opts.Audit.PolicyFile = policyFilePath
	opts.Audit.LogOptions.Path = logFilePath
	opts.Audit.LogOptions.GroupVersionString = "audit.k8s.io/v1"
	opts.Audit.LogOptions.BatchOptions.Mode = "blocking"

	return logFilePath
}

func truncateAuditLog(t *testing.T, logFilePath string) {
	t.Helper()
	if err := os.Truncate(logFilePath, 0); err != nil {
		t.Fatalf("failed to truncate audit log: %v", err)
	}
}

func getAuditEvents(t *testing.T, logFilePath string) []testutils.AuditEvent {
	t.Helper()
	stream, err := os.Open(logFilePath)
	if err != nil {
		t.Fatalf("failed to open audit log: %v", err)
	}
	defer func() {
		if err := stream.Close(); err != nil {
			t.Errorf("failed to close audit log: %v", err)
		}
	}()
	report, err := testutils.CheckAuditLinesFiltered(stream, nil, auditv1.SchemeGroupVersion, func(_, _ string) bool {
		return true // 保留所有 audit annotation。
	})
	if err != nil {
		t.Fatalf("failed to parse audit log: %v", err)
	}
	return report.AllEvents
}

func assertImpersonationAuditEvents(t *testing.T, logFilePath, wantUser string, wantEvents ...testutils.AuditEvent) {
	t.Helper()
	doAssertImpersonationAuditEvents(t, logFilePath, wantUser, false, wantEvents...)
}

func assertImpersonationAuditEventsNoLatency(t *testing.T, logFilePath, wantUser string, wantEvents ...testutils.AuditEvent) {
	t.Helper()
	doAssertImpersonationAuditEvents(t, logFilePath, wantUser, true, wantEvents...)
}

func doAssertImpersonationAuditEvents(t *testing.T, logFilePath, wantUser string, skipLatency bool, wantEvents ...testutils.AuditEvent) {
	t.Helper()

	latencyPattern := regexp.MustCompile("^[0-9.]+[µnm]s$")

	var matched []testutils.AuditEvent
	for _, event := range getAuditEvents(t, logFilePath) {
		if event.Stage != auditinternal.StageResponseComplete {
			continue
		}
		if event.User != wantUser {
			continue
		}
		if len(event.ImpersonatedUser) == 0 && !strings.Contains(event.StatusMessage, "impersonate") {
			continue
		}
		matched = append(matched, event)
	}
	if len(matched) != len(wantEvents) {
		t.Fatalf("expected %d audit event(s) from user %q with impersonation, got %d: %v", len(wantEvents), wantUser, len(matched), matched)
	}
	for i, event := range matched {
		got := testutils.AuditEvent{
			Verb:                    event.Verb,
			Code:                    event.Code,
			StatusMessage:           event.StatusMessage,
			ImpersonatedUser:        event.ImpersonatedUser,
			ImpersonatedUID:         event.ImpersonatedUID,
			ImpersonatedGroups:      event.ImpersonatedGroups,
			Resource:                event.Resource,
			Namespace:               event.Namespace,
			AuthorizeDecision:       event.AuthorizeDecision,
			ImpersonationConstraint: event.ImpersonationConstraint,
		}
		if diff := cmp.Diff(wantEvents[i], got); len(diff) > 0 {
			t.Errorf("audit event[%d] mismatch (-want +got): %s", i, diff)
		}
		if !skipLatency && event.Verb != "watch" && utilfeature.DefaultFeatureGate.Enabled(features.ConstrainedImpersonation) {
			latency := event.CustomAuditAnnotations["apiserver.latency.k8s.io/impersonation"]
			if !latencyPattern.MatchString(latency) {
				t.Errorf("audit event[%d] expected valid impersonation latency annotation, got %q", i, latency)
			}
		}
	}
}

func allowedImpersonationEvent(verb string, code int32, impersonatedUser, impersonatedGroups, resource string, constraint *string) testutils.AuditEvent {
	return testutils.AuditEvent{
		Verb:                    verb,
		Code:                    code,
		ImpersonatedUser:        impersonatedUser,
		ImpersonatedGroups:      impersonatedGroups,
		Resource:                resource,
		AuthorizeDecision:       "allow",
		ImpersonationConstraint: constraint,
	}
}

func deniedImpersonationEvent(verb, statusMessage, resource string) testutils.AuditEvent {
	return testutils.AuditEvent{
		Verb:          verb,
		Code:          http.StatusForbidden,
		StatusMessage: statusMessage,
		Resource:      resource,
	}
}

type slowImpersonationRequests struct {
	delegate genericrequest.RequestInfoResolver
}

func (s *slowImpersonationRequests) NewRequestInfo(req *http.Request) (*genericrequest.RequestInfo, error) {
	if len(req.Header.Get(authenticationv1.ImpersonateUserHeader)) > 0 {
		time.Sleep(505 * time.Millisecond) // 模拟延迟，确保 impersonation 请求产生 latency audit annotation。
	}
	return s.delegate.NewRequestInfo(req)
}

// TestConstrainedImpersonationDisabled 验证 ConstrainedImpersonation feature gate 关闭时的行为：
// 回退到 legacy impersonation，用户只需对目标 user、group 或 service account 拥有 impersonate 权限即可。
func TestConstrainedImpersonationDisabled(t *testing.T) {
	superUser := "admin/system:masters"

	authenticator := group.NewAuthenticatedGroupAdder(bearertoken.New(tokenfile.New(map[string]*user.DefaultInfo{
		superUser: {Name: "admin", Groups: []string{"system:masters"}},
		"bob":     {Name: "bob"},
		"alice":   {Name: "alice"},
		"node1":   {Name: "system:node:node1", Groups: []string{user.NodesGroup}},
		"serviceaccount1": {Name: "system:serviceaccount:default:sa1", Extra: map[string][]string{
			"authentication.kubernetes.io/node-name": {"node1"},
		}},
	})))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ConstrainedImpersonation, false)
	var auditLogFile string
	_, kubeConfig, tearDownFn := framework.StartTestServer(ctx, t, framework.TestServerSetup{
		ModifyServerRunOptions: func(opts *options.ServerRunOptions) {
			opts.Authorization.Modes = []string{"RBAC"}
			auditLogFile = setupImpersonationAudit(t, opts)
		},
		ModifyServerConfig: func(config *controlplane.Config) {
			config.ControlPlane.Generic.Authentication.Authenticator = authenticator
		},
	})
	t.Cleanup(tearDownFn)

	superuserClient, _ := clientsetForToken(superUser, kubeConfig)

	// 预先授予被模拟用户所需权限。
	authutil.GrantUserAuthorization(t, ctx, superuserClient, "system:node:node1", rbacv1.PolicyRule{
		Verbs:     []string{"list"},
		APIGroups: []string{""},
		Resources: []string{"pods"},
	})
	authutil.GrantUserAuthorization(t, ctx, superuserClient, "alice", rbacv1.PolicyRule{
		Verbs:     []string{"list"},
		APIGroups: []string{""},
		Resources: []string{"pods"},
	})

	t.Run("bob impersonating alice", func(t *testing.T) {
		truncateAuditLog(t, auditLogFile)

		authutil.GrantUserAuthorization(t, ctx, superuserClient, "bob", rbacv1.PolicyRule{
			Verbs:     []string{"impersonate:user-info"},
			APIGroups: []string{"authentication.k8s.io"},
			Resources: []string{"users"},
		})
		authutil.GrantUserAuthorization(t, ctx, superuserClient, "bob", rbacv1.PolicyRule{
			Verbs:     []string{"impersonate-on:user-info:list"},
			APIGroups: []string{""},
			Resources: []string{"pods"},
		})

		impersonatorClientConfig := rest.CopyConfig(kubeConfig)
		impersonatorClientConfig.BearerToken = "bob"
		impersonatorClientConfig.Impersonate = rest.ImpersonationConfig{
			UserName: "alice",
		}

		client := clientset.NewForConfigOrDie(impersonatorClientConfig)
		_, err := client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
		if !errors.IsForbidden(err) {
			t.Fatalf("expected forbidden error, got %T %v", err, err)
		}

		if diff := cmp.Diff(
			`users "alice" is forbidden: User "bob" cannot impersonate resource "users" in API group "" at the cluster scope`,
			err.Error(),
		); diff != "" {
			t.Fatalf("forbidden error different than expected, -got, +want:\n %s", diff)
		}

		// 加上 legacy impersonation 权限后，bob 可以 list Pod。
		authutil.GrantUserAuthorization(t, ctx, superuserClient, "bob", rbacv1.PolicyRule{
			Verbs:     []string{"impersonate"},
			APIGroups: []string{""},
			Resources: []string{"users"},
		})

		_, err = client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
		if err != nil {
			t.Fatalf("expected no error, got %T %v", err, err)
		}

		assertImpersonationAuditEvents(t, auditLogFile, "bob",
			deniedImpersonationEvent("list", `users "alice" is forbidden: User "bob" cannot impersonate resource "users" in API group "" at the cluster scope`, "pods"),
			allowedImpersonationEvent("list", http.StatusOK, "alice", "system:authenticated", "pods", nil),
		)
	})

	t.Run("serviceaccount impersonating a node", func(t *testing.T) {
		truncateAuditLog(t, auditLogFile)
		authutil.GrantUserAuthorization(t, ctx, superuserClient, "system:serviceaccount:default:sa1", rbacv1.PolicyRule{
			Verbs:     []string{"impersonate:associated-node"},
			APIGroups: []string{"authentication.k8s.io"},
			Resources: []string{"nodes"},
		})
		authutil.GrantUserAuthorization(t, ctx, superuserClient, "system:serviceaccount:default:sa1", rbacv1.PolicyRule{
			Verbs:     []string{"impersonate-on:associated-node:list"},
			APIGroups: []string{""},
			Resources: []string{"pods"},
		})

		impersonatorClientConfig := rest.CopyConfig(kubeConfig)
		impersonatorClientConfig.BearerToken = "serviceaccount1"
		impersonatorClientConfig.Impersonate = rest.ImpersonationConfig{
			UserName: "system:node:node1",
		}

		client := clientset.NewForConfigOrDie(impersonatorClientConfig)
		_, err := client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
		if !errors.IsForbidden(err) {
			t.Fatalf("expected forbidden error, got %T %v", err, err)
		}

		if diff := cmp.Diff(
			`users "system:node:node1" is forbidden: User "system:serviceaccount:default:sa1" cannot impersonate resource "users" in API group "" at the cluster scope`,
			err.Error(),
		); diff != "" {
			t.Fatalf("forbidden error different than expected, -got, +want:\n %s", diff)
		}

		// 加上 legacy impersonation 权限后，sa 可以 list Pod。
		authutil.GrantUserAuthorization(t, ctx, superuserClient, "system:serviceaccount:default:sa1", rbacv1.PolicyRule{
			Verbs:         []string{"impersonate"},
			APIGroups:     []string{""},
			Resources:     []string{"users"},
			ResourceNames: []string{"system:node:node1"},
		})
		authutil.GrantUserAuthorization(t, ctx, superuserClient, "system:serviceaccount:default:sa1", rbacv1.PolicyRule{
			Verbs:         []string{"impersonate"},
			APIGroups:     []string{""},
			Resources:     []string{"groups"},
			ResourceNames: []string{"system:nodes"},
		})

		_, err = client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
		if err != nil {
			t.Fatalf("expected no error, got %T %v", err, err)
		}

		assertImpersonationAuditEvents(t, auditLogFile, "system:serviceaccount:default:sa1",
			deniedImpersonationEvent("list", `users "system:node:node1" is forbidden: User "system:serviceaccount:default:sa1" cannot impersonate resource "users" in API group "" at the cluster scope`, "pods"),
			allowedImpersonationEvent("list", http.StatusOK, "system:node:node1", "system:authenticated", "pods", nil),
		)
	})
}

func csrPEM(t *testing.T) []byte {
	t.Helper()

	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("Unexpected error generating ed25519 key: %v", err)
	}

	csrDER, err := x509.CreateCertificateRequest(
		rand.Reader,
		&x509.CertificateRequest{
			Subject: pkix.Name{
				Organization: []string{},
			},
		},
		privateKey)
	if err != nil {
		t.Fatalf("Unexpected error creating x509 certificate request: %v", err)
	}

	csrPemBlock := &pem.Block{
		Type:  "CERTIFICATE REQUEST",
		Bytes: csrDER,
	}

	req := pem.EncodeToMemory(csrPemBlock)
	if req == nil {
		t.Fatalf("Failed to encode PEM to memory.")
	}
	return req
}

func newABACFileWithContents(t *testing.T, contents string) string {
	dir := t.TempDir()
	file := filepath.Join(dir, "auth_test")
	if err := os.WriteFile(file, []byte(contents), 0700); err != nil {
		t.Fatalf("unexpected error writing policyfile: %v", err)
	}
	return file
}

type trackingAuthorizer struct {
	requestAttributes []authorizer.Attributes
}

func (a *trackingAuthorizer) Authorize(ctx context.Context, attributes authorizer.Attributes) (authorizer.Decision, string, error) {
	a.requestAttributes = append(a.requestAttributes, attributes)
	return authorizer.DecisionAllow, "", nil
}

// ConditionsAwareAuthorize 不支持 conditions，仅转换 Authorize 的判定结果。
func (a *trackingAuthorizer) ConditionsAwareAuthorize(ctx context.Context, attributes authorizer.Attributes) authorizer.ConditionsAwareDecision {
	return authorizer.ConditionsAwareDecisionFromParts(a.Authorize(ctx, attributes))
}

// EvaluateConditions 当前 authorizer 不支持。
func (a *trackingAuthorizer) EvaluateConditions(_ context.Context, _ authorizer.ConditionsAwareDecision, _ authorizer.ConditionsData) (authorizer.Decision, string, error) {
	return authorizer.DecisionDeny, "", authorizer.ErrorConditionEvaluationNotSupported
}

// TestAuthorizationAttributeDetermination 验证鉴权属性构造正确。
func TestAuthorizationAttributeDetermination(t *testing.T) {
	tCtx := ktesting.Init(t)

	trackingAuthorizer := &trackingAuthorizer{}

	kubeClient, kubeConfig, tearDownFn := framework.StartTestServer(tCtx, t, framework.TestServerSetup{
		ModifyServerRunOptions: func(opts *options.ServerRunOptions) {
			// 当前未运行 serviceaccount controller，需禁用 ServiceAccount admission 插件。
			opts.Admission.GenericAdmission.DisablePlugins = []string{"ServiceAccount"}
			opts.Authentication.TokenFile.TokenFile = "testdata/tokens.csv"
		},
		ModifyServerConfig: func(config *controlplane.Config) {
			authz, err := unionauthz.New(
				unionauthz.NamedAuthorizer{AuthorizerName: "default", Authorizer: config.ControlPlane.Generic.Authorization.Authorizer},
				unionauthz.NamedAuthorizer{AuthorizerName: "tracking", Authorizer: trackingAuthorizer},
			)
			if err != nil {
				t.Fatalf("unionauthz.New: %v", err)
			}
			config.ControlPlane.Generic.Authorization.Authorizer = authz
		},
	})
	defer tearDownFn()

	ns := framework.CreateNamespaceOrDie(kubeClient, "auth-attribute-determination", t)
	defer framework.DeleteNamespaceOrDie(kubeClient, ns, t)

	transport, err := rest.TransportFor(kubeConfig)
	if err != nil {
		t.Fatal(err)
	}

	requests := map[string]struct {
		verb               string
		URL                string
		expectedAttributes authorizer.Attributes
	}{
		"prefix/version/resource":        {"GET", "/api/v1/pods", authorizer.AttributesRecord{APIGroup: api.GroupName, Resource: "pods"}},
		"prefix/group/version/resource":  {"GET", "/apis/extensions/v1/pods", authorizer.AttributesRecord{APIGroup: extensions.GroupName, Resource: "pods"}},
		"prefix/group/version/resource2": {"GET", "/apis/autoscaling/v1/horizontalpodautoscalers", authorizer.AttributesRecord{APIGroup: autoscaling.GroupName, Resource: "horizontalpodautoscalers"}},
	}

	currentAuthorizationAttributesIndex := 0

	for testName, r := range requests {
		token := BobToken
		req, err := http.NewRequest(r.verb, kubeConfig.Host+r.URL, nil)
		if err != nil {
			t.Logf("case %v", testName)
			t.Fatalf("unexpected error: %v", err)
		}
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
		func() {
			resp, err := transport.RoundTrip(req)
			if err != nil {
				t.Logf("case %v", r)
				t.Fatalf("unexpected error: %v", err)
			}
			defer resp.Body.Close()

			found := false
			for i := currentAuthorizationAttributesIndex; i < len(trackingAuthorizer.requestAttributes); i++ {
				if trackingAuthorizer.requestAttributes[i].GetAPIGroup() == r.expectedAttributes.GetAPIGroup() &&
					trackingAuthorizer.requestAttributes[i].GetResource() == r.expectedAttributes.GetResource() {
					found = true
					break
				}

				t.Logf("%#v did not match %#v", r.expectedAttributes, trackingAuthorizer.requestAttributes[i].(*authorizer.AttributesRecord))
			}
			if !found {
				t.Errorf("did not find %#v in %#v", r.expectedAttributes, trackingAuthorizer.requestAttributes[currentAuthorizationAttributesIndex:])
			}

			currentAuthorizationAttributesIndex = len(trackingAuthorizer.requestAttributes)
		}()
	}
}

// TestNamespaceAuthorization 验证可按 namespace 控制鉴权。
func TestNamespaceAuthorization(t *testing.T) {
	tCtx := ktesting.Init(t)

	kubeClient, kubeConfig, tearDownFn := framework.StartTestServer(tCtx, t, framework.TestServerSetup{
		ModifyServerRunOptions: func(opts *options.ServerRunOptions) {
			// 当前未运行 serviceaccount controller，需禁用 ServiceAccount admission 插件。
			opts.Admission.GenericAdmission.DisablePlugins = []string{"ServiceAccount"}
			opts.Authentication.TokenFile.TokenFile = "testdata/tokens.csv"
			opts.Authorization.PolicyFile = newABACFileWithContents(t, `{"namespace": "auth-namespace"}`)
			opts.Authorization.Modes = []string{"ABAC"}
		},
	})
	defer tearDownFn()

	ns := framework.CreateNamespaceOrDie(kubeClient, "auth-namespace", t)
	defer framework.DeleteNamespaceOrDie(kubeClient, ns, t)

	previousResourceVersion := make(map[string]float64)
	transport, err := rest.TransportFor(kubeConfig)
	if err != nil {
		t.Fatal(err)
	}

	requests := []struct {
		verb        string
		URL         string
		namespace   string
		body        string
		statusCodes map[int]bool // 允许的状态码。
	}{

		{"POST", timeoutPath("pods", ns.Name, ""), "foo", aPod, integration.Code201},
		{"GET", path("pods", ns.Name, ""), "foo", "", integration.Code200},
		{"GET", path("pods", ns.Name, "a"), "foo", "", integration.Code200},
		{"DELETE", timeoutPath("pods", ns.Name, "a"), "foo", "", integration.Code200},

		{"POST", timeoutPath("pods", "foo", ""), "bar", aPod, integration.Code403},
		{"GET", path("pods", "foo", ""), "bar", "", integration.Code403},
		{"GET", path("pods", "foo", "a"), "bar", "", integration.Code403},
		{"DELETE", timeoutPath("pods", "foo", "a"), "bar", "", integration.Code403},

		{"POST", timeoutPath("pods", metav1.NamespaceDefault, ""), "", aPod, integration.Code403},
		{"GET", path("pods", "", ""), "", "", integration.Code403},
		{"GET", path("pods", metav1.NamespaceDefault, "a"), "", "", integration.Code403},
		{"DELETE", timeoutPath("pods", metav1.NamespaceDefault, "a"), "", "", integration.Code403},
	}

	for _, r := range requests {
		token := BobToken
		var bodyStr string
		if r.body != "" {
			sub := ""
			if r.verb == "PUT" && r.body != "" {
				// 更新操作需带上之前的 resourceVersion。
				if resVersion := previousResourceVersion[getPreviousResourceVersionKey(r.URL, "")]; resVersion != 0 {
					sub += fmt.Sprintf(",\r\n\"resourceVersion\": \"%v\"", resVersion)
				}
				namespace := r.namespace
				// FIXME: 这样处理是否正确？
				if len(namespace) == 0 {
					namespace = "default"
				}
				sub += fmt.Sprintf(",\r\n\"namespace\": %q", namespace)
			}
			bodyStr = fmt.Sprintf(r.body, sub)
		}
		r.body = bodyStr
		bodyBytes := bytes.NewReader([]byte(bodyStr))
		req, err := http.NewRequest(r.verb, kubeConfig.Host+r.URL, bodyBytes)
		if err != nil {
			t.Logf("case %v", r)
			t.Fatalf("unexpected error: %v", err)
		}
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
		func() {
			resp, err := transport.RoundTrip(req)
			if err != nil {
				t.Logf("case %v", r)
				t.Fatalf("unexpected error: %v", err)
			}
			defer resp.Body.Close()
			b, _ := io.ReadAll(resp.Body)
			if _, ok := r.statusCodes[resp.StatusCode]; !ok {
				t.Logf("case %v", r)
				t.Errorf("Expected status one of %v, but got %v", r.statusCodes, resp.StatusCode)
				t.Errorf("Body: %v", string(b))
			} else {
				if r.verb == "POST" {
					// 创建成功后提取 resourceVersion，供后续更新使用。
					id, currentResourceVersion, err := parseResourceVersion(b)
					if err == nil {
						key := getPreviousResourceVersionKey(r.URL, id)
						previousResourceVersion[key] = currentResourceVersion
					}
				}
			}

		}()
	}
}

// TestKindAuthorization 验证可按资源类型控制鉴权。
func TestKindAuthorization(t *testing.T) {
	tCtx := ktesting.Init(t)

	kubeClient, kubeConfig, tearDownFn := framework.StartTestServer(tCtx, t, framework.TestServerSetup{
		ModifyServerRunOptions: func(opts *options.ServerRunOptions) {
			// 当前未运行 serviceaccount controller，需禁用 ServiceAccount admission 插件。
			opts.Admission.GenericAdmission.DisablePlugins = []string{"ServiceAccount"}
			opts.Authentication.TokenFile.TokenFile = "testdata/tokens.csv"
			opts.Authorization.PolicyFile = newABACFileWithContents(t, `{"resource": "services"}`)
			opts.Authorization.Modes = []string{"ABAC"}
		},
	})
	defer tearDownFn()

	ns := framework.CreateNamespaceOrDie(kubeClient, "auth-kind", t)
	defer framework.DeleteNamespaceOrDie(kubeClient, ns, t)

	previousResourceVersion := make(map[string]float64)
	transport, err := rest.TransportFor(kubeConfig)
	if err != nil {
		t.Fatal(err)
	}

	requests := []testRequest{
		{"POST", timeoutPath("services", ns.Name, ""), aService, integration.Code201},
		{"GET", path("services", ns.Name, ""), "", integration.Code200},
		{"GET", path("services", ns.Name, "a"), "", integration.Code200},
		{"DELETE", timeoutPath("services", ns.Name, "a"), "", integration.Code200},

		{"POST", timeoutPath("pods", ns.Name, ""), aPod, integration.Code403},
		{"GET", path("pods", "", ""), "", integration.Code403},
		{"GET", path("pods", ns.Name, "a"), "", integration.Code403},
		{"DELETE", timeoutPath("pods", ns.Name, "a"), "", integration.Code403},
	}

	for _, r := range requests {
		token := BobToken
		var bodyStr string
		if r.body != "" {
			bodyStr = fmt.Sprintf(r.body, "")
			if r.verb == "PUT" && r.body != "" {
				// 更新操作需带上之前的 resourceVersion。
				if resVersion := previousResourceVersion[getPreviousResourceVersionKey(r.URL, "")]; resVersion != 0 {
					resourceVersionJSON := fmt.Sprintf(",\r\n\"resourceVersion\": \"%v\"", resVersion)
					bodyStr = fmt.Sprintf(r.body, resourceVersionJSON)
				}
			}
		}
		r.body = bodyStr
		bodyBytes := bytes.NewReader([]byte(bodyStr))
		req, err := http.NewRequest(r.verb, kubeConfig.Host+r.URL, bodyBytes)
		if err != nil {
			t.Logf("case %v", r)
			t.Fatalf("unexpected error: %v", err)
		}
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
		func() {
			resp, err := transport.RoundTrip(req)
			if err != nil {
				t.Logf("case %v", r)
				t.Fatalf("unexpected error: %v", err)
			}
			defer resp.Body.Close()
			b, _ := io.ReadAll(resp.Body)
			if _, ok := r.statusCodes[resp.StatusCode]; !ok {
				t.Logf("case %v", r)
				t.Errorf("Expected status one of %v, but got %v", r.statusCodes, resp.StatusCode)
				t.Errorf("Body: %v", string(b))
			} else {
				if r.verb == "POST" {
					// 创建成功后提取 resourceVersion，供后续更新使用。
					id, currentResourceVersion, err := parseResourceVersion(b)
					if err == nil {
						key := getPreviousResourceVersionKey(r.URL, id)
						previousResourceVersion[key] = currentResourceVersion
					}
				}
			}

		}()
	}
}

// TestReadOnlyAuthorization 验证只读策略下的鉴权行为。
func TestReadOnlyAuthorization(t *testing.T) {
	tCtx := ktesting.Init(t)
	kubeClient, kubeConfig, tearDownFn := framework.StartTestServer(tCtx, t, framework.TestServerSetup{
		ModifyServerRunOptions: func(opts *options.ServerRunOptions) {
			// 当前未运行 serviceaccount controller，需禁用 ServiceAccount admission 插件。
			opts.Admission.GenericAdmission.DisablePlugins = []string{"ServiceAccount"}
			opts.Authentication.TokenFile.TokenFile = "testdata/tokens.csv"
			opts.Authorization.PolicyFile = newABACFileWithContents(t, `{"readonly": true}`)
			opts.Authorization.Modes = []string{"ABAC"}
		},
	})
	defer tearDownFn()

	ns := framework.CreateNamespaceOrDie(kubeClient, "auth-read-only", t)
	defer framework.DeleteNamespaceOrDie(kubeClient, ns, t)

	transport, err := rest.TransportFor(kubeConfig)
	if err != nil {
		t.Fatal(err)
	}

	requests := []testRequest{
		{"POST", path("pods", ns.Name, ""), aPod, integration.Code403},
		{"GET", path("pods", ns.Name, ""), "", integration.Code200},
		{"GET", path("pods", metav1.NamespaceDefault, "a"), "", integration.Code404},
	}

	for _, r := range requests {
		token := BobToken
		bodyBytes := bytes.NewReader([]byte(r.body))
		req, err := http.NewRequest(r.verb, kubeConfig.Host+r.URL, bodyBytes)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
		func() {
			resp, err := transport.RoundTrip(req)
			if err != nil {
				t.Logf("case %v", r)
				t.Fatalf("unexpected error: %v", err)
			}
			defer resp.Body.Close()
			if _, ok := r.statusCodes[resp.StatusCode]; !ok {
				t.Logf("case %v", r)
				t.Errorf("Expected status one of %v, but got %v", r.statusCodes, resp.StatusCode)
				b, _ := io.ReadAll(resp.Body)
				t.Errorf("Body: %v", string(b))
			}
		}()
	}
}

// TestWebhookTokenAuthenticator 验证 control plane 可通过 webhook token authenticator
// 调用远端服务进行认证决策。
func TestWebhookTokenAuthenticator(t *testing.T) {
	testWebhookTokenAuthenticator(false, t)
}

// TestWebhookTokenAuthenticatorCustomDial 与 TestWebhookTokenAuthenticator 相同，但使用自定义 dialer。
func TestWebhookTokenAuthenticatorCustomDial(t *testing.T) {
	testWebhookTokenAuthenticator(true, t)
}

func testWebhookTokenAuthenticator(customDialer bool, t *testing.T) {
	tCtx := ktesting.Init(t)
	authServer := newTestWebhookTokenAuthServer()
	defer authServer.Close()
	var authenticator authenticator.Request
	var err error

	if customDialer == false {
		authenticator, err = getTestWebhookTokenAuth(authServer.URL, nil)
	} else {
		authenticator, err = getTestWebhookTokenAuthCustomDialer(authServer.URL)
	}

	if err != nil {
		t.Fatalf("error starting webhook token authenticator server: %v", err)
	}

	kubeClient, kubeConfig, tearDownFn := framework.StartTestServer(tCtx, t, framework.TestServerSetup{
		ModifyServerRunOptions: func(opts *options.ServerRunOptions) {
			// 当前未运行 serviceaccount controller，需禁用 ServiceAccount admission 插件。
			opts.Admission.GenericAdmission.DisablePlugins = []string{"ServiceAccount"}
			opts.Authorization.Modes = []string{"ABAC"}
			opts.Authorization.PolicyFile = "testdata/allowalice.jsonl"
		},
		ModifyServerConfig: func(config *controlplane.Config) {
			config.ControlPlane.Generic.Authentication.Authenticator = group.NewAuthenticatedGroupAdder(authenticator)
			// testserver 默认设置 API audiences，此处禁用其检查。
			config.ControlPlane.Generic.Authentication.APIAudiences = nil
		},
	})
	defer tearDownFn()

	ns := framework.CreateNamespaceOrDie(kubeClient, "auth-webhook-token", t)
	defer framework.DeleteNamespaceOrDie(kubeClient, ns, t)

	transport, err := rest.TransportFor(kubeConfig)
	if err != nil {
		t.Fatal(err)
	}

	for _, r := range getTestRequests(ns.Name) {
		// 期望 Bob 的所有请求失败。
		token := BobToken
		bodyBytes := bytes.NewReader([]byte(r.body))
		req, err := http.NewRequest(r.verb, kubeConfig.Host+r.URL, bodyBytes)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

		func() {
			resp, err := transport.RoundTrip(req)
			if err != nil {
				t.Logf("case %v", r)
				t.Fatalf("unexpected error: %v", err)
			}
			defer resp.Body.Close()
			// 期望 Bob 的所有操作都返回 Forbidden。
			if resp.StatusCode != http.StatusForbidden {
				t.Logf("case %v", r)
				t.Errorf("Expected http.Forbidden, but got %s", resp.Status)
			}
		}()
		// 期望 Alice 的所有请求成功。
		token = AliceToken
		bodyBytes = bytes.NewReader([]byte(r.body))
		req, err = http.NewRequest(r.verb, kubeConfig.Host+r.URL, bodyBytes)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

		func() {
			resp, err := transport.RoundTrip(req)
			if err != nil {
				t.Logf("case %v", r)
				t.Fatalf("unexpected error: %v", err)
			}
			defer resp.Body.Close()
			// 期望 Alice 的操作至少能通过 authn/authz。
			if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
				t.Logf("case %v", r)
				t.Errorf("Expected something other than Unauthorized/Forbidden, but got %s", resp.Status)
			}
		}()
	}
}

// newTestWebhookTokenAuthServer 创建一个认识 Alice 和 Bob 的 HTTP token 认证服务。
func newTestWebhookTokenAuthServer() *httptest.Server {
	serveHTTP := func(w http.ResponseWriter, r *http.Request) {
		var review authenticationv1beta1.TokenReview
		if err := json.NewDecoder(r.Body).Decode(&review); err != nil {
			http.Error(w, fmt.Sprintf("failed to decode body: %v", err), http.StatusBadRequest)
			return
		}
		type userInfo struct {
			Username string   `json:"username"`
			UID      string   `json:"uid"`
			Groups   []string `json:"groups"`
		}
		type status struct {
			Authenticated bool     `json:"authenticated"`
			User          userInfo `json:"user"`
		}
		var username, uid string
		authenticated := false
		if review.Spec.Token == AliceToken {
			authenticated, username, uid = true, "alice", "1"
		} else if review.Spec.Token == BobToken {
			authenticated, username, uid = true, "bob", "2"
		}

		resp := struct {
			APIVersion string `json:"apiVersion"`
			Status     status `json:"status"`
		}{
			APIVersion: authenticationv1beta1.SchemeGroupVersion.String(),
			Status: status{
				authenticated,
				userInfo{
					Username: username,
					UID:      uid,
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}

	server := httptest.NewUnstartedServer(http.HandlerFunc(serveHTTP))
	server.Start()
	return server
}

func TestSloppySANCertificates(t *testing.T) {
	tCtx := ktesting.Init(t)
	_, kubeConfig, tearDownFn := framework.StartTestServer(tCtx, t, framework.TestServerSetup{
		ModifyServerRunOptions: func(opts *options.ServerRunOptions) {
			// 向 ClientCA 追加测试用 CA 证书。
			fmt.Println(opts.Authentication.ClientCert.ClientCA)
			caData, err := os.ReadFile(opts.Authentication.ClientCert.ClientCA)
			if err != nil {
				t.Fatal(err)
			}
			sloppyCAData, err := os.ReadFile("testdata/sloppy-san-root.pem")
			if err != nil {
				t.Fatal(err)
			}
			err = os.WriteFile(opts.Authentication.ClientCert.ClientCA, []byte(string(caData)+"\n"+string(sloppyCAData)), os.FileMode(0644))
			if err != nil {
				t.Fatal(err)
			}
		},
	})
	defer tearDownFn()

	var err error
	kubeConfig = rest.AnonymousClientConfig(kubeConfig)
	kubeConfig.CertData, err = os.ReadFile("testdata/sloppy-san-client.pem")
	if err != nil {
		t.Fatal(err)
	}
	kubeConfig.KeyData, err = os.ReadFile("testdata/sloppy-san-client-key.pem")
	if err != nil {
		t.Fatal(err)
	}
	c, err := clientset.NewForConfig(kubeConfig)
	if err != nil {
		t.Fatal(err)
	}
	r, err := c.AuthenticationV1().SelfSubjectReviews().Create(tCtx, &authenticationv1.SelfSubjectReview{}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if r.Status.UserInfo.Username != "sloppy-san-client" {
		t.Fatalf("expected sloppy-san-client, got %#v", r.Status.UserInfo)
	}
}
