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

package lifecycle

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/client-go/tools/record"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
	ndf "k8s.io/component-helpers/nodedeclaredfeatures"
	ndftesting "k8s.io/component-helpers/nodedeclaredfeatures/testing"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/kubernetes/pkg/kubelet/util/format"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// fakeContainerCommandRunner 记录 RunInContainer 的调用参数，并返回预置的输出与错误。
type fakeContainerCommandRunner struct {
	Cmd []string
	ID  kubecontainer.ContainerID
	Err error
	Msg string
}

// RunInContainer 记录命令与容器 ID，供测试断言生命周期钩子的执行内容。
func (f *fakeContainerCommandRunner) RunInContainer(_ context.Context, id kubecontainer.ContainerID, cmd []string, timeout time.Duration) ([]byte, error) {
	f.Cmd = cmd
	f.ID = id
	return []byte(f.Msg), f.Err
}

// stubPodStatusProvider 构造返回固定 Pod IP 的假状态提供者。
func stubPodStatusProvider(podIP string) podStatusProvider {
	return &fakePodStatusProvider{podIP: podIP}
}

// fakePodStatusProvider 为生命周期探测提供最小的 Pod 与 PodStatus 实现。
type fakePodStatusProvider struct {
	podIP string
}

// GetPod 返回仅含 UID 的 Pod，满足 handler 查询 Pod 的最小需求。
func (f *fakePodStatusProvider) GetPod(_ context.Context, uid types.UID) (*kubecontainer.Pod, error) {
	return &kubecontainer.Pod{ID: uid}, nil
}

// GetPodStatus 在状态中填入固定 IP，用于构造 HTTP 探测地址。
func (f *fakePodStatusProvider) GetPodStatus(_ context.Context, pod *kubecontainer.Pod) (*kubecontainer.PodStatus, error) {
	return &kubecontainer.PodStatus{
		ID:  pod.ID,
		IPs: []string{f.podIP},
	}, nil
}

// TestRunHandlerExec 验证 exec 生命周期钩子会把容器命令交给运行时执行。
func TestRunHandlerExec(t *testing.T) {
	_, tCtx := ktesting.NewTestContext(t)
	fakeCommandRunner := fakeContainerCommandRunner{}
	handlerRunner := NewHandlerRunner(&fakeHTTP{}, &fakeCommandRunner, nil, nil)

	containerID := kubecontainer.ContainerID{Type: "test", ID: "abc1234"}
	containerName := "containerFoo"

	container := v1.Container{
		Name: containerName,
		Lifecycle: &v1.Lifecycle{
			PostStart: &v1.LifecycleHandler{
				Exec: &v1.ExecAction{
					Command: []string{"ls", "-a"},
				},
			},
		},
	}

	pod := v1.Pod{}
	pod.ObjectMeta.Name = "podFoo"
	pod.ObjectMeta.Namespace = "nsFoo"
	pod.Spec.Containers = []v1.Container{container}
	_, err := handlerRunner.Run(tCtx, containerID, &pod, &container, container.Lifecycle.PostStart)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if fakeCommandRunner.ID != containerID ||
		!reflect.DeepEqual(container.Lifecycle.PostStart.Exec.Command, fakeCommandRunner.Cmd) {
		t.Errorf("unexpected commands: %v", fakeCommandRunner)
	}
}

// fakeHTTP 记录请求 URL 与头部，便于断言 HTTP 生命周期钩子。
type fakeHTTP struct {
	url     string
	headers http.Header
	err     error
	resp    *http.Response
}

// Do 保存请求信息并返回预置响应。
func (f *fakeHTTP) Do(req *http.Request) (*http.Response, error) {
	f.url = req.URL.String()
	f.headers = req.Header.Clone()
	return f.resp, f.err
}

// TestRunHandlerHttp 验证 HTTP 钩子使用 Pod IP 与端口构造请求地址。
func TestRunHandlerHttp(t *testing.T) {
	_, tCtx := ktesting.NewTestContext(t)
	fakeHTTPGetter := fakeHTTP{}
	fakePodStatusProvider := stubPodStatusProvider("127.0.0.1")
	handlerRunner := NewHandlerRunner(&fakeHTTPGetter, &fakeContainerCommandRunner{}, fakePodStatusProvider, nil)

	containerID := kubecontainer.ContainerID{Type: "test", ID: "abc1234"}
	containerName := "containerFoo"

	container := v1.Container{
		Name: containerName,
		Lifecycle: &v1.Lifecycle{
			PostStart: &v1.LifecycleHandler{
				HTTPGet: &v1.HTTPGetAction{
					Host: "foo",
					Port: intstr.FromInt32(8080),
					Path: "bar",
				},
			},
		},
	}
	pod := v1.Pod{}
	pod.ObjectMeta.Name = "podFoo"
	pod.ObjectMeta.Namespace = "nsFoo"
	pod.ObjectMeta.UID = "foo-bar-quux"
	pod.Spec.Containers = []v1.Container{container}
	_, err := handlerRunner.Run(tCtx, containerID, &pod, &container, container.Lifecycle.PostStart)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if fakeHTTPGetter.url != "http://foo:8080/bar" {
		t.Errorf("unexpected url: %s", fakeHTTPGetter.url)
	}
}

// TestRunHandlerHttpWithHeaders 验证用户自定义 HTTP 头会被正确附加。
func TestRunHandlerHttpWithHeaders(t *testing.T) {
	_, tCtx := ktesting.NewTestContext(t)
	fakeHTTPDoer := fakeHTTP{}
	fakePodStatusProvider := stubPodStatusProvider("127.0.0.1")

	handlerRunner := NewHandlerRunner(&fakeHTTPDoer, &fakeContainerCommandRunner{}, fakePodStatusProvider, nil)

	containerID := kubecontainer.ContainerID{Type: "test", ID: "abc1234"}
	containerName := "containerFoo"

	container := v1.Container{
		Name: containerName,
		Lifecycle: &v1.Lifecycle{
			PostStart: &v1.LifecycleHandler{
				HTTPGet: &v1.HTTPGetAction{
					Host: "foo",
					Port: intstr.FromInt32(8080),
					Path: "/bar",
					HTTPHeaders: []v1.HTTPHeader{
						{Name: "Foo", Value: "bar"},
					},
				},
			},
		},
	}
	pod := v1.Pod{}
	pod.ObjectMeta.Name = "podFoo"
	pod.ObjectMeta.Namespace = "nsFoo"
	pod.Spec.Containers = []v1.Container{container}
	_, err := handlerRunner.Run(tCtx, containerID, &pod, &container, container.Lifecycle.PostStart)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if fakeHTTPDoer.url != "http://foo:8080/bar" {
		t.Errorf("unexpected url: %s", fakeHTTPDoer.url)
	}
	if fakeHTTPDoer.headers["Foo"][0] != "bar" {
		t.Errorf("missing http header: %s", fakeHTTPDoer.headers)
	}
}

// TestRunHandlerHttps 验证 HTTPS scheme 与端口被保留在请求 URL 中。
func TestRunHandlerHttps(t *testing.T) {
	_, tCtx := ktesting.NewTestContext(t)
	fakeHTTPDoer := fakeHTTP{}
	fakePodStatusProvider := stubPodStatusProvider("127.0.0.1")
	handlerRunner := NewHandlerRunner(&fakeHTTPDoer, &fakeContainerCommandRunner{}, fakePodStatusProvider, nil)

	containerID := kubecontainer.ContainerID{Type: "test", ID: "abc1234"}
	containerName := "containerFoo"

	container := v1.Container{
		Name: containerName,
		Lifecycle: &v1.Lifecycle{
			PostStart: &v1.LifecycleHandler{
				HTTPGet: &v1.HTTPGetAction{
					Scheme: v1.URISchemeHTTPS,
					Host:   "foo",
					Path:   "bar",
				},
			},
		},
	}
	pod := v1.Pod{}
	pod.ObjectMeta.Name = "podFoo"
	pod.ObjectMeta.Namespace = "nsFoo"
	pod.Spec.Containers = []v1.Container{container}

	t.Run("consistent", func(t *testing.T) {
		container.Lifecycle.PostStart.HTTPGet.Port = intstr.FromInt32(70)
		pod.Spec.Containers = []v1.Container{container}
		_, err := handlerRunner.Run(tCtx, containerID, &pod, &container, container.Lifecycle.PostStart)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if fakeHTTPDoer.url != "https://foo:70/bar" {
			t.Errorf("unexpected url: %s", fakeHTTPDoer.url)
		}
	})
}

// TestRunHandlerHTTPPort 验证命名端口无法解析时会返回错误。
func TestRunHandlerHTTPPort(t *testing.T) {
	tests := []struct {
		Name        string
		Port        intstr.IntOrString
		ExpectError bool
		Expected    string
	}{
		{
			Name:     "consistent/with port",
			Port:     intstr.FromInt32(70),
			Expected: "https://foo:70/bar",
		}, {
			Name:        "consistent/without port",
			Port:        intstr.FromString(""),
			ExpectError: true,
		},
	}

	fakePodStatusProvider := stubPodStatusProvider("127.0.0.1")

	containerID := kubecontainer.ContainerID{Type: "test", ID: "abc1234"}
	containerName := "containerFoo"

	container := v1.Container{
		Name: containerName,
		Lifecycle: &v1.Lifecycle{
			PostStart: &v1.LifecycleHandler{
				HTTPGet: &v1.HTTPGetAction{
					Scheme: v1.URISchemeHTTPS,
					Host:   "foo",
					Port:   intstr.FromString("unexpected"),
					Path:   "bar",
				},
			},
		},
	}
	pod := v1.Pod{}
	pod.ObjectMeta.Name = "podFoo"
	pod.ObjectMeta.Namespace = "nsFoo"
	pod.Spec.Containers = []v1.Container{container}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			_, tCtx := ktesting.NewTestContext(t)
			fakeHTTPDoer := fakeHTTP{}
			handlerRunner := NewHandlerRunner(&fakeHTTPDoer, &fakeContainerCommandRunner{}, fakePodStatusProvider, nil)

			container.Lifecycle.PostStart.HTTPGet.Port = tt.Port
			pod.Spec.Containers = []v1.Container{container}
			_, err := handlerRunner.Run(tCtx, containerID, &pod, &container, container.Lifecycle.PostStart)

			if hasError := (err != nil); hasError != tt.ExpectError {
				t.Errorf("unexpected error: %v", err)
			}

			if fakeHTTPDoer.url != tt.Expected {
				t.Errorf("unexpected url: %s", fakeHTTPDoer.url)
			}
		})
	}
}

// TestRunHTTPHandler 覆盖 URL 构造的边界情况：缺失 Pod IP/host、路径斜杠、端口解析与各类 header。
func TestRunHTTPHandler(t *testing.T) {
	type expected struct {
		OldURL    string
		OldHeader http.Header
		NewURL    string
		NewHeader http.Header
	}

	tests := []struct {
		Name     string
		PodIP    string
		HTTPGet  *v1.HTTPGetAction
		Expected expected
	}{
		{
			Name:  "missing pod IP",
			PodIP: "",
			HTTPGet: &v1.HTTPGetAction{
				Path:        "foo",
				Port:        intstr.FromInt32(42),
				Host:        "example.test",
				Scheme:      "http",
				HTTPHeaders: []v1.HTTPHeader{},
			},
			Expected: expected{
				OldURL:    "http://example.test:42/foo",
				OldHeader: http.Header{},
				NewURL:    "http://example.test:42/foo",
				NewHeader: http.Header{
					"Accept":     {"*/*"},
					"User-Agent": {"kube-lifecycle/."},
				},
			},
		}, {
			Name:  "missing host",
			PodIP: "233.252.0.1",
			HTTPGet: &v1.HTTPGetAction{
				Path:        "foo",
				Port:        intstr.FromInt32(42),
				Scheme:      "http",
				HTTPHeaders: []v1.HTTPHeader{},
			},
			Expected: expected{
				OldURL:    "http://233.252.0.1:42/foo",
				OldHeader: http.Header{},
				NewURL:    "http://233.252.0.1:42/foo",
				NewHeader: http.Header{
					"Accept":     {"*/*"},
					"User-Agent": {"kube-lifecycle/."},
				},
			},
		}, {
			Name:  "path with leading slash",
			PodIP: "233.252.0.1",
			HTTPGet: &v1.HTTPGetAction{
				Path:        "/foo",
				Port:        intstr.FromInt32(42),
				Scheme:      "http",
				HTTPHeaders: []v1.HTTPHeader{},
			},
			Expected: expected{
				OldURL:    "http://233.252.0.1:42//foo",
				OldHeader: http.Header{},
				NewURL:    "http://233.252.0.1:42/foo",
				NewHeader: http.Header{
					"Accept":     {"*/*"},
					"User-Agent": {"kube-lifecycle/."},
				},
			},
		}, {
			Name:  "path without leading slash",
			PodIP: "233.252.0.1",
			HTTPGet: &v1.HTTPGetAction{
				Path:        "foo",
				Port:        intstr.FromInt32(42),
				Scheme:      "http",
				HTTPHeaders: []v1.HTTPHeader{},
			},
			Expected: expected{
				OldURL:    "http://233.252.0.1:42/foo",
				OldHeader: http.Header{},
				NewURL:    "http://233.252.0.1:42/foo",
				NewHeader: http.Header{
					"Accept":     {"*/*"},
					"User-Agent": {"kube-lifecycle/."},
				},
			},
		}, {
			Name:  "port resolution",
			PodIP: "233.252.0.1",
			HTTPGet: &v1.HTTPGetAction{
				Path:        "foo",
				Port:        intstr.FromString("quux"),
				Scheme:      "http",
				HTTPHeaders: []v1.HTTPHeader{},
			},
			Expected: expected{
				OldURL:    "http://233.252.0.1:8080/foo",
				OldHeader: http.Header{},
				NewURL:    "http://233.252.0.1:8080/foo",
				NewHeader: http.Header{
					"Accept":     {"*/*"},
					"User-Agent": {"kube-lifecycle/."},
				},
			},
		}, {
			Name:  "https",
			PodIP: "233.252.0.1",
			HTTPGet: &v1.HTTPGetAction{
				Path:        "foo",
				Port:        intstr.FromInt32(4430),
				Scheme:      "https",
				HTTPHeaders: []v1.HTTPHeader{},
			},
			Expected: expected{
				OldURL:    "http://233.252.0.1:4430/foo",
				OldHeader: http.Header{},
				NewURL:    "https://233.252.0.1:4430/foo",
				NewHeader: http.Header{
					"Accept":     {"*/*"},
					"User-Agent": {"kube-lifecycle/."},
				},
			},
		}, {
			Name:  "unknown scheme",
			PodIP: "233.252.0.1",
			HTTPGet: &v1.HTTPGetAction{
				Path:        "foo",
				Port:        intstr.FromInt32(80),
				Scheme:      "baz",
				HTTPHeaders: []v1.HTTPHeader{},
			},
			Expected: expected{
				OldURL:    "http://233.252.0.1:80/foo",
				OldHeader: http.Header{},
				NewURL:    "baz://233.252.0.1:80/foo",
				NewHeader: http.Header{
					"Accept":     {"*/*"},
					"User-Agent": {"kube-lifecycle/."},
				},
			},
		}, {
			Name:  "query param",
			PodIP: "233.252.0.1",
			HTTPGet: &v1.HTTPGetAction{
				Path:        "foo?k=v",
				Port:        intstr.FromInt32(80),
				Scheme:      "http",
				HTTPHeaders: []v1.HTTPHeader{},
			},
			Expected: expected{
				OldURL:    "http://233.252.0.1:80/foo?k=v",
				OldHeader: http.Header{},
				NewURL:    "http://233.252.0.1:80/foo?k=v",
				NewHeader: http.Header{
					"Accept":     {"*/*"},
					"User-Agent": {"kube-lifecycle/."},
				},
			},
		}, {
			Name:  "fragment",
			PodIP: "233.252.0.1",
			HTTPGet: &v1.HTTPGetAction{
				Path:        "foo#frag",
				Port:        intstr.FromInt32(80),
				Scheme:      "http",
				HTTPHeaders: []v1.HTTPHeader{},
			},
			Expected: expected{
				OldURL:    "http://233.252.0.1:80/foo#frag",
				OldHeader: http.Header{},
				NewURL:    "http://233.252.0.1:80/foo#frag",
				NewHeader: http.Header{
					"Accept":     {"*/*"},
					"User-Agent": {"kube-lifecycle/."},
				},
			},
		}, {
			Name:  "headers",
			PodIP: "233.252.0.1",
			HTTPGet: &v1.HTTPGetAction{
				Path:   "foo",
				Port:   intstr.FromInt32(80),
				Scheme: "http",
				HTTPHeaders: []v1.HTTPHeader{
					{
						Name:  "Foo",
						Value: "bar",
					},
				},
			},
			Expected: expected{
				OldURL:    "http://233.252.0.1:80/foo",
				OldHeader: http.Header{},
				NewURL:    "http://233.252.0.1:80/foo",
				NewHeader: http.Header{
					"Accept":     {"*/*"},
					"Foo":        {"bar"},
					"User-Agent": {"kube-lifecycle/."},
				},
			},
		}, {
			Name:  "host header",
			PodIP: "233.252.0.1",
			HTTPGet: &v1.HTTPGetAction{
				Host:   "example.test",
				Path:   "foo",
				Port:   intstr.FromInt32(80),
				Scheme: "http",
				HTTPHeaders: []v1.HTTPHeader{
					{
						Name:  "Host",
						Value: "from.header",
					},
				},
			},
			Expected: expected{
				OldURL:    "http://example.test:80/foo",
				OldHeader: http.Header{},
				NewURL:    "http://example.test:80/foo",
				NewHeader: http.Header{
					"Accept":     {"*/*"},
					"User-Agent": {"kube-lifecycle/."},
					"Host":       {"from.header"},
				},
			},
		},
	}

	containerID := kubecontainer.ContainerID{Type: "test", ID: "abc1234"}
	containerName := "containerFoo"

	container := v1.Container{
		Name: containerName,
		Lifecycle: &v1.Lifecycle{
			PostStart: &v1.LifecycleHandler{},
		},
		Ports: []v1.ContainerPort{
			{
				Name:          "quux",
				ContainerPort: 8080,
			},
		},
	}

	pod := v1.Pod{}
	pod.ObjectMeta.Name = "podFoo"
	pod.ObjectMeta.Namespace = "nsFoo"
	pod.Spec.Containers = []v1.Container{container}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			_, tCtx := ktesting.NewTestContext(t)
			fakePodStatusProvider := stubPodStatusProvider(tt.PodIP)

			container.Lifecycle.PostStart.HTTPGet = tt.HTTPGet
			pod.Spec.Containers = []v1.Container{container}

			// verify 断言本次请求实际发出的 header 与 URL。
			verify := func(t *testing.T, expectedHeader http.Header, expectedURL string) {
				fakeHTTPDoer := fakeHTTP{}
				handlerRunner := NewHandlerRunner(&fakeHTTPDoer, &fakeContainerCommandRunner{}, fakePodStatusProvider, nil)

				_, err := handlerRunner.Run(tCtx, containerID, &pod, &container, container.Lifecycle.PostStart)
				if err != nil {
					t.Fatal(err)
				}

				if diff := cmp.Diff(expectedHeader, fakeHTTPDoer.headers); diff != "" {
					t.Errorf("unexpected header (-want, +got)\n:%s", diff)
				}
				if fakeHTTPDoer.url != expectedURL {
					t.Errorf("url = %v; want %v", fakeHTTPDoer.url, tt.Expected.NewURL)
				}
			}

			// 只验证 HttpGet 处理器在本用例下的行为。
			t.Run("consistent", func(t *testing.T) {
				verify(t, tt.Expected.NewHeader, tt.Expected.NewURL)
			})
		})
	}
}

// TestRunHandlerNil 验证未配置任何处理器的钩子会返回错误。
func TestRunHandlerNil(t *testing.T) {
	_, tCtx := ktesting.NewTestContext(t)
	handlerRunner := NewHandlerRunner(&fakeHTTP{}, &fakeContainerCommandRunner{}, nil, nil)
	containerID := kubecontainer.ContainerID{Type: "test", ID: "abc1234"}
	podName := "podFoo"
	podNamespace := "nsFoo"
	containerName := "containerFoo"

	container := v1.Container{
		Name: containerName,
		Lifecycle: &v1.Lifecycle{
			PostStart: &v1.LifecycleHandler{},
		},
	}
	pod := v1.Pod{}
	pod.ObjectMeta.Name = podName
	pod.ObjectMeta.Namespace = podNamespace
	pod.Spec.Containers = []v1.Container{container}
	_, err := handlerRunner.Run(tCtx, containerID, &pod, &container, container.Lifecycle.PostStart)
	if err == nil {
		t.Errorf("expect error, but got nil")
	}
}

// TestRunHandlerExecFailure 验证 exec 失败时错误信息包含命令、容器与 Pod 上下文。
func TestRunHandlerExecFailure(t *testing.T) {
	_, tCtx := ktesting.NewTestContext(t)
	expectedErr := fmt.Errorf("invalid command")
	fakeCommandRunner := fakeContainerCommandRunner{Err: expectedErr, Msg: expectedErr.Error()}
	handlerRunner := NewHandlerRunner(&fakeHTTP{}, &fakeCommandRunner, nil, nil)

	containerID := kubecontainer.ContainerID{Type: "test", ID: "abc1234"}
	containerName := "containerFoo"
	command := []string{"ls", "--a"}

	container := v1.Container{
		Name: containerName,
		Lifecycle: &v1.Lifecycle{
			PostStart: &v1.LifecycleHandler{
				Exec: &v1.ExecAction{
					Command: command,
				},
			},
		},
	}

	pod := v1.Pod{}
	pod.ObjectMeta.Name = "podFoo"
	pod.ObjectMeta.Namespace = "nsFoo"
	pod.Spec.Containers = []v1.Container{container}
	expectedErrMsg := fmt.Sprintf("Exec lifecycle hook (%s) for Container %q in Pod %q failed - error: %v, message: %q", command, containerName, format.Pod(&pod), expectedErr, expectedErr.Error())
	msg, err := handlerRunner.Run(tCtx, containerID, &pod, &container, container.Lifecycle.PostStart)
	if err == nil {
		t.Errorf("expected error: %v", expectedErr)
	}
	if msg != expectedErrMsg {
		t.Errorf("unexpected error message: %q; expected %q", msg, expectedErrMsg)
	}
}

// TestRunHandlerHttpFailure 验证 HTTP 失败时返回的错误消息包含探测路径与 Pod 信息。
func TestRunHandlerHttpFailure(t *testing.T) {
	_, tCtx := ktesting.NewTestContext(t)
	expectedErr := fmt.Errorf("fake http error")
	expectedResp := http.Response{
		Body: io.NopCloser(strings.NewReader(expectedErr.Error())),
	}
	fakeHTTPGetter := fakeHTTP{err: expectedErr, resp: &expectedResp}

	fakePodStatusProvider := stubPodStatusProvider("127.0.0.1")

	handlerRunner := NewHandlerRunner(&fakeHTTPGetter, &fakeContainerCommandRunner{}, fakePodStatusProvider, nil)

	containerName := "containerFoo"
	containerID := kubecontainer.ContainerID{Type: "test", ID: "abc1234"}
	container := v1.Container{
		Name: containerName,
		Lifecycle: &v1.Lifecycle{
			PostStart: &v1.LifecycleHandler{
				HTTPGet: &v1.HTTPGetAction{
					Host: "foo",
					Port: intstr.FromInt32(8080),
					Path: "bar",
				},
			},
		},
	}
	pod := v1.Pod{}
	pod.ObjectMeta.Name = "podFoo"
	pod.ObjectMeta.Namespace = "nsFoo"
	pod.Spec.Containers = []v1.Container{container}
	expectedErrMsg := fmt.Sprintf("HTTP lifecycle hook (%s) for Container %q in Pod %q failed - error: %v", "bar", containerName, format.Pod(&pod), expectedErr)
	msg, err := handlerRunner.Run(tCtx, containerID, &pod, &container, container.Lifecycle.PostStart)
	if err == nil {
		t.Errorf("expected error: %v", expectedErr)
	}
	if msg != expectedErrMsg {
		t.Errorf("unexpected error message: %q; expected %q", msg, expectedErrMsg)
	}
	if fakeHTTPGetter.url != "http://foo:8080/bar" {
		t.Errorf("unexpected url: %s", fakeHTTPGetter.url)
	}
}

// TestRunHandlerHttpsFailureFallback 验证 HTTPS 失败后回退到 HTTP，并记录指标与事件。
func TestRunHandlerHttpsFailureFallback(t *testing.T) {
	_, tCtx := ktesting.NewTestContext(t)

	// Prometheus 的 gatherer 是全局的，其他测试可能已更新过指标，
	// 因此运行本测试前需要先重置；这也意味着本测试不能与其他测试并行。
	metrics.Register()
	legacyregistry.Reset()

	var actualHeaders http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		actualHeaders = r.Header.Clone()
	}))
	defer srv.Close()
	_, port, err := net.SplitHostPort(srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}

	recorder := &record.FakeRecorder{Events: make(chan string, 10)}

	fakePodStatusProvider := stubPodStatusProvider("127.0.0.1")

	handlerRunner := NewHandlerRunner(srv.Client(), &fakeContainerCommandRunner{}, fakePodStatusProvider, recorder).(*handlerRunner)

	containerName := "containerFoo"
	containerID := kubecontainer.ContainerID{Type: "test", ID: "abc1234"}
	container := v1.Container{
		Name: containerName,
		Lifecycle: &v1.Lifecycle{
			PostStart: &v1.LifecycleHandler{
				HTTPGet: &v1.HTTPGetAction{
					// 置为 https 以验证失败后回退到 HTTP 的行为。
					Scheme: "https",
					Host:   "127.0.0.1",
					Port:   intstr.FromInt32(int32(portNumber)),
					Path:   "bar",
					HTTPHeaders: []v1.HTTPHeader{
						{
							Name:  "Authorization",
							Value: "secret",
						},
					},
				},
			},
		},
	}
	pod := v1.Pod{}
	pod.ObjectMeta.Name = "podFoo"
	pod.ObjectMeta.Namespace = "nsFoo"
	pod.Spec.Containers = []v1.Container{container}
	msg, err := handlerRunner.Run(tCtx, containerID, &pod, &container, container.Lifecycle.PostStart)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if msg != "" {
		t.Errorf("unexpected error message: %q", msg)
	}
	if actualHeaders.Get("Authorization") != "" {
		t.Error("unexpected Authorization header")
	}

	expectedMetrics := `
# HELP kubelet_lifecycle_handler_http_fallbacks_total [ALPHA] The number of times lifecycle handlers successfully fell back to http from https.
# TYPE kubelet_lifecycle_handler_http_fallbacks_total counter
kubelet_lifecycle_handler_http_fallbacks_total 1
`

	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expectedMetrics), "kubelet_lifecycle_handler_http_fallbacks_total"); err != nil {
		t.Fatal(err)
	}

	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, "LifecycleHTTPFallback") {
			t.Fatalf("expected LifecycleHTTPFallback event, got %q", event)
		}
	default:
		t.Fatal("no event recorded")
	}
}

// TestIsHTTPResponseError 验证 TLS 握手/响应类错误能被识别为 HTTP 响应错误。
func TestIsHTTPResponseError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	defer s.Close()
	req, err := http.NewRequest("GET", s.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.URL.Scheme = "https"
	_, err = http.DefaultClient.Do(req)
	if !isHTTPResponseError(err) {
		t.Errorf("unexpected http response error: %v", err)
	}
}

// TestRunSleepHandler 验证 sleep 钩子受 Pod 终止宽限期的约束。
func TestRunSleepHandler(t *testing.T) {
	tCtx := ktesting.Init(t)
	handlerRunner := NewHandlerRunner(&fakeHTTP{}, &fakeContainerCommandRunner{}, nil, nil)
	containerID := kubecontainer.ContainerID{Type: "test", ID: "abc1234"}
	containerName := "containerFoo"
	container := v1.Container{
		Name: containerName,
		Lifecycle: &v1.Lifecycle{
			PreStop: &v1.LifecycleHandler{},
		},
	}
	pod := v1.Pod{}
	pod.ObjectMeta.Name = "podFoo"
	pod.ObjectMeta.Namespace = "nsFoo"
	pod.Spec.Containers = []v1.Container{container}

	tests := []struct {
		name                          string
		sleepSeconds                  int64
		terminationGracePeriodSeconds int64
		expectErr                     bool
		expectedErr                   string
	}{
		{
			name:                          "valid seconds",
			sleepSeconds:                  5,
			terminationGracePeriodSeconds: 30,
		},
		{
			name:                          "longer than TerminationGracePeriodSeconds",
			sleepSeconds:                  3,
			terminationGracePeriodSeconds: 2,
			expectErr:                     true,
			expectedErr:                   "container terminated before sleep hook finished",
		},
	}

	for _, tt := range tests {
		tCtx.SyncTest(tt.name, func(tCtx ktesting.TContext) {
			t := tCtx.TB()
			pod.Spec.Containers[0].Lifecycle.PreStop.Sleep = &v1.SleepAction{Seconds: tt.sleepSeconds}
			ctx, cancel := context.WithTimeout(tCtx, time.Duration(tt.terminationGracePeriodSeconds)*time.Second)
			defer cancel()

			_, err := handlerRunner.Run(ctx, containerID, &pod, &container, container.Lifecycle.PreStop)

			if !tt.expectErr && err != nil {
				t.Errorf("unexpected success")
			}
			if tt.expectErr && err.Error() != tt.expectedErr {
				t.Errorf("%s: expected error want %s, got %s", tt.name, tt.expectedErr, err.Error())
			}
		})
	}
}

// TestDeclaredFeaturesAdmitHandler 验证声明式特性准入：节点未声明的特性会拒绝 Pod，已 GA 的特性则允许。
func TestDeclaredFeaturesAdmitHandler(t *testing.T) {
	tCtx := ktesting.Init(t)
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pod",
		},
	}
	// createMockFeature 构造带指定名称、推断行为与最高版本的假特性。
	createMockFeature := func(t *testing.T, name string, inferForSched bool, maxVersionStr string) *ndftesting.MockFeature {
		m := ndftesting.NewMockFeature(t)
		m.SetName(name)
		m.SetInferForScheduling(func(podInfo *ndf.PodInfo) bool { return inferForSched })
		if maxVersionStr != "" {
			m.SetMaxVersion(version.MustParseSemantic(maxVersionStr))
		} else {
			m.SetMaxVersion(nil)
		}
		return m
	}
	testCases := []struct {
		name                 string
		nodeDeclaredFeatures []string
		version              *version.Version
		registeredFeatures   []ndf.Feature
		expectedAdmit        bool
		expectedReason       string
		expectedMessage      string
	}{
		{
			name:                 "Admit: no requirements",
			nodeDeclaredFeatures: []string{"FeatureA"},
			version:              version.MustParseSemantic("1.30.0"),
			registeredFeatures: []ndf.Feature{
				createMockFeature(t, "FeatureA", true, ""),
			},
			expectedAdmit: true,
		},
		{
			name:                 "Admit: requirements met",
			nodeDeclaredFeatures: []string{"FeatureA"},
			version:              version.MustParseSemantic("1.30.0"),
			registeredFeatures: []ndf.Feature{
				createMockFeature(t, "FeatureA", true, ""),
			},
			expectedAdmit: true,
		},
		{
			name:                 "Reject: requirements not met",
			nodeDeclaredFeatures: []string{},
			version:              version.MustParseSemantic("1.30.0"),
			registeredFeatures: []ndf.Feature{
				createMockFeature(t, "FeatureA", true, ""),
			},
			expectedAdmit:   false,
			expectedReason:  PodFeatureUnsupported,
			expectedMessage: "Pod requires node features that are not available: FeatureA",
		},
		{
			name:                 "Admit without feature declared - feature generally available",
			nodeDeclaredFeatures: []string{},
			version:              version.MustParseSemantic("1.35.0"),
			registeredFeatures: []ndf.Feature{
				createMockFeature(t, "FeatureA", true, "1.34.0"),
			},
			expectedAdmit: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			framework := ndf.New(tc.registeredFeatures)
			fs := framework.MustMapSorted(tc.nodeDeclaredFeatures)
			handler := NewDeclaredFeaturesAdmitHandler(framework, fs, tc.version)
			attrs := &PodAdmitAttributes{Pod: pod}

			result := handler.Admit(tCtx, attrs)

			require.Equal(t, tc.expectedAdmit, result.Admit)
			if !result.Admit {
				require.Equal(t, tc.expectedReason, result.Reason)
				require.Contains(t, result.Message, tc.expectedMessage, "Expected message '%s' to contain '%s'", result.Message, tc.expectedMessage)
			}
		})
	}
}
