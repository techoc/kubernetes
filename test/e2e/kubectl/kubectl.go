/*
Copyright 2015 The Kubernetes Authors.

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

// OWNER = sig/cli

package kubectl

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	openapi_v2 "github.com/google/gnostic-models/openapiv2"

	"sigs.k8s.io/yaml"

	v1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	utilnettesting "k8s.io/apimachinery/pkg/util/net/testing"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/authentication/serviceaccount"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubectl/pkg/polymorphichelpers"
	"k8s.io/kubernetes/pkg/controller"
	commonutils "k8s.io/kubernetes/test/e2e/common"
	"k8s.io/kubernetes/test/e2e/framework"
	e2eauth "k8s.io/kubernetes/test/e2e/framework/auth"
	e2edebug "k8s.io/kubernetes/test/e2e/framework/debug"
	e2eendpointslice "k8s.io/kubernetes/test/e2e/framework/endpointslice"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	e2enetwork "k8s.io/kubernetes/test/e2e/framework/network"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	e2eservice "k8s.io/kubernetes/test/e2e/framework/service"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	e2etestfiles "k8s.io/kubernetes/test/e2e/framework/testfiles"
	"k8s.io/kubernetes/test/e2e/scheduling"
	testutils "k8s.io/kubernetes/test/utils"
	"k8s.io/kubernetes/test/utils/crd"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"
	uexec "k8s.io/utils/exec"
	"k8s.io/utils/ptr"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const (
	updateDemoSelector         = "name=update-demo"
	guestbookStartupTimeout    = 10 * time.Minute
	guestbookResponseTimeout   = 3 * time.Minute
	simplePodSelector          = "name=agnhost"
	simplePodName              = "agnhost"
	simplePodResourceName      = "pod/agnhost"
	agnhostDefaultOutput       = "itworks"
	simplePodPort              = 80
	pausePodSelector           = "name=pause"
	pausePodName               = "pause"
	busyboxPodSelector         = "app=busybox1"
	busyboxPodName             = "busybox1"
	kubeCtlManifestPath        = "test/e2e/testing-manifests/kubectl"
	agnhostControllerFilename  = "agnhost-primary-controller.json.in"
	agnhostServiceFilename     = "agnhost-primary-service.json"
	agnhostDeployment1Filename = "agnhost-deployment1.yaml.in"
	agnhostDeployment2Filename = "agnhost-deployment2.yaml.in"
	agnhostDeployment3Filename = "agnhost-deployment3.yaml.in"
	metaPattern                = `"kind":"%s","apiVersion":"%s/%s","metadata":{"name":"%s"}`
)

func unknownFieldMetadataJSON(gvk schema.GroupVersionKind, name string) string {
	return fmt.Sprintf(`"kind":"%s","apiVersion":"%s/%s","metadata":{"unknownMeta": "foo", "name":"%s"}`, gvk.Kind, gvk.Group, gvk.Version, name)
}

var (
	// 如果本套件仍因超时而 flake，应把它改成 framework.PodStartTimeout
	podRunningTimeoutArg = fmt.Sprintf("--pod-running-timeout=%s", framework.PodStartShortTimeout.String())
)

var proxyRegexp = regexp.MustCompile("Starting to serve on 127.0.0.1:([0-9]+)")

var schemaFoo = []byte(`description: Foo CRD for Testing
type: object
properties:
  spec:
    type: object
    description: Specification of Foo
    properties:
      bars:
        description: List of Bars and their specs.
        type: array
        items:
          type: object
          required:
          - name
          properties:
            name:
              description: Name of Bar.
              type: string
            age:
              description: Age of Bar.
              type: string
            bazs:
              description: List of Bazs.
              items:
                type: string
              type: array
  status:
    description: Status of Foo
    type: object
    properties:
      bars:
        description: List of Bars and their statuses.
        type: array
        items:
          type: object
          properties:
            name:
              description: Name of Bar.
              type: string
            available:
              description: Whether the Bar is installed.
              type: boolean
            quxType:
              description: Indicates to external qux type.
              pattern: in-tree|out-of-tree
              type: string`)

var schemaFooEmbedded = []byte(`description: Foo CRD with an embedded resource
type: object
properties:
  spec:
    type: object
    properties:
      template:
        type: object
        x-kubernetes-embedded-resource: true
        properties:
          metadata:
            type: object
            properties:
              name:
                type: string
          spec:
            type: object
  metadata:
    type: object
    properties:
      name:
        type: string`)

// 停止 filePath 中 ns 命名空间的所有内容，并检查给定命名空间中匹配 selectors 的内容是否都被正确停止。
// 知道 kubectl example files map。
func cleanupKubectlInputs(fileContents string, ns string, selectors ...string) {
	ginkgo.By("using delete to clean up resources")
	// 支持向后兼容：文件路径或原始 json —— 因为我们正在移除本测试对
	// 文件路径的依赖。
	e2ekubectl.RunKubectlOrDieInput(ns, fileContents, "delete", "--grace-period=0", "--force", "-f", "-")
	assertCleanup(ns, selectors...)
}

// assertCleanup 断言命名空间相对于 selectors 的清理已完成。
func assertCleanup(ns string, selectors ...string) {
	var e error
	verifyCleanupFunc := func() (bool, error) {
		e = nil
		for _, selector := range selectors {
			resources := e2ekubectl.RunKubectlOrDie(ns, "get", "rc,svc", "-l", selector, "--no-headers")
			if resources != "" {
				e = fmt.Errorf("Resources left running after stop:\n%s", resources)
				return false, nil
			}
			pods := e2ekubectl.RunKubectlOrDie(ns, "get", "pods", "-l", selector, "-o", "go-template={{ range .items }}{{ if not .metadata.deletionTimestamp }}{{ .metadata.name }}{{ \"\\n\" }}{{ end }}{{ end }}")
			if pods != "" {
				e = fmt.Errorf("Pods left unterminated after stop:\n%s", pods)
				return false, nil
			}
		}
		return true, nil
	}
	err := wait.PollImmediate(500*time.Millisecond, 1*time.Minute, verifyCleanupFunc)
	if err != nil {
		framework.Fail(e.Error())
	}
}

func readTestFileOrDie(file string) []byte {
	data, err := e2etestfiles.Read(path.Join(kubeCtlManifestPath, file))
	if err != nil {
		framework.Fail(err.Error(), 1)
	}
	return data
}

func runKubectlRetryOrDie(ns string, args ...string) string {
	var err error
	var output string
	for range 5 {
		output, err = e2ekubectl.RunKubectl(ns, args...)
		if err == nil || (!strings.Contains(err.Error(), genericregistry.OptimisticLockErrorMsg) && !strings.Contains(err.Error(), "Operation cannot be fulfilled")) {
			break
		}
		time.Sleep(time.Second)
	}
	// 重试结束后预期没有错误
	// 复制自 framework #ExecOrDie
	framework.Logf("stdout: %q", output)
	framework.ExpectNoError(err)
	return output
}

// 匹配 localhost / loopback 的跳过规则，参见
// https://pkg.go.dev/golang.org/x/net/http/httpproxy#Config.ProxyFunc
func hostIsLocal(host string) bool {
	if host == "localhost" {
		return true
	}
	nip, err := netip.ParseAddr(host)
	var ip net.IP
	if err == nil {
		ip = net.IP(nip.AsSlice())
		if ip.IsLoopback() {
			return true
		}
	}
	return false
}

func debugDiscovery() {
	home := os.Getenv("HOME")
	if len(home) == 0 {
		framework.Logf("no $HOME envvar set")
		return
	}

	cacheDir := filepath.Join(home, ".kube", "cache", "discovery")
	err := filepath.Walk(cacheDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// 只关注 $host_$port/v1/serverresources.json 文件
		subpath := strings.TrimPrefix(path, cacheDir+string(filepath.Separator))
		parts := filepath.SplitList(subpath)
		if len(parts) != 3 || parts[1] != "v1" || parts[2] != "serverresources.json" {
			return nil
		}
		framework.Logf("%s modified at %s (current time: %s)", path, info.ModTime(), time.Now())

		data, readError := os.ReadFile(path)
		if readError != nil {
			framework.Logf("%s error: %v", path, readError)
		} else {
			framework.Logf("%s content: %s", path, string(data))
		}
		return nil
	})
	framework.Logf("scanned %s for discovery docs: %v", home, err)
}

var _ = SIGDescribe("Kubectl client", func() {
	defer ginkgo.GinkgoRecover()
	f := framework.NewDefaultFramework("kubectl")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	// 可复用的集群状态函数，不会受 framework 懒初始化的不利影响。
	clusterState := func() *framework.ClusterVerification {
		return f.NewClusterVerification(
			f.Namespace,
			framework.PodStateVerification{
				Selectors:   map[string]string{"app": "agnhost"},
				ValidPhases: []v1.PodPhase{v1.PodRunning /*v1.PodPending*/},
			})
	}
	forEachPod := func(ctx context.Context, podFunc func(p v1.Pod)) {
		_ = clusterState().ForEach(ctx, podFunc)
	}
	var c clientset.Interface
	var ns string
	ginkgo.BeforeEach(func() {
		c = f.ClientSet
		ns = f.Namespace.Name
	})

	// 为本测试定制的 Wait / ForEach 包装。这些示例展示了在大型
	// 测试文件中包装 ClusterVerification 结构体以获得语法糖的惯用方式。
	//
	// 超时前若找不到至少 atLeast 个 Pod，则打印调试信息
	waitForOrFailWithDebug := func(ctx context.Context, atLeast int) {
		pods, err := clusterState().WaitFor(ctx, atLeast, framework.PodStartTimeout)
		if err != nil || len(pods) < atLeast {
			// TODO: 把这些调试信息统一集成进测试，以便需要时总能拿到
			e2edebug.DumpAllNamespaceInfo(ctx, f.ClientSet, ns)
			framework.Failf("Verified %d of %d pods , error: %v", len(pods), atLeast, err)
		}
	}

	ginkgo.Describe("Update Demo", func() {
		var nautilus string
		ginkgo.BeforeEach(func() {
			updateDemoRoot := "test/fixtures/doc-yaml/user-guide/update-demo"
			data, err := e2etestfiles.Read(filepath.Join(updateDemoRoot, "nautilus-rc.yaml.in"))
			if err != nil {
				framework.Fail(err.Error())
			}
			nautilus = commonutils.SubstituteImageName(string(data))
		})
		/*
			Release: v1.9
			Testname: Kubectl, replication controller
			Description: 创建 Pod 和带给定镜像的容器。配置 replication controller 运行 2 个副本。Pod 的运行实例数必须等于 replication controller 设置的副本数（2）。
		*/
		framework.ConformanceIt("should create and stop a replication controller", func(ctx context.Context) {
			defer cleanupKubectlInputs(nautilus, ns, updateDemoSelector)

			ginkgo.By("creating a replication controller")
			e2ekubectl.RunKubectlOrDieInput(ns, nautilus, "create", "-f", "-")
			validateController(ctx, c, imageutils.GetE2EImage(imageutils.Nautilus), 2, "update-demo", updateDemoSelector, getUDData("nautilus.jpg", ns), ns)
		})

		/*
			Release: v1.9
			Testname: Kubectl, scale replication controller
			Description: 创建 Pod 和带给定镜像的容器。配置 replication controller 运行 2 个副本。Pod 的运行实例数必须等于副本数（2）。把 replicaset 更新为 1，Pod 运行实例数必须为 1；再更新为 2，Pod 运行实例数必须为 2。
		*/
		framework.ConformanceIt("should scale a replication controller", func(ctx context.Context) {
			defer cleanupKubectlInputs(nautilus, ns, updateDemoSelector)
			nautilusImage := imageutils.GetE2EImage(imageutils.Nautilus)

			ginkgo.By("creating a replication controller")
			e2ekubectl.RunKubectlOrDieInput(ns, nautilus, "create", "-f", "-")
			validateController(ctx, c, nautilusImage, 2, "update-demo", updateDemoSelector, getUDData("nautilus.jpg", ns), ns)
			ginkgo.By("scaling down the replication controller")
			debugDiscovery()
			e2ekubectl.RunKubectlOrDie(ns, "scale", "rc", "update-demo-nautilus", "--replicas=1", "--timeout=5m")
			validateController(ctx, c, nautilusImage, 1, "update-demo", updateDemoSelector, getUDData("nautilus.jpg", ns), ns)
			ginkgo.By("scaling up the replication controller")
			debugDiscovery()
			e2ekubectl.RunKubectlOrDie(ns, "scale", "rc", "update-demo-nautilus", "--replicas=2", "--timeout=5m")
			validateController(ctx, c, nautilusImage, 2, "update-demo", updateDemoSelector, getUDData("nautilus.jpg", ns), ns)
		})
	})

	ginkgo.Describe("Guestbook application", func() {
		forEachGBFile := func(run func(s string)) {
			guestbookRoot := "test/e2e/testing-manifests/guestbook"
			for _, gbAppFile := range []string{
				"agnhost-replica-service.yaml",
				"agnhost-primary-service.yaml",
				"frontend-service.yaml",
				"frontend-deployment.yaml.in",
				"agnhost-primary-deployment.yaml.in",
				"agnhost-replica-deployment.yaml.in",
			} {
				data, err := e2etestfiles.Read(filepath.Join(guestbookRoot, gbAppFile))
				if err != nil {
					framework.Fail(err.Error())
				}
				contents := commonutils.SubstituteImageName(string(data))
				run(contents)
			}
		}

		/*
			Release: v1.9
			Testname: Kubectl, guestbook application
			Description: 创建 Guestbook 应用，包含一个 agnhost 主服务器、2 个 agnhost 副本、frontend 应用、frontend Service、agnhost 主 Service 和 agnhost 副本 Service。测试通过 frontend Service 向 guestbook 应用写入一条记录，该记录会存入后端 agnhost store。应用流程必须按预期工作，写入的数据必须能读取到。
		*/
		framework.ConformanceIt("should create and stop a working application", func(ctx context.Context) {
			defer forEachGBFile(func(contents string) {
				cleanupKubectlInputs(contents, ns)
			})
			ginkgo.By("creating all guestbook components")
			forEachGBFile(func(contents string) {
				framework.Logf("%s", contents)
				e2ekubectl.RunKubectlOrDieInput(ns, contents, "create", "-f", "-")
			})

			ginkgo.By("validating guestbook app")
			validateGuestbookApp(ctx, c, ns)
		})
	})

	ginkgo.Describe("Simple pod", func() {
		var podYaml string
		ginkgo.BeforeEach(func(ctx context.Context) {
			ginkgo.By(fmt.Sprintf("creating the pod from %v", podYaml))
			podYaml = commonutils.SubstituteImageName(string(readTestFileOrDie("pod-with-readiness-probe.yaml.in")))
			e2ekubectl.RunKubectlOrDieInput(ns, podYaml, "create", "-f", "-")
			framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, c, simplePodName, ns, framework.PodStartTimeout))
		})
		ginkgo.AfterEach(func() {
			cleanupKubectlInputs(podYaml, ns, simplePodSelector)
		})

		ginkgo.It("should support exec", func(ctx context.Context) {
			ginkgo.By("executing a command in the container")
			execOutput := e2ekubectl.RunKubectlOrDie(ns, "exec", podRunningTimeoutArg, simplePodName, "--", "echo", "running", "in", "container")
			if e, a := "running in container", strings.TrimSpace(execOutput); e != a {
				framework.Failf("Unexpected kubectl exec output. Wanted %q, got %q", e, a)
			}

			ginkgo.By("executing a very long command in the container")
			veryLongData := make([]rune, 20000)
			for i := range veryLongData {
				veryLongData[i] = 'a'
			}
			execOutput = e2ekubectl.RunKubectlOrDie(ns, "exec", podRunningTimeoutArg, simplePodName, "--", "echo", string(veryLongData))
			gomega.Expect(string(veryLongData)).To(gomega.Equal(strings.TrimSpace(execOutput)), "Unexpected kubectl exec output")

			ginkgo.By("executing a command in the container with noninteractive stdin")
			execOutput = e2ekubectl.NewKubectlCommand(ns, "exec", "-i", podRunningTimeoutArg, simplePodName, "--", "cat").
				WithStdinData("abcd1234").
				ExecOrDie(ns)
			if e, a := "abcd1234", execOutput; e != a {
				framework.Failf("Unexpected kubectl exec output. Wanted %q, got %q", e, a)
			}

			// 假装自己是交互式 shell 中的用户
			r, closer, err := newBlockingReader("echo hi\nexit\n")
			if err != nil {
				framework.Failf("Error creating blocking reader: %v", err)
			}
			// 注意这仅用于测试清理！
			defer closer.Close()

			ginkgo.By("executing a command in the container with pseudo-interactive stdin")
			execOutput = e2ekubectl.NewKubectlCommand(ns, "exec", "-i", podRunningTimeoutArg, simplePodName, "--", "sh").
				WithStdinReader(r).
				ExecOrDie(ns)
			if e, a := "hi", strings.TrimSpace(execOutput); e != a {
				framework.Failf("Unexpected kubectl exec output. Wanted %q, got %q", e, a)
			}
		})

		ginkgo.It("should support exec using resource/name", func(ctx context.Context) {
			ginkgo.By("executing a command in the container")
			execOutput := e2ekubectl.RunKubectlOrDie(ns, "exec", podRunningTimeoutArg, simplePodResourceName, "--", "echo", "running", "in", "container")
			if e, a := "running in container", strings.TrimSpace(execOutput); e != a {
				framework.Failf("Unexpected kubectl exec output. Wanted %q, got %q", e, a)
			}
		})

		ginkgo.It("should support exec through an HTTP proxy", func(ctx context.Context) {
			// testContextHost 是一个 KUBECONFIG URL
			testContextHost := getTestContextHost()

			// 检查 testContextHost 是否位于 localhost 并跳过测试
			// localhost 和 loopback IP 上代理环境变量总会被忽略
			// https://pkg.go.dev/golang.org/x/net/http/httpproxy#Config.ProxyFunc
			// TODO: 考虑是否能用其他方式测试本地集群的代理
			// https://github.com/kubernetes/kubectl/issues/1655#issuecomment-2829408755
			u, err := url.Parse(testContextHost)
			framework.ExpectNoError(err, "parsing test context host: %s", testContextHost)
			if hostIsLocal(u.Hostname()) {
				e2eskipper.Skipf("Test host %q is on localhost and would not be proxied by HTTP_PROXY, skipping test", testContextHost)
			}

			ginkgo.By("Starting http_proxy")
			var proxyLogs bytes.Buffer
			testSrv := httptest.NewServer(utilnettesting.NewHTTPProxyHandler(ginkgo.GinkgoTB(), func(req *http.Request) bool {
				fmt.Fprintf(&proxyLogs, "Accepting %s to %s\n", req.Method, req.Host)
				return true
			}))
			defer testSrv.Close()
			proxyAddr := testSrv.URL

			for _, proxyVar := range []string{"https_proxy", "HTTPS_PROXY"} {
				proxyLogs.Reset()
				ginkgo.By("Running kubectl via an HTTP proxy using " + proxyVar)
				output := e2ekubectl.NewKubectlCommand(ns, "exec", podRunningTimeoutArg, simplePodName, "--", "echo", "running", "in", "container").
					AppendEnv([]string{fmt.Sprintf("%s=%s", proxyVar, proxyAddr)}).
					ExecOrDie(ns)

				// 校验拿到了 exec server 捕获的正常输出
				expectedExecOutput := "running in container\n"
				if output != expectedExecOutput {
					framework.Failf("Unexpected kubectl exec output. Wanted %q, got  %q", expectedExecOutput, output)
				}

				// 校验 proxy server 日志中看到了连接
				expectedProxyLog := fmt.Sprintf("Accepting CONNECT to %s", strings.TrimSuffix(strings.TrimPrefix(testContextHost, "https://"), "/api"))

				proxyLog := proxyLogs.String()
				if !strings.Contains(proxyLog, expectedProxyLog) {
					framework.Failf("Missing expected log result on proxy server for %s. Expected: %q, got %q", proxyVar, expectedProxyLog, proxyLog)
				}
			}
		})

		// https://issues.k8s.io/128314
		f.It(f.WithSlow(), "should support exec idle connections", func(ctx context.Context) {
			ginkgo.By("executing a command in the container")

			execOutput := e2ekubectl.RunKubectlOrDie(ns, "exec", podRunningTimeoutArg, simplePodName, "--", "/bin/sh", "-c", "sleep 320 && echo running in container")
			if expected, got := "running in container", strings.TrimSpace(execOutput); expected != got {
				framework.Failf("Unexpected kubectl exec output. Wanted %q, got %q", expected, got)
			}
		})

		ginkgo.It("should support exec through kubectl proxy", func(ctx context.Context) {
			_ = getTestContextHost()

			ginkgo.By("Starting kubectl proxy")
			port, proxyCmd, err := startProxyServer(ns)
			framework.ExpectNoError(err)
			defer framework.TryKill(proxyCmd)

			//proxyLogs.Reset()
			host := fmt.Sprintf("--server=http://127.0.0.1:%d", port)
			ginkgo.By("Running kubectl via kubectl proxy using " + host)
			output := e2ekubectl.NewKubectlCommand(
				ns, host,
				"exec", podRunningTimeoutArg, simplePodName, "--", "echo", "running", "in", "container",
			).ExecOrDie(ns)

			// 校验拿到了 exec server 捕获的正常输出
			expectedExecOutput := "running in container\n"
			if output != expectedExecOutput {
				framework.Failf("Unexpected kubectl exec output. Wanted %q, got  %q", expectedExecOutput, output)
			}
		})

		ginkgo.Context("should return command exit codes", func() {
			ginkgo.It("execing into a container with a successful command", func(ctx context.Context) {
				_, err := e2ekubectl.NewKubectlCommand(ns, "exec", simplePodName, podRunningTimeoutArg, "--", "/bin/sh", "-c", "exit 0").Exec()
				framework.ExpectNoError(err)
			})

			ginkgo.It("execing into a container with a failing command", func(ctx context.Context) {
				_, err := e2ekubectl.NewKubectlCommand(ns, "exec", simplePodName, podRunningTimeoutArg, "--", "/bin/sh", "-c", "exit 42").Exec()
				ee, ok := err.(uexec.ExitError)
				if !ok {
					framework.Failf("Got unexpected error type, expected uexec.ExitError, got %T: %v", err, err)
				}
				gomega.Expect(ee.ExitStatus()).To(gomega.Equal(42))
			})

			ginkgo.It("should support port-forward", func(ctx context.Context) {
				ginkgo.By("forwarding the container port to a local port")
				cmd := runPortForward(ns, simplePodName, simplePodPort)
				defer cmd.Stop()

				ginkgo.By("curling local port output")
				localAddr := fmt.Sprintf("http://localhost:%d/echo?msg=%s", cmd.port, agnhostDefaultOutput)
				body, err := curl(localAddr)
				framework.Logf("got: %s", body)
				if err != nil {
					framework.Failf("Failed http.Get of forwarded port (%s): %v", localAddr, err)
				}
				if !strings.Contains(body, agnhostDefaultOutput) {
					framework.Failf("Container port output missing expected value. Wanted:'%s', got: %s", agnhostDefaultOutput, body)
				}
			})

			ginkgo.It("should handle in-cluster config", func(ctx context.Context) {
				// TODO: 想办法下载并复制合适的 kubectl 二进制，或者用多架构 kubectl 镜像
				// 目前这只能在 amd64 上运行
				e2eskipper.SkipUnlessNodeOSArchIs("amd64")

				ginkgo.By("adding rbac permissions")
				// 广泛授予 view 权限，便于检查 `invalid` 命名空间和 default 命名空间
				cleanupFunc, err := e2eauth.BindClusterRole(ctx, f.ClientSet.RbacV1(), "view", f.Namespace.Name,
					rbacv1.Subject{Kind: rbacv1.ServiceAccountKind, Namespace: f.Namespace.Name, Name: "default"})
				framework.ExpectNoError(err)
				defer cleanupFunc(ctx)

				err = e2eauth.WaitForAuthorizationUpdate(ctx, f.ClientSet.AuthorizationV1(),
					serviceaccount.MakeUsername(f.Namespace.Name, "default"),
					f.Namespace.Name, "list", schema.GroupResource{Resource: "pods"}, true)
				framework.ExpectNoError(err)

				ginkgo.By("overriding icc with values provided by flags")
				kubectlPath := framework.TestContext.KubectlPath
				// 需要真正的 kubectl 二进制，而不是 script wrapper
				kubectlPathNormalizer := exec.Command("which", kubectlPath)
				if strings.HasSuffix(kubectlPath, "kubectl.sh") {
					kubectlPathNormalizer = exec.Command(kubectlPath, "path")
				}
				kubectlPathNormalized, err := kubectlPathNormalizer.Output()
				framework.ExpectNoError(err)
				kubectlPath = strings.TrimSpace(string(kubectlPathNormalized))

				inClusterHost := strings.TrimSpace(e2eoutput.RunHostCmdOrDie(ns, simplePodName, "printenv KUBERNETES_SERVICE_HOST"))
				inClusterPort := strings.TrimSpace(e2eoutput.RunHostCmdOrDie(ns, simplePodName, "printenv KUBERNETES_SERVICE_PORT"))
				inClusterURL := net.JoinHostPort(inClusterHost, inClusterPort)
				framework.Logf("copying %s to the %s pod", kubectlPath, simplePodName)
				e2ekubectl.RunKubectlOrDie(ns, "cp", kubectlPath, ns+"/"+simplePodName+":/tmp/")

				// 构建一个 kubeconfig，使用注入的 ca 和 token，
				// 但指向 DNS host 和 default 命名空间
				tmpDir, err := os.MkdirTemp("", "icc-override")
				overrideKubeconfigName := "icc-override.kubeconfig"
				framework.ExpectNoError(err)
				defer func() { os.Remove(tmpDir) }()
				framework.ExpectNoError(os.WriteFile(filepath.Join(tmpDir, overrideKubeconfigName), []byte(`
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
`), os.FileMode(0755)))
				framework.Logf("copying override kubeconfig to the %s pod", simplePodName)
				e2ekubectl.RunKubectlOrDie(ns, "cp", filepath.Join(tmpDir, overrideKubeconfigName), ns+"/"+simplePodName+":/tmp/")

				framework.ExpectNoError(os.WriteFile(filepath.Join(tmpDir, "invalid-configmap-with-namespace.yaml"), []byte(`
kind: ConfigMap
apiVersion: v1
metadata:
  name: "configmap with namespace and invalid name"
  namespace: configmap-namespace
`), os.FileMode(0755)))
				framework.ExpectNoError(os.WriteFile(filepath.Join(tmpDir, "invalid-configmap-without-namespace.yaml"), []byte(`
kind: ConfigMap
apiVersion: v1
metadata:
  name: "configmap without namespace and invalid name"
`), os.FileMode(0755)))
				framework.Logf("copying configmap manifests to the %s pod", simplePodName)
				e2ekubectl.RunKubectlOrDie(ns, "cp", filepath.Join(tmpDir, "invalid-configmap-with-namespace.yaml"), ns+"/"+simplePodName+":/tmp/")
				e2ekubectl.RunKubectlOrDie(ns, "cp", filepath.Join(tmpDir, "invalid-configmap-without-namespace.yaml"), ns+"/"+simplePodName+":/tmp/")

				ginkgo.By("getting pods with in-cluster configs")
				execOutput := e2eoutput.RunHostCmdOrDie(ns, simplePodName, "/tmp/kubectl get pods --v=6 2>&1")
				gomega.Expect(execOutput).To(gomega.MatchRegexp("agnhost +1/1 +Running"))
				gomega.Expect(execOutput).To(gomega.ContainSubstring("Using in-cluster namespace"))
				gomega.Expect(execOutput).To(gomega.ContainSubstring("Using in-cluster configuration"))

				ginkgo.By("creating an object containing a namespace with in-cluster config")
				_, err = e2eoutput.RunHostCmd(ns, simplePodName, "/tmp/kubectl create -f /tmp/invalid-configmap-with-namespace.yaml --v=6 2>&1")
				gomega.Expect(err).To(gomega.ContainSubstring("Using in-cluster namespace"))
				gomega.Expect(err).To(gomega.ContainSubstring("Using in-cluster configuration"))

				gomega.Expect(err).To(gomega.ContainSubstring(fmt.Sprintf(`verb="POST" url="https://%s/api/v1/namespaces/configmap-namespace/configmaps`, inClusterURL)))

				ginkgo.By("creating an object not containing a namespace with in-cluster config")
				_, err = e2eoutput.RunHostCmd(ns, simplePodName, "/tmp/kubectl create -f /tmp/invalid-configmap-without-namespace.yaml --v=6 2>&1")
				gomega.Expect(err).To(gomega.ContainSubstring("Using in-cluster namespace"))
				gomega.Expect(err).To(gomega.ContainSubstring("Using in-cluster configuration"))
				gomega.Expect(err).To(gomega.ContainSubstring(fmt.Sprintf(`verb="POST" url="https://%s/api/v1/namespaces/%s/configmaps`, inClusterURL, f.Namespace.Name)))

				ginkgo.By("trying to use kubectl with invalid token")
				_, err = e2eoutput.RunHostCmd(ns, simplePodName, "/tmp/kubectl get pods --token=invalid --v=7 2>&1")
				framework.Logf("got err %v", err)
				gomega.Expect(err).To(gomega.HaveOccurred())
				gomega.Expect(err).To(gomega.ContainSubstring("Using in-cluster namespace"))
				gomega.Expect(err).To(gomega.ContainSubstring("Using in-cluster configuration"))
				gomega.Expect(err).To(gomega.ContainSubstring(`"Response" status="401 Unauthorized"`))

				ginkgo.By("trying to use kubectl with invalid server")
				_, err = e2eoutput.RunHostCmd(ns, simplePodName, "/tmp/kubectl get pods --server=invalid --v=6 2>&1")
				framework.Logf("got err %v", err)
				gomega.Expect(err).To(gomega.HaveOccurred())
				gomega.Expect(err).To(gomega.ContainSubstring("Unable to connect to the server"))
				gomega.Expect(err).To(gomega.ContainSubstring(`verb="GET" url="http://invalid/api`))

				ginkgo.By("trying to use kubectl with invalid namespace")
				execOutput = e2eoutput.RunHostCmdOrDie(ns, simplePodName, "/tmp/kubectl get pods --namespace=invalid --v=6 2>&1")
				gomega.Expect(execOutput).To(gomega.ContainSubstring("No resources found"))
				gomega.Expect(execOutput).ToNot(gomega.ContainSubstring("Using in-cluster namespace"))
				gomega.Expect(execOutput).To(gomega.ContainSubstring("Using in-cluster configuration"))
				gomega.Expect(execOutput).To(gomega.MatchRegexp(fmt.Sprintf(`verb="GET" url="http[s]?://[\[]?%s[\]]?:%s/api/v1/namespaces/invalid/pods`, inClusterHost, inClusterPort)))

				ginkgo.By("trying to use kubectl with kubeconfig")
				execOutput = e2eoutput.RunHostCmdOrDie(ns, simplePodName, "/tmp/kubectl get pods --kubeconfig=/tmp/"+overrideKubeconfigName+" --v=6 2>&1")
				gomega.Expect(execOutput).ToNot(gomega.ContainSubstring("Using in-cluster namespace"))
				gomega.Expect(execOutput).ToNot(gomega.ContainSubstring("Using in-cluster configuration"))
				gomega.Expect(execOutput).To(gomega.ContainSubstring(`verb="GET" url="https://kubernetes.default.svc:443/api/v1/namespaces/default/pods`))
			})
		})

		ginkgo.Describe("Kubectl run", func() {
			ginkgo.It("running a successful command", func(ctx context.Context) {
				_, err := e2ekubectl.NewKubectlCommand(ns, "run", "-i", "--image="+imageutils.GetE2EImage(imageutils.BusyBox), "--restart=Never", podRunningTimeoutArg, "success", "--", "/bin/sh", "-c", "exit 0").Exec()
				framework.ExpectNoError(err)
			})

			ginkgo.It("running a failing command", func(ctx context.Context) {
				_, err := e2ekubectl.NewKubectlCommand(ns, "run", "-i", "--image="+imageutils.GetE2EImage(imageutils.BusyBox), "--restart=Never", podRunningTimeoutArg, "failure-1", "--", "/bin/sh", "-c", "exit 42").Exec()
				ee, ok := err.(uexec.ExitError)
				if !ok {
					framework.Failf("Got unexpected error type, expected uexec.ExitError, got %T: %v", err, err)
				}
				gomega.Expect(ee.ExitStatus()).To(gomega.Equal(42))
			})

			f.It(f.WithSlow(), "running a failing command without --restart=Never", func(ctx context.Context) {
				_, err := e2ekubectl.NewKubectlCommand(ns, "run", "-i", "--image="+imageutils.GetE2EImage(imageutils.BusyBox), "--restart=OnFailure", podRunningTimeoutArg, "failure-2", "--", "/bin/sh", "-c", "cat && exit 42").
					WithStdinData("abcd1234").
					Exec()
				ee, ok := err.(uexec.ExitError)
				if !ok {
					framework.Failf("Got unexpected error type, expected uexec.ExitError, got %T: %v", err, err)
				}
				if !strings.Contains(ee.String(), "timed out") {
					framework.Failf("Missing expected 'timed out' error, got: %#v", ee)
				}
			})

			f.It(f.WithSlow(), "running a failing command without --restart=Never, but with --rm", func(ctx context.Context) {
				_, err := e2ekubectl.NewKubectlCommand(ns, "run", "-i", "--image="+imageutils.GetE2EImage(imageutils.BusyBox), "--restart=OnFailure", "--rm", podRunningTimeoutArg, "failure-3", "--", "/bin/sh", "-c", "cat && exit 42").
					WithStdinData("abcd1234").
					Exec()
				ee, ok := err.(uexec.ExitError)
				if !ok {
					framework.Failf("Got unexpected error type, expected uexec.ExitError, got %T: %v", err, err)
				}
				if !strings.Contains(ee.String(), "timed out") {
					framework.Failf("Missing expected 'timed out' error, got: %#v", ee)
				}
				framework.ExpectNoError(e2epod.WaitForPodNotFoundInNamespace(ctx, f.ClientSet, "failure-3", ns, 2*v1.DefaultTerminationGracePeriodSeconds*time.Second))
			})

			f.It(f.WithSlow(), "running a failing command with --leave-stdin-open", func(ctx context.Context) {
				_, err := e2ekubectl.NewKubectlCommand(ns, "run", "-i", "--image="+imageutils.GetE2EImage(imageutils.BusyBox), "--restart=Never", podRunningTimeoutArg, "failure-4", "--leave-stdin-open", "--", "/bin/sh", "-c", "exit 42").
					WithStdinData("abcd1234").
					Exec()
				framework.ExpectNoError(err)
			})
		})

		ginkgo.It("should support inline execution and attach", func(ctx context.Context) {
			waitForStdinContent := func(pod, content string) string {
				var logOutput string
				err := wait.Poll(10*time.Second, 5*time.Minute, func() (bool, error) {
					logOutput = e2ekubectl.RunKubectlOrDie(ns, "logs", pod)
					return strings.Contains(logOutput, content), nil
				})

				framework.ExpectNoError(err, "waiting for '%v' output", content)
				return logOutput
			}

			ginkgo.By("executing a command with run and attach with stdin")
			// 等待非空行，以此确认 kubectl 已 attach
			e2ekubectl.NewKubectlCommand(ns, "run", "run-test", "--image="+imageutils.GetE2EImage(imageutils.BusyBox), "--restart=OnFailure", podRunningTimeoutArg, "--attach=true", "--stdin", "--", "sh", "-c", "echo -n read: && cat && echo 'stdin closed'").
				WithStdinData("value\nabcd1234").
				ExecOrDie(ns)

			runOutput := waitForStdinContent("run-test", "stdin closed")
			gomega.Expect(runOutput).To(gomega.ContainSubstring("read:value"))
			gomega.Expect(runOutput).To(gomega.ContainSubstring("abcd1234"))
			gomega.Expect(runOutput).To(gomega.ContainSubstring("stdin closed"))

			framework.ExpectNoError(c.CoreV1().Pods(ns).Delete(ctx, "run-test", metav1.DeleteOptions{}))

			ginkgo.By("executing a command with run and attach without stdin")
			// #73099 描述了这个场景中的竞态
			// 如果没能在容器打印
			// "stdin closed" 之前 attach 上，测试就会失败，但容器尚未退出。
			// 打印前等待 10 秒，给 kubectl 留出 attach
			// 到容器的时间，但这并不能消除竞态。
			e2ekubectl.NewKubectlCommand(ns, "run", "run-test-2", "--image="+imageutils.GetE2EImage(imageutils.BusyBox), "--restart=OnFailure", podRunningTimeoutArg, "--attach=true", "--leave-stdin-open=true", "--", "sh", "-c", "cat && echo 'stdin closed'").
				WithStdinData("abcd1234").
				ExecOrDie(ns)

			runOutput = waitForStdinContent("run-test-2", "stdin closed")
			gomega.Expect(runOutput).ToNot(gomega.ContainSubstring("abcd1234"))
			gomega.Expect(runOutput).To(gomega.ContainSubstring("stdin closed"))

			framework.ExpectNoError(c.CoreV1().Pods(ns).Delete(ctx, "run-test-2", metav1.DeleteOptions{}))

			ginkgo.By("executing a command with run and attach with stdin with open stdin should remain running")
			e2ekubectl.NewKubectlCommand(ns, "run", "run-test-3", "--image="+imageutils.GetE2EImage(imageutils.BusyBox), "--restart=OnFailure", podRunningTimeoutArg, "--attach=true", "--leave-stdin-open=true", "--stdin", "--", "sh", "-c", "cat && echo 'stdin closed'").
				WithStdinData("abcd1234\n").
				ExecOrDie(ns)

			runOutput = waitForStdinContent("run-test-3", "abcd1234")
			gomega.Expect(runOutput).To(gomega.ContainSubstring("abcd1234"))
			gomega.Expect(runOutput).ToNot(gomega.ContainSubstring("stdin closed"))

			g := func(pods []*v1.Pod) sort.Interface { return sort.Reverse(controller.ActivePods(pods)) }
			runTestPod, _, err := polymorphichelpers.GetFirstPod(f.ClientSet.CoreV1(), ns, "run=run-test-3", 1*time.Minute, g)
			framework.ExpectNoError(err)
			framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, c, runTestPod.Name, ns, time.Minute))

			framework.ExpectNoError(c.CoreV1().Pods(ns).Delete(ctx, "run-test-3", metav1.DeleteOptions{}))
		})

		ginkgo.It("should support inline execution and attach with websockets or fallback to spdy", func(ctx context.Context) {
			waitForStdinContent := func(pod, content string) string {
				var logOutput string
				err := wait.PollUntilContextTimeout(ctx, 10*time.Second, 5*time.Minute, false, func(ctx context.Context) (bool, error) {
					logOutput = e2ekubectl.RunKubectlOrDie(ns, "logs", pod)
					return strings.Contains(logOutput, content), nil
				})
				framework.ExpectNoError(err, "waiting for '%v' output", content)
				return logOutput
			}

			ginkgo.By("executing a command with run and attach with stdin")
			// 等待非空行，以此确认 kubectl 已 attach
			e2ekubectl.NewKubectlCommand(ns, "run", "run-test", "--image="+imageutils.GetE2EImage(imageutils.BusyBox), "--restart=OnFailure", podRunningTimeoutArg, "--attach=true", "--stdin", "--", "sh", "-c", "echo -n read: && cat && echo 'stdin closed'").
				WithStdinData("value\nabcd1234").
				ExecOrDie(ns)

			runOutput := waitForStdinContent("run-test", "stdin closed")
			gomega.Expect(runOutput).To(gomega.ContainSubstring("read:value"))
			gomega.Expect(runOutput).To(gomega.ContainSubstring("abcd1234"))
			gomega.Expect(runOutput).To(gomega.ContainSubstring("stdin closed"))

			framework.ExpectNoError(c.CoreV1().Pods(ns).Delete(ctx, "run-test", metav1.DeleteOptions{}))

			ginkgo.By("executing a command with run and attach without stdin")
			// #73099 描述了这个场景中的竞态
			// 如果没能在容器打印
			// "stdin closed" 之前 attach 上，测试就会失败，但容器尚未退出。
			// 打印前等待 10 秒，给 kubectl 留出 attach
			// 到容器的时间，但这并不能消除竞态。
			e2ekubectl.NewKubectlCommand(ns, "run", "run-test-2", "--image="+imageutils.GetE2EImage(imageutils.BusyBox), "--restart=OnFailure", podRunningTimeoutArg, "--attach=true", "--leave-stdin-open=true", "--", "sh", "-c", "cat && echo 'stdin closed'").
				WithStdinData("abcd1234").
				ExecOrDie(ns)

			runOutput = waitForStdinContent("run-test-2", "stdin closed")
			gomega.Expect(runOutput).ToNot(gomega.ContainSubstring("abcd1234"))
			gomega.Expect(runOutput).To(gomega.ContainSubstring("stdin closed"))

			framework.ExpectNoError(c.CoreV1().Pods(ns).Delete(ctx, "run-test-2", metav1.DeleteOptions{}))

			ginkgo.By("executing a command with run and attach with stdin with open stdin should remain running")
			e2ekubectl.NewKubectlCommand(ns, "run", "run-test-3", "--image="+imageutils.GetE2EImage(imageutils.BusyBox), "--restart=OnFailure", podRunningTimeoutArg, "--attach=true", "--leave-stdin-open=true", "--stdin", "--", "sh", "-c", "cat && echo 'stdin closed'").
				WithStdinData("abcd1234\n").
				ExecOrDie(ns)

			runOutput = waitForStdinContent("run-test-3", "abcd1234")
			gomega.Expect(runOutput).To(gomega.ContainSubstring("abcd1234"))
			gomega.Expect(runOutput).ToNot(gomega.ContainSubstring("stdin closed"))

			g := func(pods []*v1.Pod) sort.Interface { return sort.Reverse(controller.ActivePods(pods)) }
			runTestPod, _, err := polymorphichelpers.GetFirstPod(f.ClientSet.CoreV1(), ns, "run=run-test-3", 1*time.Minute, g)
			framework.ExpectNoError(err)
			framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, c, runTestPod.Name, ns, time.Minute))

			framework.ExpectNoError(c.CoreV1().Pods(ns).Delete(ctx, "run-test-3", metav1.DeleteOptions{}))
		})

		ginkgo.It("should contain last line of the log", func(ctx context.Context) {
			podName := "run-log-test"

			ginkgo.By("executing a command with run")
			e2ekubectl.RunKubectlOrDie(ns, "run", podName, "--image="+imageutils.GetE2EImage(imageutils.BusyBox), "--restart=OnFailure", podRunningTimeoutArg, "--", "sh", "-c", "sleep 10; for i in {1..100}; do echo $i; sleep 0.01; done; echo EOF")

			if !e2epod.CheckPodsRunningReadyOrSucceeded(ctx, c, ns, []string{podName}, framework.PodStartTimeout) {
				framework.Failf("Pod for run-log-test was not ready")
			}

			logOutput := e2ekubectl.RunKubectlOrDie(ns, "logs", "-f", "run-log-test")
			gomega.Expect(logOutput).To(gomega.ContainSubstring("EOF"))
		})
	})

	ginkgo.Describe("Kubectl api-versions", func() {
		/*
			Release: v1.9
			Testname: Kubectl, check version v1
			Description: 运行 kubectl 获取 api versions，输出必须包含返回的 versions 且列出 'v1'。
		*/
		framework.ConformanceIt("should check if v1 is in available api versions", func(ctx context.Context) {
			ginkgo.By("validating api versions")
			output := e2ekubectl.RunKubectlOrDie(ns, "api-versions")
			if !strings.Contains(output, "v1") {
				framework.Failf("No v1 in kubectl api-versions")
			}
		})
	})

	ginkgo.Describe("Kubectl get componentstatuses", func() {
		ginkgo.It("should get componentstatuses", func(ctx context.Context) {
			ginkgo.By("getting list of componentstatuses")
			output := e2ekubectl.RunKubectlOrDie(ns, "get", "componentstatuses", "-o", "jsonpath={.items[*].metadata.name}")
			components := strings.Split(output, " ")
			ginkgo.By("getting details of componentstatuses")
			for _, component := range components {
				ginkgo.By("getting status of " + component)
				e2ekubectl.RunKubectlOrDie(ns, "get", "componentstatuses", component)
			}
		})
	})

	ginkgo.Describe("Kubectl diff", func() {
		/*
			Release: v1.19
			Testname: Kubectl, diff Deployment
			Description: 用 agnhost 镜像创建 Deployment。再用不同镜像 busybox 声明同一个 Deployment。线上 Deployment 与声明的 Deployment 的 diff 必须包含两者镜像的差异。
		*/
		framework.ConformanceIt("should check if kubectl diff finds a difference for Deployments", func(ctx context.Context) {
			ginkgo.By("create deployment with agnhost image")
			deployment := commonutils.SubstituteImageName(string(readTestFileOrDie(agnhostDeployment3Filename)))
			e2ekubectl.RunKubectlOrDieInput(ns, deployment, "create", "-f", "-")

			ginkgo.By("verify diff finds difference between live and declared image")
			deployment = strings.Replace(deployment, imageutils.GetE2EImage(imageutils.AgnhostPrev), imageutils.GetE2EImage(imageutils.BusyBox), 1)
			if !strings.Contains(deployment, imageutils.GetE2EImage(imageutils.BusyBox)) {
				framework.Failf("Failed replacing image from %s to %s in:\n%s\n", imageutils.GetE2EImage(imageutils.AgnhostPrev), imageutils.GetE2EImage(imageutils.BusyBox), deployment)
			}
			output, err := e2ekubectl.RunKubectlInput(ns, deployment, "diff", "-f", "-")
			if err, ok := err.(*exec.ExitError); ok && err.ExitCode() == 1 {
				framework.Failf("Expected kubectl diff exit code of 1, but got %d: %v\n", err.ExitCode(), err)
			}
			requiredItems := []string{imageutils.GetE2EImage(imageutils.AgnhostPrev), imageutils.GetE2EImage(imageutils.BusyBox)}
			for _, item := range requiredItems {
				if !strings.Contains(output, item) {
					framework.Failf("Missing %s in kubectl diff output:\n%s\n%v\n", item, output, err)
				}
			}

			e2ekubectl.RunKubectlOrDieInput(ns, deployment, "delete", "-f", "-")
		})
	})

	ginkgo.Describe("Kubectl server-side dry-run", func() {
		/*
			Release: v1.19
			Testname: Kubectl, server-side dry-run Pod
			Description: 命令 'kubectl run' 必须创建带指定镜像名的 Pod。随后命令 'kubectl patch pod -p {...} --dry-run=server' 应启用 server-side dry-run 并用新镜像名更新 Pod。镜像名不得改变。
		*/
		framework.ConformanceIt("should check if kubectl can dry-run update Pods", func(ctx context.Context) {
			agnhostImage := imageutils.GetE2EImage(imageutils.Agnhost)
			ginkgo.By("running the image " + agnhostImage)
			podName := "e2e-test-agnhost-pod"
			e2ekubectl.RunKubectlOrDie(ns, "run", podName, "--image="+agnhostImage, podRunningTimeoutArg, "--labels=run="+podName)

			ginkgo.By("replace the image in the pod with server-side dry-run")
			specImage := fmt.Sprintf(`{"spec":{"containers":[{"name": "%s","image": "%s"}]}}`, podName, imageutils.GetE2EImage(imageutils.BusyBox))
			e2ekubectl.RunKubectlOrDie(ns, "patch", "pod", podName, "-p", specImage, "--dry-run=server")

			ginkgo.By("verifying the pod " + podName + " has the right image " + agnhostImage)
			pod, err := c.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{})
			if err != nil {
				framework.Failf("Failed getting pod %s: %v", podName, err)
			}
			containers := pod.Spec.Containers
			if checkContainersImage(containers, agnhostImage) {
				framework.Failf("Failed creating pod with expected image %s", agnhostImage)
			}

			e2ekubectl.RunKubectlOrDie(ns, "delete", "pods", podName)
		})
	})

	// 若指定 GVK 被列为 x-kubernetes-group-version-kind 扩展，definitionMatchesGVK 返回 true
	definitionMatchesGVK := func(extensions []*openapi_v2.NamedAny, desiredGVK schema.GroupVersionKind) bool {
		for _, extension := range extensions {
			if extension.GetValue().GetYaml() == "" ||
				extension.GetName() != "x-kubernetes-group-version-kind" {
				continue
			}
			var values []map[string]string
			err := yaml.Unmarshal([]byte(extension.GetValue().GetYaml()), &values)
			if err != nil {
				framework.Logf("%v\n%s", err, string(extension.GetValue().GetYaml()))
				continue
			}
			for _, value := range values {
				if value["group"] != desiredGVK.Group {
					continue
				}
				if value["version"] != desiredGVK.Version {
					continue
				}
				if value["kind"] != desiredGVK.Kind {
					continue
				}
				return true
			}
		}
		return false
	}

	// schemaForGVK 返回指定 GVK 的 schema（如果有定义）
	schemaForGVK := func(desiredGVK schema.GroupVersionKind) *openapi_v2.Schema {
		d, err := f.ClientSet.Discovery().OpenAPISchema()
		if err != nil {
			framework.Failf("%v", err)
		}
		if d == nil || d.Definitions == nil {
			return nil
		}
		for _, p := range d.Definitions.AdditionalProperties {
			if p == nil || p.Value == nil {
				continue
			}
			if !definitionMatchesGVK(p.Value.VendorExtension, desiredGVK) {
				continue
			}
			return p.Value
		}
		return nil
	}

	ginkgo.Describe("Kubectl validation", func() {
		ginkgo.It("should create/apply a CR with unknown fields for CRD with no validation schema", func(ctx context.Context) {
			ginkgo.By("create CRD with no validation schema")
			crd, err := crd.CreateTestCRD(f)
			if err != nil {
				framework.Failf("failed to create test CRD: %v", err)
			}
			ginkgo.DeferCleanup(crd.CleanUp)

			ginkgo.By("sleep for 10s to wait for potential crd openapi publishing alpha feature")
			time.Sleep(10 * time.Second)

			meta := fmt.Sprintf(metaPattern, crd.Crd.Spec.Names.Kind, crd.Crd.Spec.Group, crd.Crd.Spec.Versions[0].Name, "test-cr")
			randomCR := fmt.Sprintf(`{%s,"a":{"b":[{"c":"d"}]}}`, meta)
			if err := createApplyCustomResource(randomCR, f.Namespace.Name, "test-cr", crd); err != nil {
				framework.Failf("%v", err)
			}
		})

		ginkgo.It("should create/apply a valid CR for CRD with validation schema", func(ctx context.Context) {
			ginkgo.By("prepare CRD with validation schema")
			crd, err := crd.CreateTestCRD(f, func(crd *apiextensionsv1.CustomResourceDefinition) {
				props := &apiextensionsv1.JSONSchemaProps{}
				if err := yaml.Unmarshal(schemaFoo, props); err != nil {
					framework.Failf("failed to unmarshal schema: %v", err)
				}
				for i := range crd.Spec.Versions {
					crd.Spec.Versions[i].Schema = &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: props}
				}
			})
			if err != nil {
				framework.Failf("failed to create test CRD: %v", err)
			}
			ginkgo.DeferCleanup(crd.CleanUp)

			ginkgo.By("sleep for 10s to wait for potential crd openapi publishing alpha feature")
			time.Sleep(10 * time.Second)

			meta := fmt.Sprintf(metaPattern, crd.Crd.Spec.Names.Kind, crd.Crd.Spec.Group, crd.Crd.Spec.Versions[0].Name, "test-cr")
			validCR := fmt.Sprintf(`{%s,"spec":{"bars":[{"name":"test-bar"}]}}`, meta)
			if err := createApplyCustomResource(validCR, f.Namespace.Name, "test-cr", crd); err != nil {
				framework.Failf("%v", err)
			}
		})

		ginkgo.It("should create/apply an invalid/valid CR with arbitrary-extra properties for CRD with partially-specified validation schema", func(ctx context.Context) {
			ginkgo.By("prepare CRD with partially-specified validation schema")
			crd, err := crd.CreateTestCRD(f, func(crd *apiextensionsv1.CustomResourceDefinition) {
				props := &apiextensionsv1.JSONSchemaProps{}
				if err := yaml.Unmarshal(schemaFoo, props); err != nil {
					framework.Failf("failed to unmarshal schema: %v", err)
				}
				// 允许额外的任意属性。
				props.XPreserveUnknownFields = ptr.To(true)
				for i := range crd.Spec.Versions {
					crd.Spec.Versions[i].Schema = &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: props}
				}
			})
			if err != nil {
				framework.Failf("failed to create test CRD: %v", err)
			}
			ginkgo.DeferCleanup(crd.CleanUp)

			ginkgo.By("sleep for 10s to wait for potential crd openapi publishing alpha feature")
			time.Sleep(10 * time.Second)

			schema := schemaForGVK(schema.GroupVersionKind{Group: crd.Crd.Spec.Group, Version: crd.Crd.Spec.Versions[0].Name, Kind: crd.Crd.Spec.Names.Kind})
			gomega.Expect(schema).ToNot(gomega.BeNil(), "retrieving a schema for the crd")

			meta := fmt.Sprintf(metaPattern, crd.Crd.Spec.Names.Kind, crd.Crd.Spec.Group, crd.Crd.Spec.Versions[0].Name, "test-cr")

			// XPreserveUnknownFields 定义在 schema 根节点，因此 spec 中的未知字段
			// 仍会被视为无效
			invalidArbitraryCR := fmt.Sprintf(`{%s,"spec":{"bars":[{"name":"test-bar"}],"extraProperty":"arbitrary-value"}}`, meta)
			err = createApplyCustomResource(invalidArbitraryCR, f.Namespace.Name, "test-cr", crd)
			gomega.Expect(err).To(gomega.HaveOccurred(), "creating custom resource")

			if !strings.Contains(err.Error(), `unknown field "spec.extraProperty"`) {
				framework.Failf("incorrect error from createApplyCustomResource: %v", err)
			}

			// 根节点上的未知字段被视为有效
			validArbitraryCR := fmt.Sprintf(`{%s,"spec":{"bars":[{"name":"test-bar"}]},"extraProperty":"arbitrary-value"}`, meta)
			err = createApplyCustomResource(validArbitraryCR, f.Namespace.Name, "test-cr", crd)
			framework.ExpectNoError(err, "creating custom resource")
		})

		ginkgo.It("should detect unknown metadata fields in both the root and embedded object of a CR", func(ctx context.Context) {
			ginkgo.By("prepare CRD with x-kubernetes-embedded-resource: true")
			testCRD, err := crd.CreateTestCRD(f, func(crd *apiextensionsv1.CustomResourceDefinition) {
				props := &apiextensionsv1.JSONSchemaProps{}
				if err := yaml.Unmarshal(schemaFooEmbedded, props); err != nil {
					framework.Failf("failed to unmarshal schema: %v", err)
				}
				crd.Spec.Versions = []apiextensionsv1.CustomResourceDefinitionVersion{
					{
						Name:    "v1",
						Served:  true,
						Storage: true,
						Schema: &apiextensionsv1.CustomResourceValidation{
							OpenAPIV3Schema: props,
						},
					},
				}
			})
			if err != nil {
				framework.Failf("failed to create test CRD: %v", err)
			}
			ginkgo.DeferCleanup(testCRD.CleanUp)

			ginkgo.By("sleep for 10s to wait for potential crd openapi publishing alpha feature")
			time.Sleep(10 * time.Second)

			ginkgo.By("attempting to create a CR with unknown metadata fields at the root level")
			gvk := schema.GroupVersionKind{Group: testCRD.Crd.Spec.Group, Version: testCRD.Crd.Spec.Versions[0].Name, Kind: testCRD.Crd.Spec.Names.Kind}
			schema := schemaForGVK(gvk)
			gomega.Expect(schema).ToNot(gomega.BeNil(), "retrieving a schema for the crd")
			embeddedCRPattern := `

{%s,
  "spec": {
    "template": {
      "apiVersion": "foo/v1",
      "kind": "Sub",
      "metadata": {
        %s
        "name": "subobject",
        "namespace": "%s"
      }
    }
  }
}`
			meta := unknownFieldMetadataJSON(gvk, "test-cr")
			unknownRootMetaCR := fmt.Sprintf(embeddedCRPattern, meta, "", ns)
			_, err = e2ekubectl.RunKubectlInput(ns, unknownRootMetaCR, "create", "--validate=true", "-f", "-")
			if err == nil {
				framework.Failf("unexpected nil error when creating CR with unknown root metadata field")
			}
			if !(strings.Contains(err.Error(), `unknown field "unknownMeta"`) || strings.Contains(err.Error(), `unknown field "metadata.unknownMeta"`)) {
				framework.Failf("error missing root unknown metadata field, got: %v", err)
			}
			if strings.Contains(err.Error(), `unknown field "namespace"`) || strings.Contains(err.Error(), `unknown field "metadata.namespace"`) {
				framework.Failf("unexpected error, CR's root metadata namespace field unrecognized: %v", err)
			}

			ginkgo.By("attempting to create a CR with unknown metadata fields in the embedded object")
			metaEmbedded := fmt.Sprintf(metaPattern, testCRD.Crd.Spec.Names.Kind, testCRD.Crd.Spec.Group, testCRD.Crd.Spec.Versions[0].Name, "test-cr-embedded")
			unknownEmbeddedMetaCR := fmt.Sprintf(embeddedCRPattern, metaEmbedded, `"unknownMetaEmbedded": "bar",`, ns)
			_, err = e2ekubectl.RunKubectlInput(ns, unknownEmbeddedMetaCR, "create", "--validate=true", "-f", "-")
			if err == nil {
				framework.Failf("unexpected nil error when creating CR with unknown embedded metadata field")
			}
			if !(strings.Contains(err.Error(), `unknown field "unknownMetaEmbedded"`) || strings.Contains(err.Error(), `unknown field "spec.template.metadata.unknownMetaEmbedded"`)) {
				framework.Failf("error missing embedded unknown metadata field, got: %v", err)
			}
			if strings.Contains(err.Error(), `unknown field "namespace"`) || strings.Contains(err.Error(), `unknown field "spec.template.metadata.namespace"`) {
				framework.Failf("unexpected error, CR's embedded metadata namespace field unrecognized: %v", err)
			}
		})

		ginkgo.It("should detect unknown metadata fields of a typed object", func(ctx context.Context) {
			ginkgo.By("calling kubectl create deployment")
			invalidMetaDeployment := `
	{
		"apiVersion": "apps/v1",
		"kind": "Deployment",
		"metadata": {
			"name": "my-dep",
			"unknownMeta": "foo",
			"labels": {"app": "nginx"}
		},
		"spec": {
			"selector": {
				"matchLabels": {
					"app": "nginx"
				}
			},
			"template": {
				"metadata": {
					"labels": {
						"app": "nginx"
					}
				},
				"spec": {
					"containers": [{
						"name":  "nginx",
						"image": "nginx:latest"
					}]
				}
			}
		}
	}
		`
			_, err := e2ekubectl.RunKubectlInput(ns, invalidMetaDeployment, "create", "-f", "-")
			if err == nil {
				framework.Failf("unexpected nil error when creating deployment with unknown metadata field")
			}
			if !(strings.Contains(err.Error(), `unknown field "unknownMeta"`) || strings.Contains(err.Error(), `unknown field "metadata.unknownMeta"`)) {
				framework.Failf("error missing unknown metadata field, got: %v", err)
			}
			if strings.Contains(err.Error(), `unknown field "namespace"`) || strings.Contains(err.Error(), `unknown field "metadata.namespace"`) {
				framework.Failf("unexpected error, deployment's metadata namespace field unrecognized: %v", err)
			}

		})
	})

	ginkgo.Describe("Kubectl cluster-info", func() {
		/*
				Release: v1.9
				Testname: Kubectl, cluster info
			Description: 调用 kubectl 获取 cluster-info，输出必须包含返回的 cluster-info，Kubernetes control plane 应处于运行状态。
		*/
		framework.ConformanceIt("should check if Kubernetes control plane services is included in cluster-info", func(ctx context.Context) {
			ginkgo.By("validating cluster-info")
			output := e2ekubectl.RunKubectlOrDie(ns, "cluster-info")
			// 由于终端控制命令（颜色），无法检查精确字符串
			requiredItems := []string{"Kubernetes control plane", "is running at"}
			for _, item := range requiredItems {
				if !strings.Contains(output, item) {
					framework.Failf("Missing %s in kubectl cluster-info", item)
				}
			}
		})
	})

	ginkgo.Describe("Kubectl cluster-info dump", func() {
		ginkgo.It("should check if cluster-info dump succeeds", func(ctx context.Context) {
			ginkgo.By("running cluster-info dump")
			e2ekubectl.RunKubectlOrDie(ns, "cluster-info", "dump")
		})
	})

	ginkgo.Describe("Kubectl describe", func() {
		/*
				Release: v1.9
				Testname: Kubectl, describe pod or rc
			Description: 部署一个 agnhost controller 和一个 agnhost Service。Kubectl describe pods 应按预期返回名称、命名空间、label、状态等信息。Kubectl describe rc、service、node 和 namespace 也应返回正确信息。
		*/
		framework.ConformanceIt("should check if kubectl describe prints relevant information for rc and pods", func(ctx context.Context) {
			controllerJSON := commonutils.SubstituteImageName(string(readTestFileOrDie(agnhostControllerFilename)))
			serviceJSON := readTestFileOrDie(agnhostServiceFilename)

			e2ekubectl.RunKubectlOrDieInput(ns, controllerJSON, "create", "-f", "-")
			e2ekubectl.RunKubectlOrDieInput(ns, string(serviceJSON[:]), "create", "-f", "-")

			ginkgo.By("Waiting for Agnhost primary to start.")
			waitForOrFailWithDebug(ctx, 1)

			// Pod
			forEachPod(ctx, func(pod v1.Pod) {
				output := e2ekubectl.RunKubectlOrDie(ns, "describe", "pod", pod.Name)
				requiredStrings := [][]string{
					{"Name:", "agnhost-primary-"},
					{"Namespace:", ns},
					{"Node:"},
					{"Labels:", "app=agnhost"},
					{"role=primary"},
					{"Annotations:"},
					{"Status:", "Running"},
					{"IP:"},
					{"Controlled By:", "ReplicationController/agnhost-primary"},
					{"Image:", imageutils.GetE2EImage(imageutils.Agnhost)},
					{"State:", "Running"},
					{"QoS Class:", "BestEffort"},
				}
				checkOutput(output, requiredStrings)
			})

			// Rc
			requiredStrings := [][]string{
				{"Name:", "agnhost-primary"},
				{"Namespace:", ns},
				{"Selector:", "app=agnhost,role=primary"},
				{"Labels:", "app=agnhost"},
				{"role=primary"},
				{"Annotations:"},
				{"Replicas:", "1 current", "1 desired"},
				{"Pods Status:", "1 Running", "0 Waiting", "0 Succeeded", "0 Failed"},
				{"Pod Template:"},
				{"Image:", imageutils.GetE2EImage(imageutils.Agnhost)},
				{"Events:"}}
			checkKubectlOutputWithRetry(ns, requiredStrings, "describe", "rc", "agnhost-primary")

			// Service
			output := e2ekubectl.RunKubectlOrDie(ns, "describe", "service", "agnhost-primary")
			requiredStrings = [][]string{
				{"Name:", "agnhost-primary"},
				{"Namespace:", ns},
				{"Labels:", "app=agnhost"},
				{"role=primary"},
				{"Annotations:"},
				{"Selector:", "app=agnhost", "role=primary"},
				{"Type:", "ClusterIP"},
				{"IP:"},
				{"Port:", "<unset>", "6379/TCP"},
				{"Endpoints:"},
				{"Session Affinity:", "None"}}
			checkOutput(output, requiredStrings)

			// Node
			// 过滤掉不可调度的节点，避免误选其他并行测试创建的临时 fake 节点
			//（例如 test/e2e/node/node_lifecycle.go）。
			nodes, err := c.CoreV1().Nodes().List(ctx, metav1.ListOptions{
				FieldSelector: "spec.unschedulable=false",
			})
			framework.ExpectNoError(err)
			node := nodes.Items[0]
			output = e2ekubectl.RunKubectlOrDie(ns, "describe", "node", node.Name)
			requiredStrings = [][]string{
				{"Name:", node.Name},
				{"Labels:"},
				{"Annotations:"},
				{"CreationTimestamp:"},
				{"Conditions:"},
				{"Type", "Status", "LastHeartbeatTime", "LastTransitionTime", "Reason", "Message"},
				{"Addresses:"},
				{"Capacity:"},
				{"Version:"},
				{"Kernel Version:"},
				{"OS Image:"},
				{"Container Runtime Version:"},
				{"Kubelet Version:"},
				{"Pods:"}}
			checkOutput(output, requiredStrings)

			// Namespace
			output = e2ekubectl.RunKubectlOrDie(ns, "describe", "namespace", ns)
			requiredStrings = [][]string{
				{"Name:", ns},
				{"Labels:"},
				{"Annotations:"},
				{"Status:", "Active"}}
			checkOutput(output, requiredStrings)

			// Quota 和 limitrange 暂时跳过。
		})

		ginkgo.It("should check if kubectl describe prints relevant information for cronjob", func(ctx context.Context) {
			ginkgo.By("creating a cronjob")
			cronjobYaml := commonutils.SubstituteImageName(string(readTestFileOrDie("busybox-cronjob.yaml.in")))
			e2ekubectl.RunKubectlOrDieInput(ns, cronjobYaml, "create", "-f", "-")

			ginkgo.By("waiting for cronjob to start.")
			err := wait.PollImmediate(time.Second, time.Minute, func() (bool, error) {
				cj, err := c.BatchV1().CronJobs(ns).List(ctx, metav1.ListOptions{})
				if err != nil {
					return false, fmt.Errorf("Failed getting CronJob %s: %w", ns, err)
				}
				return len(cj.Items) > 0, nil
			})
			framework.ExpectNoError(err)

			ginkgo.By("verifying kubectl describe prints")
			output := e2ekubectl.RunKubectlOrDie(ns, "describe", "cronjob", "cronjob-test")
			requiredStrings := [][]string{
				{"Name:", "cronjob-test"},
				{"Namespace:", ns},
				{"Labels:"},
				{"Annotations:"},
				{"Schedule:", "*/1 * * * *"},
				{"Concurrency Policy:", "Allow"},
				{"Suspend:", "False"},
				{"Successful Job History Limit:", "3"},
				{"Failed Job History Limit:", "1"},
				{"Starting Deadline Seconds:", "30s"},
				{"Selector:"},
				{"Parallelism:"},
				{"Completions:"},
			}
			checkOutput(output, requiredStrings)
		})
	})

	ginkgo.Describe("Kubectl expose", func() {
		/*
				Release: v1.9
				Testname: Kubectl, create service, replication controller
			Description: 创建一个运行 agnhost 并监听 6379 端口的 Pod。用 kubectl expose 在 1234 端口暴露 agnhost 主 replication controller。校验 replication controller 监听 1234 端口且 target port 设为 6379（agnhost 主服务监听的端口）。再用 kubectl expose 在 2345 端口把 agnhost 主服务暴露为 Service。Service 必须监听 2345 端口且 target port 设为 6379。
		*/
		framework.ConformanceIt("should create services for rc", func(ctx context.Context) {
			controllerJSON := commonutils.SubstituteImageName(string(readTestFileOrDie(agnhostControllerFilename)))

			agnhostPort := 6379

			ginkgo.By("creating Agnhost RC")

			framework.Logf("namespace %v", ns)
			e2ekubectl.RunKubectlOrDieInput(ns, controllerJSON, "create", "-f", "-")

			// 某些情况下 Pod 注册需要时间，等待确认。
			ginkgo.By("Waiting for Agnhost primary to start.")
			waitForOrFailWithDebug(ctx, 1)
			forEachPod(ctx, func(pod v1.Pod) {
				framework.Logf("wait on agnhost-primary startup in %v ", ns)
				e2eoutput.LookForStringInLog(ns, pod.Name, "agnhost-primary", "Paused", framework.PodStartTimeout)
			})
			validateService := func(name string, servicePort int, timeout time.Duration) {
				err := wait.Poll(framework.Poll, timeout, func() (bool, error) {
					slices, err := c.DiscoveryV1().EndpointSlices(ns).List(ctx, metav1.ListOptions{LabelSelector: fmt.Sprintf("%s=%s", discoveryv1.LabelServiceName, name)})
					if err != nil {
						// 记录真实的错误
						framework.Logf("List endpointslices failed (interval %v): %v", framework.Poll, err)

						// 如果错误是 API not found、找不到默认凭据或 TLS 握手超时，重试
						if apierrors.IsNotFound(err) ||
							apierrors.IsUnauthorized(err) ||
							apierrors.IsServerTimeout(err) {
							err = nil
						}
						return false, err
					}

					uidToPort := e2eendpointslice.GetContainerPortsByPodUID(slices.Items)
					if len(uidToPort) == 0 {
						framework.Logf("No endpoint found, retrying")
						return false, nil
					}
					if len(uidToPort) > 1 {
						framework.Failf("Too many endpoints found")
					}
					for _, port := range uidToPort {
						if port[0] != agnhostPort {
							framework.Failf("Wrong endpoint port: %d", port[0])
						}
					}
					return true, nil
				})
				framework.ExpectNoError(err)

				e2eservice, err := c.CoreV1().Services(ns).Get(ctx, name, metav1.GetOptions{})
				framework.ExpectNoError(err)

				if len(e2eservice.Spec.Ports) != 1 {
					framework.Failf("1 port is expected")
				}
				port := e2eservice.Spec.Ports[0]
				if port.Port != int32(servicePort) {
					framework.Failf("Wrong service port: %d", port.Port)
				}
				if port.TargetPort.IntValue() != agnhostPort {
					framework.Failf("Wrong target port: %d", port.TargetPort.IntValue())
				}
			}

			ginkgo.By("exposing RC")
			e2ekubectl.RunKubectlOrDie(ns, "expose", "rc", "agnhost-primary", "--name=rm2", "--port=1234", fmt.Sprintf("--target-port=%d", agnhostPort))
			framework.ExpectNoError(e2enetwork.WaitForService(ctx, c, ns, "rm2", true, framework.Poll, framework.ServiceStartTimeout))
			validateService("rm2", 1234, framework.ServiceStartTimeout)

			ginkgo.By("exposing service")
			e2ekubectl.RunKubectlOrDie(ns, "expose", "service", "rm2", "--name=rm3", "--port=2345", fmt.Sprintf("--target-port=%d", agnhostPort))
			framework.ExpectNoError(e2enetwork.WaitForService(ctx, c, ns, "rm3", true, framework.Poll, framework.ServiceStartTimeout))
			validateService("rm3", 2345, framework.ServiceStartTimeout)
		})
	})

	ginkgo.Describe("Kubectl label", func() {
		var podYaml string
		ginkgo.BeforeEach(func(ctx context.Context) {
			ginkgo.By("creating the pod")
			podYaml = commonutils.SubstituteImageName(string(readTestFileOrDie("pause-pod.yaml.in")))
			e2ekubectl.RunKubectlOrDieInput(ns, podYaml, "create", "-f", "-")
			framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, c, pausePodName, ns, framework.PodStartTimeout))
		})
		ginkgo.AfterEach(func() {
			cleanupKubectlInputs(podYaml, ns, pausePodSelector)
		})

		/*
				Release: v1.9
				Testname: Kubectl, label update
			Description: Pod 运行时用 'kubectl label' 命令更新 Label。Pod 上必须创建该 label。带 -l 选项的 'kubectl get pod' 必须能读回该 label。用 'kubectl label label-' 移除 label。带 -l 选项的 'kubectl get pod' 不应再列出已删除的 label。
		*/
		framework.ConformanceIt("should update the label on a resource", func(ctx context.Context) {
			labelName := "testing-label"
			labelValue := "testing-label-value"

			ginkgo.By("adding the label " + labelName + " with value " + labelValue + " to a pod")
			e2ekubectl.RunKubectlOrDie(ns, "label", "pods", pausePodName, labelName+"="+labelValue)
			ginkgo.By("verifying the pod has the label " + labelName + " with the value " + labelValue)
			output := e2ekubectl.RunKubectlOrDie(ns, "get", "pod", pausePodName, "-L", labelName)
			if !strings.Contains(output, labelValue) {
				framework.Fail("Failed updating label " + labelName + " to the pod " + pausePodName)
			}

			ginkgo.By("removing the label " + labelName + " of a pod")
			e2ekubectl.RunKubectlOrDie(ns, "label", "pods", pausePodName, labelName+"-")
			ginkgo.By("verifying the pod doesn't have the label " + labelName)
			output = e2ekubectl.RunKubectlOrDie(ns, "get", "pod", pausePodName, "-L", labelName)
			if strings.Contains(output, labelValue) {
				framework.Fail("Failed removing label " + labelName + " of the pod " + pausePodName)
			}
		})
	})

	ginkgo.Describe("Kubectl copy", func() {
		var podYaml string
		ginkgo.BeforeEach(func(ctx context.Context) {
			ginkgo.By("creating the pod")
			podYaml = commonutils.SubstituteImageName(string(readTestFileOrDie("busybox-pod.yaml.in")))
			e2ekubectl.RunKubectlOrDieInput(ns, podYaml, "create", "-f", "-")
			framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, c, busyboxPodName, ns, framework.PodStartTimeout))
		})
		ginkgo.AfterEach(func() {
			cleanupKubectlInputs(podYaml, ns, busyboxPodSelector)
		})

		/*
				Release: v1.12
				Testname: Kubectl, copy
			Description: Pod 运行时，从中复制一个已知文件到本地临时目标。
		*/
		ginkgo.It("should copy a file from a running Pod", func(ctx context.Context) {
			remoteContents := "foobar\n"
			podSource := fmt.Sprintf("%s:/root/foo/bar/foo.bar", busyboxPodName)
			tempDestination, err := os.CreateTemp(os.TempDir(), "copy-foobar")
			if err != nil {
				framework.Failf("Failed creating temporary destination file: %v", err)
			}

			ginkgo.By("specifying a remote filepath " + podSource + " on the pod")
			e2ekubectl.RunKubectlOrDie(ns, "cp", podSource, tempDestination.Name())
			ginkgo.By("verifying that the contents of the remote file " + podSource + " have been copied to a local file " + tempDestination.Name())
			localData, err := io.ReadAll(tempDestination)
			if err != nil {
				framework.Failf("Failed reading temporary local file: %v", err)
			}
			if string(localData) != remoteContents {
				framework.Failf("Failed copying remote file contents. Expected %s but got %s", remoteContents, string(localData))
			}
		})
	})

	ginkgo.Describe("Kubectl patch", func() {
		/*
				Release: v1.9
				Testname: Kubectl, patch to annotate
			Description: 启动 agnhost 和一个 replication controller。Pod 运行后，用 'kubectl patch' 命令添加 annotation。annotation 必须加到运行中的 Pod 上，且应能从 replication controller 下的每个 Pod 读回。
		*/
		framework.ConformanceIt("should add annotations for pods in rc", func(ctx context.Context) {
			controllerJSON := commonutils.SubstituteImageName(string(readTestFileOrDie(agnhostControllerFilename)))
			ginkgo.By("creating Agnhost RC")
			e2ekubectl.RunKubectlOrDieInput(ns, controllerJSON, "create", "-f", "-")
			ginkgo.By("Waiting for Agnhost primary to start.")
			waitForOrFailWithDebug(ctx, 1)
			ginkgo.By("patching all pods")
			forEachPod(ctx, func(pod v1.Pod) {
				e2ekubectl.RunKubectlOrDie(ns, "patch", "pod", pod.Name, "-p", "{\"metadata\":{\"annotations\":{\"x\":\"y\"}}}")
			})

			ginkgo.By("checking annotations")
			forEachPod(ctx, func(pod v1.Pod) {
				found := false
				for key, val := range pod.Annotations {
					if key == "x" && val == "y" {
						found = true
						break
					}
				}
				if !found {
					framework.Failf("Added annotation not found")
				}
			})
		})
	})

	ginkgo.Describe("Kubectl version", func() {
		/*
				Release: v1.9
				Testname: Kubectl, version
			Description: 命令 'kubectl version' 必须返回 kubectl 所连接客户端和 server 的 major、minor 版本、GitCommit 等信息。
		*/
		framework.ConformanceIt("should check is all data is printed", func(ctx context.Context) {
			versionString := e2ekubectl.RunKubectlOrDie(ns, "version")
			// 预期以下值：Major -> 数字，Minor -> 数字后跟可选 '+'，GitCommit -> 字母数字
			requiredItems := []string{"Client Version: ", "Server Version: "}
			for _, item := range requiredItems {
				// 1.28 之前打印的是长版本信息
				oldMatched, _ := regexp.MatchString(item+`version.Info\{Major:"\d", Minor:"\d+\+?", GitVersion:"v\d\.\d+\.[\d\w\-\.\+]+", GitCommit:"[0-9a-f]+"`, versionString)
				// 1.28+ 打印短信息
				newMatched, _ := regexp.MatchString(item+`v\d\.\d+\.[\d\w\-\.\+]+`, versionString)
				// 为向后兼容，很可能到 1.30 之前都要同时匹配两种
				if !oldMatched && !newMatched {
					framework.Failf("Item %s value is not valid in %s\n", item, versionString)
				}
			}
		})
	})

	ginkgo.Describe("Kubectl run pod", func() {
		var podName string

		ginkgo.BeforeEach(func() {
			podName = "e2e-test-agnhost-pod"
		})

		ginkgo.AfterEach(func() {
			e2ekubectl.RunKubectlOrDie(ns, "delete", "pods", podName)
		})

		/*
				Release: v1.9
				Testname: Kubectl, run pod
			Description: 当 run 命令指定了镜像名时，命令 'kubectl run' 必须创建 Pod。运行命令后应存在一个 Pod，其中有一个容器运行指定镜像。
		*/
		framework.ConformanceIt("should create a pod from an image when restart is Never", func(ctx context.Context) {
			agnhostImage := imageutils.GetE2EImage(imageutils.Agnhost)
			ginkgo.By("running the image " + agnhostImage)
			e2ekubectl.RunKubectlOrDie(ns, "run", podName, "--restart=Never", podRunningTimeoutArg, "--image="+agnhostImage)
			ginkgo.By("verifying the pod " + podName + " was created")
			pod, err := c.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{})
			if err != nil {
				framework.Failf("Failed getting pod %s: %v", podName, err)
			}
			containers := pod.Spec.Containers
			if checkContainersImage(containers, agnhostImage) {
				framework.Failf("Failed creating pod %s with expected image %s", podName, agnhostImage)
			}
			if pod.Spec.RestartPolicy != v1.RestartPolicyNever {
				framework.Failf("Failed creating a pod with correct restart policy for --restart=Never")
			}
		})
	})

	ginkgo.Describe("Kubectl replace", func() {
		var podName string

		ginkgo.BeforeEach(func() {
			podName = "e2e-test-agnhost-pod"
		})

		ginkgo.AfterEach(func() {
			e2ekubectl.RunKubectlOrDie(ns, "delete", "pods", podName)
		})

		/*
				Release: v1.9
				Testname: Kubectl, replace
			Description: 对已有 Pod 用新 spec 执行 'kubectl replace' 必须更新 Pod 中容器的镜像。'kubectl replace' 的 -f 选项应强制重新创建资源。新 Pod 中的容器应带有镜像变更。
		*/
		framework.ConformanceIt("should update a single-container pod's image", func(ctx context.Context) {
			agnhostImage := imageutils.GetE2EImage(imageutils.Agnhost)
			ginkgo.By("running the image " + agnhostImage)
			e2ekubectl.RunKubectlOrDie(ns, "run", podName, "--image="+agnhostImage, podRunningTimeoutArg, "--labels=run="+podName)

			ginkgo.By("verifying the pod " + podName + " is running")
			label := labels.SelectorFromSet(labels.Set(map[string]string{"run": podName}))
			err := testutils.WaitForPodsWithLabelRunning(c, ns, label)
			if err != nil {
				framework.Failf("Failed getting pod %s: %v", podName, err)
			}

			ginkgo.By("verifying the pod " + podName + " was created")
			podJSON := e2ekubectl.RunKubectlOrDie(ns, "get", "pod", podName, "-o", "json")
			if !strings.Contains(podJSON, podName) {
				framework.Failf("Failed to find pod %s in [%s]", podName, podJSON)
			}

			ginkgo.By("replace the image in the pod")
			busyboxImage := imageutils.GetE2EImage(imageutils.BusyBox)
			podJSON = strings.Replace(podJSON, agnhostImage, busyboxImage, 1)
			e2ekubectl.RunKubectlOrDieInput(ns, podJSON, "replace", "-f", "-")

			ginkgo.By("verifying the pod " + podName + " has the right image " + busyboxImage)
			pod, err := c.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{})
			if err != nil {
				framework.Failf("Failed getting deployment %s: %v", podName, err)
			}
			containers := pod.Spec.Containers
			if checkContainersImage(containers, busyboxImage) {
				framework.Failf("Failed creating pod with expected image %s", busyboxImage)
			}
		})
	})

	ginkgo.Describe("Proxy server", func() {
		// TODO: 测试 proxy 选项（static、prefix 等）
		/*
				Release: v1.9
				Testname: Kubectl, proxy port zero
			Description: 用 --port=0 运行 'kubectl proxy' 在 0 端口启动 proxy server。通过 unix socket 请求 api versions 调用该 proxy server。proxy server 必须至少提供一个版本字符串。
		*/
		framework.ConformanceIt("should support proxy with --port 0", func(ctx context.Context) {
			ginkgo.By("starting the proxy server")
			port, cmd, err := startProxyServer(ns)
			if cmd != nil {
				defer framework.TryKill(cmd)
			}
			if err != nil {
				framework.Failf("Failed to start proxy server: %v", err)
			}
			ginkgo.By("curling proxy /api/ output")
			localAddr := fmt.Sprintf("http://localhost:%d/api/", port)
			apiVersions, err := getAPIVersions(localAddr)
			if err != nil {
				framework.Failf("Expected at least one supported apiversion, got error %v", err)
			}
			if len(apiVersions.Versions) < 1 {
				framework.Failf("Expected at least one supported apiversion, got %v", apiVersions)
			}
		})

		/*
				Release: v1.9
				Testname: Kubectl, proxy socket
			Description: 用 --unix-socket=<some path> 运行 'kubectl proxy' 启动 proxy server。通过请求 http://locahost:0/api 调用该 proxy server。proxy server 必须至少提供一个版本字符串。
		*/
		framework.ConformanceIt("should support --unix-socket=/path", func(ctx context.Context) {
			ginkgo.By("Starting the proxy")
			tmpdir, err := os.MkdirTemp("", "kubectl-proxy-unix")
			if err != nil {
				framework.Failf("Failed to create temporary directory: %v", err)
			}
			path := filepath.Join(tmpdir, "test")
			defer os.Remove(path)
			defer os.Remove(tmpdir)
			tk := e2ekubectl.NewTestKubeconfig(framework.TestContext.CertDir, framework.TestContext.Host, framework.TestContext.KubeConfig, framework.TestContext.KubeContext, framework.TestContext.KubectlPath, ns)
			cmd := tk.KubectlCmd("proxy", fmt.Sprintf("--unix-socket=%s", path))
			stdout, stderr, err := framework.StartCmdAndStreamOutput(cmd)
			if err != nil {
				framework.Failf("Failed to start kubectl command: %v", err)
			}
			defer stdout.Close()
			defer stderr.Close()
			defer framework.TryKill(cmd)
			buf := make([]byte, 128)
			if _, err = stdout.Read(buf); err != nil {
				framework.Failf("Expected output from kubectl proxy: %v", err)
			}
			ginkgo.By("retrieving proxy /api/ output")
			_, err = curlUnix("http://unused/api", path)
			if err != nil {
				framework.Failf("Failed get of /api at %s: %v", path, err)
			}
		})
	})

	// 本测试必须 [Serial] 运行，因为它会修改节点使 Pod 无法在节点上执行，
	// 从而影响其他并行运行的内容。
	f.Describe("Kubectl taint", framework.WithSerial(), func() {
		ginkgo.It("should update the taint on a node", func(ctx context.Context) {
			testTaint := v1.Taint{
				Key:    fmt.Sprintf("kubernetes.io/e2e-taint-key-001-%s", string(uuid.NewUUID())),
				Value:  "testing-taint-value",
				Effect: v1.TaintEffectNoSchedule,
			}

			nodeName := scheduling.GetNodeThatCanRunPod(ctx, f)

			ginkgo.By("adding the taint " + testTaint.ToString() + " to a node")
			runKubectlRetryOrDie(ns, "taint", "nodes", nodeName, testTaint.ToString())
			ginkgo.DeferCleanup(e2enode.RemoveTaintOffNode, f.ClientSet, nodeName, testTaint)

			ginkgo.By("verifying the node has the taint " + testTaint.ToString())
			output := runKubectlRetryOrDie(ns, "describe", "node", nodeName)
			requiredStrings := [][]string{
				{"Name:", nodeName},
				{"Taints:"},
				{testTaint.ToString()},
			}
			checkOutput(output, requiredStrings)

			ginkgo.By("removing the taint " + testTaint.ToString() + " of a node")
			runKubectlRetryOrDie(ns, "taint", "nodes", nodeName, testTaint.Key+":"+string(testTaint.Effect)+"-")
			ginkgo.By("verifying the node doesn't have the taint " + testTaint.Key)
			output = runKubectlRetryOrDie(ns, "describe", "node", nodeName)
			if strings.Contains(output, testTaint.Key) {
				framework.Fail("Failed removing taint " + testTaint.Key + " of the node " + nodeName)
			}
		})

		ginkgo.It("should remove all the taints with the same key off a node", func(ctx context.Context) {
			testTaint := v1.Taint{
				Key:    fmt.Sprintf("kubernetes.io/e2e-taint-key-002-%s", string(uuid.NewUUID())),
				Value:  "testing-taint-value",
				Effect: v1.TaintEffectNoSchedule,
			}

			nodeName := scheduling.GetNodeThatCanRunPod(ctx, f)

			ginkgo.By("adding the taint " + testTaint.ToString() + " to a node")
			runKubectlRetryOrDie(ns, "taint", "nodes", nodeName, testTaint.ToString())
			ginkgo.DeferCleanup(e2enode.RemoveTaintOffNode, f.ClientSet, nodeName,
				testTaint)

			ginkgo.By("verifying the node has the taint " + testTaint.ToString())
			output := runKubectlRetryOrDie(ns, "describe", "node", nodeName)
			requiredStrings := [][]string{
				{"Name:", nodeName},
				{"Taints:"},
				{testTaint.ToString()},
			}
			checkOutput(output, requiredStrings)

			newTestTaint := v1.Taint{
				Key:    testTaint.Key,
				Value:  "another-testing-taint-value",
				Effect: v1.TaintEffectPreferNoSchedule,
			}
			ginkgo.By("adding another taint " + newTestTaint.ToString() + " to the node")
			runKubectlRetryOrDie(ns, "taint", "nodes", nodeName, newTestTaint.ToString())
			ginkgo.DeferCleanup(e2enode.RemoveTaintOffNode, f.ClientSet, nodeName, newTestTaint)

			ginkgo.By("verifying the node has the taint " + newTestTaint.ToString())
			output = runKubectlRetryOrDie(ns, "describe", "node", nodeName)
			requiredStrings = [][]string{
				{"Name:", nodeName},
				{"Taints:"},
				{newTestTaint.ToString()},
			}
			checkOutput(output, requiredStrings)

			noExecuteTaint := v1.Taint{
				Key:    testTaint.Key,
				Value:  "testing-taint-value-no-execute",
				Effect: v1.TaintEffectNoExecute,
			}
			ginkgo.By("adding NoExecute taint " + noExecuteTaint.ToString() + " to the node")
			runKubectlRetryOrDie(ns, "taint", "nodes", nodeName, noExecuteTaint.ToString())
			ginkgo.DeferCleanup(e2enode.RemoveTaintOffNode, f.ClientSet, nodeName, noExecuteTaint)

			ginkgo.By("verifying the node has the taint " + noExecuteTaint.ToString())
			output = runKubectlRetryOrDie(ns, "describe", "node", nodeName)
			requiredStrings = [][]string{
				{"Name:", nodeName},
				{"Taints:"},
				{noExecuteTaint.ToString()},
			}
			checkOutput(output, requiredStrings)

			ginkgo.By("removing all taints that have the same key " + testTaint.Key + " of the node")
			runKubectlRetryOrDie(ns, "taint", "nodes", nodeName, testTaint.Key+"-")
			ginkgo.By("verifying the node doesn't have the taints that have the same key " + testTaint.Key)
			output = runKubectlRetryOrDie(ns, "describe", "node", nodeName)
			if strings.Contains(output, testTaint.Key) {
				framework.Fail("Failed removing taints " + testTaint.Key + " of the node " + nodeName)
			}
		})
	})

	ginkgo.Describe("Kubectl events", func() {
		ginkgo.It("should show event when pod is created", func(ctx context.Context) {
			podName := "e2e-test-agnhost-pod"
			agnhostImage := imageutils.GetE2EImage(imageutils.Agnhost)
			ginkgo.By("running the image " + agnhostImage)
			e2ekubectl.RunKubectlOrDie(ns, "run", podName, "--image="+agnhostImage, podRunningTimeoutArg, "--labels=run="+podName)

			ginkgo.By("verifying the pod " + podName + " is running")
			label := labels.SelectorFromSet(map[string]string{"run": podName})
			err := testutils.WaitForPodsWithLabelRunning(c, ns, label)
			if err != nil {
				framework.Failf("Failed getting pod %s: %v", podName, err)
			}

			ginkgo.By("show started event for this pod")
			events := e2ekubectl.RunKubectlOrDie(ns, "events", "--for=pod/"+podName)

			// 把多个空格替换成单个空格
			eventsStr := strings.Join(strings.Fields(strings.TrimSpace(events)), " ")
			if !strings.Contains(string(eventsStr), fmt.Sprintf("Normal Scheduled Pod/%s", podName)) {
				framework.Failf("failed to list expected event with pod name: %s, got: %s", podName, events)
			}

			ginkgo.By("expect not showing any WARNING message except timeouts")
			events = e2ekubectl.RunKubectlOrDie(ns, "events", "--types=WARNING", "--for=pod/"+podName)
			if events != "" && !strings.Contains(events, "timed out") {
				framework.Failf("unexpected non-timeout WARNING event fired, got: %s ", events)
			}
		})
	})

	ginkgo.Describe("Kubectl create quota", func() {
		ginkgo.It("should create a quota without scopes", func(ctx context.Context) {
			quotaName := "million"

			ginkgo.By("calling kubectl quota")
			e2ekubectl.RunKubectlOrDie(ns, "create", "quota", quotaName, "--hard=pods=1000000,services=1000000")

			ginkgo.By("verifying that the quota was created")
			quota, err := c.CoreV1().ResourceQuotas(ns).Get(ctx, quotaName, metav1.GetOptions{})
			if err != nil {
				framework.Failf("Failed getting quota %s: %v", quotaName, err)
			}

			if len(quota.Spec.Scopes) != 0 {
				framework.Failf("Expected empty scopes, got %v", quota.Spec.Scopes)
			}
			if len(quota.Spec.Hard) != 2 {
				framework.Failf("Expected two resources, got %v", quota.Spec.Hard)
			}
			r, found := quota.Spec.Hard[v1.ResourcePods]
			if expected := resource.MustParse("1000000"); !found || (&r).Cmp(expected) != 0 {
				framework.Failf("Expected pods=1000000, got %v", r)
			}
			r, found = quota.Spec.Hard[v1.ResourceServices]
			if expected := resource.MustParse("1000000"); !found || (&r).Cmp(expected) != 0 {
				framework.Failf("Expected services=1000000, got %v", r)
			}
		})

		ginkgo.It("should create a quota with scopes", func(ctx context.Context) {
			quotaName := "scopes"

			ginkgo.By("calling kubectl quota")
			e2ekubectl.RunKubectlOrDie(ns, "create", "quota", quotaName, "--hard=pods=1000000", "--scopes=BestEffort,NotTerminating")

			ginkgo.By("verifying that the quota was created")
			quota, err := c.CoreV1().ResourceQuotas(ns).Get(ctx, quotaName, metav1.GetOptions{})
			if err != nil {
				framework.Failf("Failed getting quota %s: %v", quotaName, err)
			}

			if len(quota.Spec.Scopes) != 2 {
				framework.Failf("Expected two scopes, got %v", quota.Spec.Scopes)
			}
			scopes := make(map[v1.ResourceQuotaScope]struct{})
			for _, scope := range quota.Spec.Scopes {
				scopes[scope] = struct{}{}
			}
			if _, found := scopes[v1.ResourceQuotaScopeBestEffort]; !found {
				framework.Failf("Expected BestEffort scope, got %v", quota.Spec.Scopes)
			}
			if _, found := scopes[v1.ResourceQuotaScopeNotTerminating]; !found {
				framework.Failf("Expected NotTerminating scope, got %v", quota.Spec.Scopes)
			}
		})

		ginkgo.It("should reject quota with invalid scopes", func(ctx context.Context) {
			quotaName := "scopes"

			ginkgo.By("calling kubectl quota")
			out, err := e2ekubectl.RunKubectl(ns, "create", "quota", quotaName, "--hard=hard=pods=1000000", "--scopes=Foo")
			if err == nil {
				framework.Failf("Expected kubectl to fail, but it succeeded: %s", out)
			}
		})
	})

	ginkgo.Describe("kubectl wait", func() {
		ginkgo.It("should ignore not found error with --for=delete", func(ctx context.Context) {
			ginkgo.By("calling kubectl wait --for=delete")
			e2ekubectl.RunKubectlOrDie(ns, "wait", "--for=delete", "pod/doesnotexist")
			e2ekubectl.RunKubectlOrDie(ns, "wait", "--for=delete", "pod", "--selector=app.kubernetes.io/name=noexist")
		})
	})

	ginkgo.Describe("kubectl subresource flag", func() {
		ginkgo.It("should not be used in a bulk GET", func() {
			ginkgo.By("calling kubectl get nodes --subresource=status")
			out, err := e2ekubectl.RunKubectl("", "get", "nodes", "--subresource=status")
			gomega.Expect(err).To(gomega.HaveOccurred(), fmt.Sprintf("Expected kubectl to fail, but it succeeded: %s", out))
			gomega.Expect(err).To(gomega.ContainSubstring("subresource cannot be used when bulk resources are specified"))
		})
		ginkgo.It("GET on status subresource of built-in type (node) returns identical info as GET on the built-in type", func(ctx context.Context) {
			ginkgo.By("first listing nodes in the cluster, and using first node of the list")
			nodes, err := c.CoreV1().Nodes().List(ctx, metav1.ListOptions{
				FieldSelector: "spec.unschedulable=false",
			})
			framework.ExpectNoError(err)
			gomega.Expect(nodes.Items).ToNot(gomega.BeEmpty())
			node := nodes.Items[0]
			// 避免比较那些在两次 kubectl 调用之间可能
			// 发生变化的字段值。这里
			// 只比较 name 和 version 字段。
			ginkgo.By(fmt.Sprintf("calling kubectl get nodes %s", node.Name))
			outBuiltIn := e2ekubectl.RunKubectlOrDie("", "get", "nodes", node.Name,
				"--output=jsonpath='{.metadata.name}{.status.nodeInfo.kubeletVersion}'",
			)
			ginkgo.By(fmt.Sprintf("calling kubectl get nodes %s --subresource=status", node.Name))
			outStatusSubresource := e2ekubectl.RunKubectlOrDie("", "get", "nodes", node.Name,
				"--output=jsonpath='{.metadata.name}{.status.nodeInfo.kubeletVersion}'",
				"--subresource=status",
			)
			gomega.Expect(outBuiltIn).To(gomega.Equal(outStatusSubresource))
		})
	})
})

func getTestContextHost() string {
	if len(framework.TestContext.Host) > 0 {
		return framework.TestContext.Host
	}
	// 如果有 kubeconfig，从中选第一个 server
	if framework.TestContext.KubeConfig != "" {
		c, err := clientcmd.LoadFromFile(framework.TestContext.KubeConfig)
		if err == nil {
			for _, v := range c.Clusters {
				if v.Server != "" {
					framework.Logf("--host variable was not set, picking up the first server from %s",
						framework.TestContext.KubeConfig)
					return v.Server
				}
			}
		}
	}
	framework.Failf("--host variable must be set to the full URI to the api server on e2e run.")
	return ""
}

// 检查按行拆分后的输出是否包含所需元素。
func checkOutputReturnError(output string, required [][]string) error {
	outputLines := strings.Split(output, "\n")
	currentLine := 0
	for _, requirement := range required {
		for currentLine < len(outputLines) && !strings.Contains(outputLines[currentLine], requirement[0]) {
			currentLine++
		}
		if currentLine == len(outputLines) {
			return fmt.Errorf("failed to find %s in %s", requirement[0], output)
		}
		for _, item := range requirement[1:] {
			if !strings.Contains(outputLines[currentLine], item) {
				return fmt.Errorf("failed to find %s in %s", item, outputLines[currentLine])
			}
		}
	}
	return nil
}

func checkOutput(output string, required [][]string) {
	err := checkOutputReturnError(output, required)
	if err != nil {
		framework.Failf("%v", err)
	}
}

func checkKubectlOutputWithRetry(namespace string, required [][]string, args ...string) {
	var pollErr error
	wait.PollImmediate(time.Second, time.Minute, func() (bool, error) {
		output := e2ekubectl.RunKubectlOrDie(namespace, args...)
		err := checkOutputReturnError(output, required)
		if err != nil {
			pollErr = err
			return false, nil
		}
		pollErr = nil
		return true, nil
	})
	if pollErr != nil {
		framework.Failf("%v", pollErr)
	}
	return
}

func checkContainersImage(containers []v1.Container, expectImage string) bool {
	return containers == nil || len(containers) != 1 || containers[0].Image != expectImage
}

func getAPIVersions(apiEndpoint string) (*metav1.APIVersions, error) {
	body, err := curl(apiEndpoint)
	if err != nil {
		return nil, fmt.Errorf("Failed http.Get of %s: %w", apiEndpoint, err)
	}
	var apiVersions metav1.APIVersions
	if err := json.Unmarshal([]byte(body), &apiVersions); err != nil {
		return nil, fmt.Errorf("Failed to parse /api output %s: %w", body, err)
	}
	return &apiVersions, nil
}

func startProxyServer(ns string) (int, *exec.Cmd, error) {
	// 指定端口 0 表示希望操作系统挑选随机端口。
	tk := e2ekubectl.NewTestKubeconfig(framework.TestContext.CertDir, framework.TestContext.Host, framework.TestContext.KubeConfig, framework.TestContext.KubeContext, framework.TestContext.KubectlPath, ns)
	cmd := tk.KubectlCmd("proxy", "-p", "0", "--disable-filter")
	stdout, stderr, err := framework.StartCmdAndStreamOutput(cmd)
	if err != nil {
		return -1, nil, err
	}
	buf := make([]byte, 128)
	var n int
	if n, err = stdout.Read(buf); err != nil {
		return -1, cmd, fmt.Errorf("Failed to read from kubectl proxy stdout: %w", err)
	}
	go func() {
		out, _ := io.ReadAll(stdout)
		framework.Logf("kubectl proxy stdout: %s", string(buf[:n])+string(out))
		stdout.Close()
	}()
	go func() {
		err, _ := io.ReadAll(stderr)
		framework.Logf("kubectl proxy stderr: %s", string(err))
		stderr.Close()
	}()
	output := string(buf[:n])
	match := proxyRegexp.FindStringSubmatch(output)
	if len(match) == 2 {
		if port, err := strconv.Atoi(match[1]); err == nil {
			return port, cmd, nil
		}
	}
	return -1, cmd, fmt.Errorf("Failed to parse port from proxy stdout: %s", output)
}

func curlUnix(url string, path string) (string, error) {
	dial := func(ctx context.Context, proto, addr string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", path)
	}
	transport := utilnet.SetTransportDefaults(&http.Transport{
		DialContext: dial,
	})
	return curlTransport(url, transport)
}

func curlTransport(url string, transport *http.Transport) (string, error) {
	client := &http.Client{Transport: transport}
	resp, err := client.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body[:]), nil
}

func curl(url string) (string, error) {
	return curlTransport(url, utilnet.SetTransportDefaults(&http.Transport{}))
}

func validateGuestbookApp(ctx context.Context, c clientset.Interface, ns string) {
	framework.Logf("Waiting for all frontend pods to be Running.")
	label := labels.SelectorFromSet(labels.Set(map[string]string{"tier": "frontend", "app": "guestbook"}))
	err := testutils.WaitForPodsWithLabelRunning(c, ns, label)
	framework.ExpectNoError(err)
	framework.Logf("Waiting for frontend to serve content.")
	if !waitForGuestbookResponse(ctx, c, "get", "", `{"data":""}`, guestbookStartupTimeout, ns) {
		framework.Failf("Frontend service did not start serving content in %v seconds.", guestbookStartupTimeout.Seconds())
	}

	framework.Logf("Trying to add a new entry to the guestbook.")
	if !waitForGuestbookResponse(ctx, c, "set", "TestEntry", `{"message":"Updated"}`, guestbookResponseTimeout, ns) {
		framework.Failf("Cannot added new entry in %v seconds.", guestbookResponseTimeout.Seconds())
	}

	framework.Logf("Verifying that added entry can be retrieved.")
	if !waitForGuestbookResponse(ctx, c, "get", "", `{"data":"TestEntry"}`, guestbookResponseTimeout, ns) {
		framework.Failf("Entry to guestbook wasn't correctly added in %v seconds.", guestbookResponseTimeout.Seconds())
	}
}

// 返回是否按时从 guestbook 收到预期响应。
func waitForGuestbookResponse(ctx context.Context, c clientset.Interface, cmd, arg, expectedResponse string, timeout time.Duration, ns string) bool {
	for start := time.Now(); time.Since(start) < timeout && ctx.Err() == nil; time.Sleep(5 * time.Second) {
		res, err := makeRequestToGuestbook(ctx, c, cmd, arg, ns)
		if err == nil && res == expectedResponse {
			return true
		}
		framework.Logf("Failed to get response from guestbook. err: %v, response: %s", err, res)
	}
	return false
}

func makeRequestToGuestbook(ctx context.Context, c clientset.Interface, cmd, value string, ns string) (string, error) {
	proxyRequest, errProxy := e2eservice.GetServicesProxyRequest(c, c.CoreV1().RESTClient().Get())
	if errProxy != nil {
		return "", errProxy
	}

	ctx, cancel := context.WithTimeout(ctx, framework.SingleCallTimeout)
	defer cancel()

	result, err := proxyRequest.Namespace(ns).
		Name("frontend").
		Suffix("/guestbook").
		Param("cmd", cmd).
		Param("key", "messages").
		Param("value", value).
		Do(ctx).
		Raw()
	return string(result), err
}

type updateDemoData struct {
	Image string
}

const applyTestLabel = "kubectl.kubernetes.io/apply-test"

func readReplicationControllerFromString(contents string) *v1.ReplicationController {
	rc := v1.ReplicationController{}
	if err := yaml.Unmarshal([]byte(contents), &rc); err != nil {
		framework.Fail(err.Error())
	}

	return &rc
}

func modifyReplicationControllerConfiguration(contents string) io.Reader {
	rc := readReplicationControllerFromString(contents)
	rc.Labels[applyTestLabel] = "ADDED"
	rc.Spec.Selector[applyTestLabel] = "ADDED"
	rc.Spec.Template.Labels[applyTestLabel] = "ADDED"
	data, err := json.Marshal(rc)
	if err != nil {
		framework.Failf("json marshal failed: %s\n", err)
	}

	return bytes.NewReader(data)
}

func forEachReplicationController(ctx context.Context, c clientset.Interface, ns, selectorKey, selectorValue string, fn func(v1.ReplicationController)) {
	var rcs *v1.ReplicationControllerList
	var err error
	for t := time.Now(); time.Since(t) < framework.PodListTimeout && ctx.Err() == nil; time.Sleep(framework.Poll) {
		label := labels.SelectorFromSet(labels.Set(map[string]string{selectorKey: selectorValue}))
		options := metav1.ListOptions{LabelSelector: label.String()}
		rcs, err = c.CoreV1().ReplicationControllers(ns).List(ctx, options)
		framework.ExpectNoError(err)
		if len(rcs.Items) > 0 {
			break
		}
	}

	if rcs == nil || len(rcs.Items) == 0 {
		framework.Failf("No replication controllers found")
	}

	for _, rc := range rcs.Items {
		fn(rc)
	}
}

func validateReplicationControllerConfiguration(rc v1.ReplicationController) {
	if rc.Name == "agnhost-primary" {
		if _, ok := rc.Annotations[v1.LastAppliedConfigAnnotation]; !ok {
			framework.Failf("Annotation not found in modified configuration:\n%v\n", rc)
		}

		if value, ok := rc.Labels[applyTestLabel]; !ok || value != "ADDED" {
			framework.Failf("Added label %s not found in modified configuration:\n%v\n", applyTestLabel, rc)
		}
	}
}

// getUDData 根据输入字符串（例如 kitten.jpg）创建校验函数。
// 例如发送 "kitten.jpg" 时，该函数校验容器中 json 字段的 image jpg = kitten.jpg
func getUDData(jpgExpected string, ns string) func(context.Context, clientset.Interface, string) error {

	// getUDData 校验 update-demo 中的 data.json（数据正常时返回 nil）。
	return func(ctx context.Context, c clientset.Interface, podID string) error {
		framework.Logf("validating pod %s", podID)

		ctx, cancel := context.WithTimeout(ctx, framework.SingleCallTimeout)
		defer cancel()

		body, err := c.CoreV1().RESTClient().Get().
			Namespace(ns).
			Resource("pods").
			SubResource("proxy").
			Name(podID).
			Suffix("data.json").
			Do(ctx).
			Raw()

		if err != nil {
			if ctx.Err() != nil {
				framework.Failf("Failed to retrieve data from container: %v", err)
			}
			return err
		}
		framework.Logf("got data: %s", body)
		var data updateDemoData
		if err := json.Unmarshal(body, &data); err != nil {
			return err
		}
		framework.Logf("Unmarshalled json jpg/img => %s , expecting %s .", data, jpgExpected)
		if strings.Contains(data.Image, jpgExpected) {
			return nil
		}
		return fmt.Errorf("data served up in container is inaccurate, %s didn't contain %s", data, jpgExpected)
	}
}

// newBlockingReader 返回一个 reader，允许读取给定字符串，
// 然后阻塞直到返回的 closer 被调用 Close()。
//
// 这里显式把 reader 和 closer 分开返回，因为
// closer 必须是 os.Pipe() 得到的 *os.File。这样 kubectl 的 exec 才能把底层文件描述符
// 传给 exec syscall，而不是再创建一个 os.Pipe 并在源（如 stdin）与管道写端之间
// 的 io.Copy 上阻塞。
func newBlockingReader(s string) (io.Reader, io.Closer, error) {
	r, w, err := os.Pipe()
	if err != nil {
		return nil, nil, err
	}
	w.Write([]byte(s))
	return r, w, nil
}

// createApplyCustomResource 断言给定 CustomResource 能被创建和应用，
// 且不会被 kubectl 校验拒绝
func createApplyCustomResource(resource, namespace, name string, crd *crd.TestCrd) error {
	ginkgo.By("successfully create CR")
	if _, err := e2ekubectl.RunKubectlInput(namespace, resource, "create", "--validate=true", "-f", "-"); err != nil {
		return fmt.Errorf("failed to create CR %s in namespace %s: %w", resource, namespace, err)
	}
	if _, err := e2ekubectl.RunKubectl(namespace, "delete", crd.Crd.Spec.Names.Plural, name); err != nil {
		return fmt.Errorf("failed to delete CR %s: %w", name, err)
	}
	ginkgo.By("successfully apply CR")
	if _, err := e2ekubectl.RunKubectlInput(namespace, resource, "apply", "--validate=true", "-f", "-"); err != nil {
		return fmt.Errorf("failed to apply CR %s in namespace %s: %w", resource, namespace, err)
	}
	if _, err := e2ekubectl.RunKubectl(namespace, "delete", crd.Crd.Spec.Names.Plural, name); err != nil {
		return fmt.Errorf("failed to delete CR %s: %w", name, err)
	}
	return nil
}

// trimDockerRegistry 用于从镜像名开头去掉 docker.io/library。
// 社区版 docker 安装不会给镜像名加 registry 前缀，而使用其他运行时或 RHEL 额外仓库安装的 docker 会加。
// 因此该函数在存在时帮忙去掉 docker.io/library 前缀
func trimDockerRegistry(imagename string) string {
	imagename = strings.Replace(imagename, "docker.io/", "", 1)
	return strings.Replace(imagename, "library/", "", 1)
}

// validatorFn 是各个测试会实现的函数。
// 也许将来我们希望它返回的不仅是 error。
type validatorFn func(ctx context.Context, c clientset.Interface, podID string) error

// validateController 是测试运行中的 RC 的通用机制。
// 它接受容器名、测试名，以及由具体测试插入的校验函数。
// "containername"：要 grep 的对象。
// "containerImage"：期望启动的镜像名。不要与要校验的镜像（kitten.jpg）混淆。
// "testname"：出错时会冒泡到日志/失败消息中。
// "validator" 函数：接收 podID 和 client，可执行某些具体校验。
func validateController(ctx context.Context, c clientset.Interface, containerImage string, replicas int, containername string, testname string, validator validatorFn, ns string) {
	containerImage = trimDockerRegistry(containerImage)
	getPodsTemplate := "--template={{range.items}}{{.metadata.name}} {{end}}"

	getContainerStateTemplate := fmt.Sprintf(`--template={{if (exists . "status" "containerStatuses")}}{{range .status.containerStatuses}}{{if (and (eq .name "%s") (exists . "state" "running"))}}true{{end}}{{end}}{{end}}`, containername)

	getImageTemplate := fmt.Sprintf(`--template={{if (exists . "spec" "containers")}}{{range .spec.containers}}{{if eq .name "%s"}}{{.image}}{{end}}{{end}}{{end}}`, containername)

	ginkgo.By(fmt.Sprintf("waiting for all containers in %s pods to come up.", testname)) //testname 应为 selector
waitLoop:
	for start := time.Now(); time.Since(start) < framework.PodStartTimeout && ctx.Err() == nil; time.Sleep(5 * time.Second) {
		getPodsOutput := e2ekubectl.RunKubectlOrDie(ns, "get", "pods", "-o", "template", getPodsTemplate, "-l", testname)
		pods := strings.Fields(getPodsOutput)
		if numPods := len(pods); numPods != replicas {
			ginkgo.By(fmt.Sprintf("Replicas for %s: expected=%d actual=%d", testname, replicas, numPods))
			continue
		}
		var runningPods []string
		for _, podID := range pods {
			running := e2ekubectl.RunKubectlOrDie(ns, "get", "pods", podID, "-o", "template", getContainerStateTemplate)
			if running != "true" {
				framework.Logf("%s is created but not running", podID)
				continue waitLoop
			}

			currentImage := e2ekubectl.RunKubectlOrDie(ns, "get", "pods", podID, "-o", "template", getImageTemplate)
			currentImage = trimDockerRegistry(currentImage)
			if currentImage != containerImage {
				framework.Logf("%s is created but running wrong image; expected: %s, actual: %s", podID, containerImage, currentImage)
				continue waitLoop
			}

			// 在这里调用通用校验函数。
			// 例如可校验：(1) 获取 url 可用；(2) url 返回的内容正确。
			if err := validator(ctx, c, podID); err != nil {
				framework.Logf("%s is running right image but validator function failed: %v", podID, err)
				continue waitLoop
			}

			framework.Logf("%s is verified up and running", podID)
			runningPods = append(runningPods, podID)
		}
		// 执行到这里说明所有检查都通过了。
		if len(runningPods) == replicas {
			return
		}
	}
	// 执行到这里意味着有一项或多项检查多次失败。假设不是竞态条件，那就是哪里坏了。
	framework.Failf("Timed out after %v seconds waiting for %s pods to reach valid state", framework.PodStartTimeout.Seconds(), testname)
}

// mustListObjectsInNamespace 查询给定命名空间中我们测试使用的所有对象。
// 目前只有 ConfigMap。
// 会过滤 "系统" ConfigMap，例如 "kube-root-ca.crt"。
func mustListObjectsInNamespace(ctx context.Context, c clientset.Interface, ns string) []runtime.Object {
	var objects []runtime.Object
	configMaps, err := c.CoreV1().ConfigMaps(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		framework.Failf("error listing configmaps: %v", err)
	}
	for i := range configMaps.Items {
		cm := &configMaps.Items[i]
		if cm.Name == "kube-root-ca.crt" {
			// 忽略系统对象
			continue
		}
		objects = append(objects, cm)
	}
	return objects
}

// mustGetNames 返回包含每个对象 metadata.name 的切片。
func mustGetNames(objects []runtime.Object) []string {
	var names []string
	for _, obj := range objects {
		metaAccessor, err := meta.Accessor(obj)
		if err != nil {
			framework.Failf("error getting accessor for %T: %v", obj, err)
		}
		name := metaAccessor.GetName()
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
