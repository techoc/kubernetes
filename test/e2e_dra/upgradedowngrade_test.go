/*
Copyright 2022 The Kubernetes Authors.

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

package e2edra

import (
	"archive/tar"
	"compress/gzip"
	_ "embed"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/onsi/gomega"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/version"
	restclient "k8s.io/client-go/rest"
	"k8s.io/dynamic-resource-allocation/resourceslice"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	drautils "k8s.io/kubernetes/test/e2e/dra/utils"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2etestfiles "k8s.io/kubernetes/test/e2e/framework/testfiles"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
	"k8s.io/kubernetes/test/utils/localupcluster"
)

var errHTTP404 = errors.New("resource not found (404)")

func init() {
	// -v=5 可能有助于调试 driver 操作，但通常不需要。
	ktesting.SetDefaultVerbosity(2)
}

type initialTestFunc func(tCtx ktesting.TContext, builder *drautils.Builder) upgradedTestFunc

type upgradedTestFunc func(tCtx ktesting.TContext) downgradedTestFunc

type downgradedTestFunc func(tCtx ktesting.TContext)

// countSlices 返回为给定
// driver 资源集创建多少 ResourceSlice（每个 entry 的每个 pool 中每个 slice 一个）。
func countSlices(driverResources map[string]resourceslice.DriverResources) int {
	n := 0
	for _, dr := range driverResources {
		for _, pool := range dr.Pools {
			n += len(pool.Slices)
		}
	}
	return n
}

// waitForSlices 等待 driver 的 ResourceSlice 被重建。
func waitForSlices(tCtx ktesting.TContext, name string, b *drautils.Builder, driverResources map[string]resourceslice.DriverResources) {
	if driverResources == nil {
		// 跳过等待 slice 就绪。
		// 但每个测试仍可能有自己的 slice，应在各测试中处理。
		return
	}
	numSlices := countSlices(driverResources)
	tCtx.WithStep(fmt.Sprintf("wait for %s ResourceSlices", name)).
		Eventually(b.Driver.NewGetSlices()).
		WithTimeout(5 * time.Minute).
		Should(gomega.HaveField("Items", gomega.HaveLen(numSlices)))
}

var repoRoot = repoRootDefault()

const (
	localUpClusterRuntimeConfig = "resource.k8s.io/v1beta1,resource.k8s.io/v1beta2,resource.k8s.io/v1alpha3"
)

func currentBinDir() (envName, content string) {
	envName = "KUBERNETES_SERVER_BIN_DIR"
	content, _ = os.LookupEnv(envName)
	return
}

// repoRootDefault 判断 E2E suite 是在其目录中调用（如 `go test ./test/e2e`），
// 直接在根目录调用（如 `make test-e2e` 或 `ginkgo ./test/e2e`），还是
// 在 _output 目录深处调用（`ginkgo _output/bin/e2e.test`，其中 `_output/bin` 实际是符号链接）。
func repoRootDefault() string {
	for i := range 10 {
		path := "." + strings.Repeat("/..", i)
		if _, err := os.Stat(path + "/test/e2e/framework"); err == nil {
			return path
		}
	}
	// 传统的默认值。
	return "../../"
}

func TestUpgradeDowngrade(t *testing.T) { testUpgradeDowngrade(ktesting.Init(t)) }
func testUpgradeDowngrade(tCtx ktesting.TContext) {
	// 通常由 test/e2e 完成的其他事情。
	e2etestfiles.AddFileSource(e2etestfiles.RootFileSource{Root: repoRoot})

	// 理想情况下不应有直接调用 gomega.Expect 的代码，
	// 但目前还没做到（如 e2epod.MakePod）。所以现在安装
	// 一个 fail handler，把失败记录到主测试 context 中。
	gomega.RegisterFailHandler(func(message string, callerSkip ...int) {
		tCtx.Helper()
		tCtx.Fatal(message)
	})

	envName, dir := currentBinDir()
	if dir == "" {
		tCtx.Fatalf("%s must be set to test DRA upgrade/downgrade scenarios.", envName)
	}

	// 确定要降级到哪个版本。
	var major, previousMinor uint
	var gitVersion string

	tCtx.Step("get source code version", func(tCtx ktesting.TContext) {
		var err error
		gitVersion, _, err = sourceVersion(tCtx, repoRoot)
		tCtx.ExpectNoError(err, "determine source code version for repo root %q", repoRoot)
		version, err := version.ParseGeneric(gitVersion)
		tCtx.ExpectNoError(err, "parse version %s of repo root %q", gitVersion, repoRoot)
		major, previousMinor = version.Major(), version.Minor()-1
		if strings.Contains(gitVersion, "-alpha.0") {
			// 所有直到并包括 x.y.z-alpha.0 的版本都视为
			// 仍是上一个 minor 版本 x.(y-1)。原因有两个：
			//
			// - 在 -rc.0 前后的代码冻结期，master 分支已经
			//   用 -alpha.0 标识自己为下一个 release。没有这个
			//   特例，我们会把版本偏差测试从已验证可行的做法
			//   改成别的做法，这可能会出问题，而且至少出过一次问题。
			//
			// - 在下一个周期早期，与上一个 release 的差异
			//   很小，所以回退更早的版本更有意义。
			//
			previousMinor--
		}
		tCtx.Logf("got version: major: %d, minor: %d, previous minor: %d", major, version.Minor(), previousMinor)
	})

	// 可以设置 KUBERNETES_SERVER_CACHE_DIR，在测试重启间保留下载文件。
	binDir, cacheBinaries := os.LookupEnv("KUBERNETES_SERVER_CACHE_DIR")
	if !cacheBinaries {
		binDir = tCtx.TempDir()
	}
	haveBinaries := false

	// 获取上一个 release。
	var previousURL, previousVersion string
	tCtx.Step("get previous release info", func(tCtx ktesting.TContext) {
		tCtx.Logf("stable release %d.%d", major, previousMinor)
		var err error
		previousURL, previousVersion, err = serverDownloadURL(tCtx, "stable", major, previousMinor)
		if errors.Is(err, errHTTP404) {
			tCtx.Logf("stable doesn't exist, get latest release %d.%d", major, previousMinor)
			previousURL, previousVersion, err = serverDownloadURL(tCtx, "latest", major, previousMinor)
		}
		tCtx.ExpectNoError(err)
		tCtx.Logf("got previous release version: %s, URL: %s", previousVersion, previousURL)
	})

	if cacheBinaries {
		binDir = path.Join(binDir, previousVersion)
		_, err := os.Stat(path.Join(binDir, string(localupcluster.KubeClusterComponents[0])))
		if err == nil {
			haveBinaries = true
		}
	}
	if !haveBinaries {
		tCtx.Step(fmt.Sprintf("download and unpack %s", previousURL), func(tCtx ktesting.TContext) {
			req, err := http.NewRequestWithContext(tCtx, http.MethodGet, previousURL, nil)
			tCtx.ExpectNoError(err, "construct request")
			response, err := http.DefaultClient.Do(req)
			tCtx.ExpectNoError(err, "download")
			defer func() {
				_ = response.Body.Close()
			}()
			decompress, err := gzip.NewReader(response.Body)
			tCtx.ExpectNoError(err, "construct gzip reader")
			unpack := tar.NewReader(decompress)
			for {
				header, err := unpack.Next()
				if err == io.EOF {
					break
				}
				base := path.Base(header.Name)
				if slices.Contains(localupcluster.KubeClusterComponents, localupcluster.ClusterComponentName(base)) {
					data, err := io.ReadAll(unpack)
					tCtx.ExpectNoError(err, fmt.Sprintf("read content of %s", header.Name))
					tCtx.ExpectNoError(os.MkdirAll(binDir, 0755), "create directory for binaries")
					tCtx.ExpectNoError(os.WriteFile(path.Join(binDir, base), data, 0555), fmt.Sprintf("write content of %s", header.Name))
				}
			}
		})
	}

	cluster := localupcluster.New()
	tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
		tCtx.Step("cleanup", cluster.Stop)
	})
	tCtx.Step(fmt.Sprintf("bring up v%d.%d", major, previousMinor), func(tCtx ktesting.TContext) {
		localUpClusterEnv := map[string]string{
			"RUNTIME_CONFIG": localUpClusterRuntimeConfig,
			"FEATURE_GATES":  "DynamicResourceAllocation=true,DRADeviceTaintRules=true,DRADeviceTaints=true,DRAExtendedResource=true,DRAPartitionableDevices=true",
			// 不需要，因为 driver 将以 "local filesystem" 模式运行（即 driver.IsLocal）："ALLOW_PRIVILEGED": "1",
		}
		cluster.Start(tCtx, fmt.Sprintf("0-initial-%d.%d", major, previousMinor), binDir, localUpClusterEnv, "")
	})

	restConfig := cluster.LoadConfig(tCtx)
	restConfig.UserAgent = fmt.Sprintf("%s -- dra", restclient.DefaultKubernetesUserAgent())
	tCtx = tCtx.WithRESTConfig(restConfig).WithNamespace("default")

	var nodes *drautils.Nodes
	tCtx.Step(fmt.Sprintf("v%d.%d", major, previousMinor), func(tCtx ktesting.TContext) {
		tCtx.ExpectNoError(e2enode.WaitForAllNodesSchedulable(tCtx, tCtx.Client(), 5*time.Minute), "wait for all nodes to be schedulable")
		nodes = drautils.NewNodesNow(tCtx, 1, 1)
	})

	// 升级/降级测试的整体流程始终相同：
	//
	// - 用上一个 release 启动集群。
	// - 部署测试用 DRA driver（如果指定，每个子测试用唯一 driver 名称）。
	//   通过传给每个子测试的 builder 可访问它的 DeviceClass。
	// - 第 1 步：在上一个 release 上运行测试代码。
	// - 把集群升级到当前代码。
	// - 第 2 步：在升级后的集群上运行测试代码。
	// - 把集群降级回上一个 release。
	// - 第 3 步：在降级后的集群上运行测试代码。
	//
	// "测试代码"在这里为每个子测试注册一个函数。
	// 该函数再返回下一段代码，后者再返回最终代码。每个
	// 回调函数都作为一个子测试执行。
	// builder 配置为子测试结束时删除对象，
	// 因此对象会保留到整个测试结束。
	//
	// 每个子测试可提供 driverResources 来定义
	// driver 发布的 ResourceSlice。升级/降级后 kubelet 会清除所有 slice；
	// 框架会自动等待它们被重建。driverResources 为 nil 的子测试
	// 可自行管理 slice，但必须在升级/降级后
	// 自己等待 slice 重建。
	//
	//由于 map 迭代，子测试以未指定的顺序运行，因此每个
	// 都必须自包含。它们共享同一个集群，但各自有
	// 自己的 driver 和 device。
	//
	var subTests = map[string]struct {
		test            initialTestFunc
		driverResources map[string]resourceslice.DriverResources
		configureDriver func(*drautils.Driver)
	}{
		"core-dra": {
			test:            coreDRA,
			driverResources: coreDRAResources(nodes),
		},
		"resource-claim-device-status": {
			test: resourceClaimDeviceStatus,
		},
		"device-taints": {
			test: deviceTaints,
		},
		"partitionable-devices": {
			test:            partitionableDevices,
			driverResources: partitionableDeviceResources(nodes),
		},
		"extended-resource-explicit": {
			test:            extendedResourceUpgradeDowngrade(resourceTypeExplicit),
			driverResources: extendedResourcesDriverResources(nodes),
		},
		"extended-resource-implicit": {
			test:            extendedResourceUpgradeDowngrade(resourceTypeImplicit),
			driverResources: extendedResourcesDriverResources(nodes),
		},
		"pool-name-field-selector-fallback": {
			test:            poolNameFieldSelectorFallback,
			driverResources: poolNameFieldSelectorFallbackResources(nodes),
			configureDriver: func(d *drautils.Driver) {
				d.ReconcilePoolWithName = poolNameFieldSelectorFallbackPool
				d.UsePrivilegedClient = true
			},
		},
	}

	// 为每个子测试创建 driver 和 builder。本地打开 socket
	// 可避免通过重启的 apiserver 代理带来的偶发错误和延迟。builder
	// 在外层 tCtx 上创建，这样清理会在整个测试结束时运行，
	// 而不是在某个子测试步骤结束时。
	//
	builders := make(map[string]*drautils.Builder, len(subTests))
	for name, def := range subTests {
		d := drautils.NewDriverInstance(tCtx)
		d.SetNameSuffix(tCtx, name)
		d.IsLocal = true
		if def.configureDriver != nil {
			def.configureDriver(d)
		}
		d.Run(tCtx, "/var/lib/kubelet", nodes, def.driverResources)
		b := drautils.NewBuilderNow(tCtx, d)
		b.SkipCleanup = true
		builders[name] = b
	}

	upgradedTestFuncs := make(map[string]upgradedTestFunc, len(subTests))
	tCtx.Run("after-cluster-creation", func(tCtx ktesting.TContext) {
		for subTest, def := range subTests {
			tCtx.Run(subTest, func(tCtx ktesting.TContext) {
				// 只有当 def.test 没有因致命错误 panic 时才会设置它，
				// 所以如果第 1 步已经失败，下面不会继续。
				// 其他子测试不受影响。
				upgradedTestFuncs[subTest] = def.test(tCtx, builders[subTest])
			})
		}
	})

	// 可以把它拆成先更新 apiserver，再更新控制面组件，然后重启 kubelet。
	// 对本测试而言，我们主要关心完整的前后对比，所以暂不这样做。
	// TODO: 暂不拆分升级步骤。
	restoreOptions := cluster.Modify(tCtx.WithStep(fmt.Sprintf("update to %s", gitVersion)), "1-"+gitVersion, localupcluster.ModifyOptions{BinDir: dir})

	// kubelet 会清除所有 resource slice，因为它不知道哪些 driver 在运行。
	// 需要等待它们被重建。
	for name, b := range builders {
		waitForSlices(tCtx, name, b, subTests[name].driverResources)
	}

	downgradedTestFuncs := make(map[string]downgradedTestFunc, len(subTests))
	tCtx.Run("after-cluster-upgrade", func(tCtx ktesting.TContext) {
		for subTest, f := range upgradedTestFuncs {
			tCtx.Run(subTest, func(tCtx ktesting.TContext) {
				downgradedTestFuncs[subTest] = f(tCtx)
			})
		}
	})

	// 回滚。
	cluster.Modify(tCtx.WithStep("downgrade"), fmt.Sprintf("2-restored-%d.%d", major, previousMinor), restoreOptions)

	// kubelet 会清除所有 resource slice，因为它不知道哪些 driver 在运行。
	// 需要等待它们被重建。
	for name, b := range builders {
		waitForSlices(tCtx, name, b, subTests[name].driverResources)
	}

	tCtx.Run("after-cluster-downgrade", func(tCtx ktesting.TContext) {
		for subTest, f := range downgradedTestFuncs {
			tCtx.Run(subTest, func(tCtx ktesting.TContext) {
				f(tCtx)
			})
		}
	})
}

// sourceVersion 根据 hack/print-workspace-status.sh 识别 Kubernetes git 版本。
//
// 改写自 https://github.com/kubernetes-sigs/kind/blob/3df64e784cc0ea74125b2a2e9877817418afa3af/pkg/build/nodeimage/internal/kube/source.go#L71-L104
func sourceVersion(tCtx ktesting.TContext, kubeRoot string) (gitVersion string, dockerTag string, err error) {
	// 获取版本输出。
	cmd := exec.CommandContext(tCtx, "hack/print-workspace-status.sh")
	cmd.Dir = kubeRoot
	output, err := cmd.Output()
	if err != nil {
		return "", "", err
	}
	tCtx.Logf("workspace status:\n%s", output)

	// 解析它。
	for _, line := range strings.Split(string(output), "\n") {
		parts := strings.SplitN(line, " ", 2)
		if len(parts) != 2 {
			continue
		}
		switch parts[0] {
		case "gitVersion":
			gitVersion = parts[1]
		case "STABLE_DOCKER_TAG":
			dockerTag = parts[1]
		}
	}
	if gitVersion == "" {
		return "", "", errors.Errorf("could not obtain Kubernetes git version: %q", string(output))

	}
	if dockerTag == "" {
		return "", "", errors.Errorf("count not obtain docker tag: %q", string(output))
	}
	return
}

// serverDownloadURL 根据给定的前缀、major 和 minor 版本号构造
// Kubernetes server tarball 的下载 URL。它发起 HTTP GET 请求从远程
// 文本文件获取版本字符串，然后用取回的版本、
// 当前 OS 和架构构造最终 tarball URL。如果版本文件不存在（HTTP 404），
// 返回 errHTTP404，让调用方尝试其他前缀。返回 tarball URL、版本字符串，
// 或任一步骤失败时的错误。
// 该函数使用提供的 testing context 进行日志和错误处理。
//
// 参数：
// - tCtx: 用于测试 context 和错误处理的 ktesting.TContext。
// - prefix: release 前缀（例如 "stable"、"latest"）。
// - major: major 版本号。
// - minor: minor 版本号。
//
// 返回：
// - 构造好的 tarball 下载 URL 字符串。
// - 从远程文件取回的版本字符串。
// - 请求失败、响应无效或版本文件不存在时的错误。
func serverDownloadURL(tCtx ktesting.TContext, prefix string, major, minor uint) (string, string, error) {
	tCtx.Helper()
	url := fmt.Sprintf("https://dl.k8s.io/release/%s-%d.%d.txt", prefix, major, minor)
	get, err := http.NewRequestWithContext(tCtx, http.MethodGet, url, nil)
	if err != nil {
		return "", "", fmt.Errorf("constructing GET for %s failed: %w", url, err)
	}
	resp, err := http.DefaultClient.Do(get)
	if err != nil {
		return "", "", fmt.Errorf("downloading %s failed: %w", url, err)
	}
	if resp.StatusCode == http.StatusNotFound {
		// 调用方应能区分 HTTP 404
		// 以尝试其他前缀（'stable' 返回 404 时通常试 'latest'）
		return "", "", errHTTP404
	}
	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("getting %s failed: status code: %d, status: %s", url, resp.StatusCode, resp.Status)
	}
	if resp.Body == nil {
		return "", "", fmt.Errorf("empty response for %s", url)
	}
	defer func() {
		tCtx.ExpectNoError(resp.Body.Close(), "close response body")
	}()
	version, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", fmt.Errorf("reading response body for %s failed: %w", url, err)
	}
	return fmt.Sprintf("https://dl.k8s.io/release/%s/kubernetes-server-%s-%s.tar.gz", string(version), runtime.GOOS, runtime.GOARCH), string(version), nil
}

// eventuallyClaimsGone 轮询直到所有指定的 ResourceClaim 都消失（404）。
func eventuallyClaimsGone(tCtx ktesting.TContext, claimNames []string, timeout time.Duration) {
	tCtx.Helper()
	ns := tCtx.Namespace()
	remaining := make([]string, len(claimNames))
	copy(remaining, claimNames)
	tCtx.WithStep("wait for ResourceClaims GC").
		Eventually(func(tCtx ktesting.TContext) int {
			still := remaining[:0]
			for _, name := range remaining {
				_, err := tCtx.Client().ResourceV1().ResourceClaims(ns).Get(tCtx, name, metav1.GetOptions{})
				if err == nil {
					still = append(still, name)
				} else if !apierrors.IsNotFound(err) {
					tCtx.ExpectNoError(err, "check ResourceClaim %s", name)
				}
			}
			remaining = still
			return len(remaining)
		}).WithTimeout(timeout).Should(gomega.Equal(0), "ResourceClaims should be GC'd")
	tCtx.Logf("Verified ResourceClaims %s were garbage collected", claimNames)
}
