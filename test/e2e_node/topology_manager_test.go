/*
Copyright 2019 The Kubernetes Authors.

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

package e2enode

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	admissionapi "k8s.io/pod-security-admission/api"
	"k8s.io/utils/cpuset"

	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	e2etestfiles "k8s.io/kubernetes/test/e2e/framework/testfiles"
	testutils "k8s.io/kubernetes/test/utils"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const (
	numaAlignmentCommand      = `export CPULIST_ALLOWED=$( awk -F":\t*" '/Cpus_allowed_list/ { print $2 }' /proc/self/status); env;`
	numaAlignmentSleepCommand = numaAlignmentCommand + `sleep 1d;`
	podScopeTopology          = "pod"
	containerScopeTopology    = "container"

	minNumaNodes                  = 2
	minNumaNodesPreferClosestNUMA = 4
	minCoreCount                  = 4
	minSriovResource              = 7 // 被测系统上所需的最少 SRIOV VF 数量。
)

// makeTopologyManagerPod() 的辅助类型。
type tmCtnAttribute struct {
	ctnName       string
	cpuRequest    string
	cpuLimit      string
	deviceName    string
	deviceRequest string
	deviceLimit   string
	restartPolicy *v1.ContainerRestartPolicy
}

func detectNUMANodes() int {
	outData, err := exec.Command("/bin/sh", "-c", "lscpu | grep \"NUMA node(s):\" | cut -d \":\" -f 2").Output()
	framework.ExpectNoError(err)

	numaNodes, err := strconv.Atoi(strings.TrimSpace(string(outData)))
	framework.ExpectNoError(err)

	return numaNodes
}

func detectCoresPerSocket() int {
	outData, err := exec.Command("/bin/sh", "-c", "lscpu | grep \"Core(s) per socket:\" | cut -d \":\" -f 2").Output()
	framework.ExpectNoError(err)

	coreCount, err := strconv.Atoi(strings.TrimSpace(string(outData)))
	framework.ExpectNoError(err)

	return coreCount
}

func detectSockets() int {
	outData, err := exec.Command("/bin/sh", "-c", "lscpu | grep \"Socket(s):\" | cut -d \":\" -f 2").Output()
	framework.ExpectNoError(err)

	socketCount, err := strconv.Atoi(strings.TrimSpace(string(outData)))
	framework.ExpectNoError(err)

	return socketCount
}

func detectThreadPerCore() int {
	outData, err := exec.Command("/bin/sh", "-c", "lscpu | grep \"Thread(s) per core:\" | cut -d \":\" -f 2").Output()
	framework.ExpectNoError(err)

	threadCount, err := strconv.Atoi(strings.TrimSpace(string(outData)))
	framework.ExpectNoError(err)

	return threadCount
}

// 目前只查找距离最小的节点对，并假设距离表是对称的。
func getMinRemoteDistanceForNode(nodeToDistances map[int][]int) int {
	var minDistance int = 99
	for myID, distances := range nodeToDistances {
		if len(distances) == 0 {
			continue
		}
		for nodeID, distance := range distances {
			// 自身距离恒为 10，表示本地访问。
			if myID == nodeID {
				continue
			}
			if distance < minDistance {
				minDistance = distance
			}
		}
	}

	return minDistance
}

func detectNUMADistances(numaNodes int) map[int][]int {
	ginkgo.GinkgoHelper()

	nodeToDistances := make(map[int][]int)
	for i := range numaNodes {
		outData, err := os.ReadFile(fmt.Sprintf("/sys/devices/system/node/node%d/distance", i))
		framework.ExpectNoError(err)

		nodeToDistances[i] = make([]int, 0, numaNodes)

		for _, distance := range strings.Split(strings.TrimSpace(string(outData)), " ") {
			distanceValue, err := strconv.Atoi(strings.TrimSpace(distance))
			framework.ExpectNoError(err)

			nodeToDistances[i] = append(nodeToDistances[i], distanceValue)
		}
	}

	return nodeToDistances
}

func makeContainers(ctnCmd string, ctnAttributes []tmCtnAttribute) (ctns []v1.Container) {
	for _, ctnAttr := range ctnAttributes {
		ctn := v1.Container{
			Name:  ctnAttr.ctnName,
			Image: busyboxImage,
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceCPU):    resource.MustParse(ctnAttr.cpuRequest),
					v1.ResourceName(v1.ResourceMemory): resource.MustParse("100Mi"),
				},
				Limits: v1.ResourceList{
					v1.ResourceName(v1.ResourceCPU):    resource.MustParse(ctnAttr.cpuLimit),
					v1.ResourceName(v1.ResourceMemory): resource.MustParse("100Mi"),
				},
			},
			Command:       []string{"sh", "-c", ctnCmd},
			RestartPolicy: ctnAttr.restartPolicy,
		}
		if ctnAttr.deviceName != "" {
			ctn.Resources.Requests[v1.ResourceName(ctnAttr.deviceName)] = resource.MustParse(ctnAttr.deviceRequest)
			ctn.Resources.Limits[v1.ResourceName(ctnAttr.deviceName)] = resource.MustParse(ctnAttr.deviceLimit)
		}
		ctns = append(ctns, ctn)
	}
	return
}

func makeTopologyManagerTestPod(podName string, tmCtnAttributes, tmInitCtnAttributes []tmCtnAttribute) *v1.Pod {
	var containers, initContainers []v1.Container
	for _, attr := range tmInitCtnAttributes {
		cmd := numaAlignmentCommand
		if attr.restartPolicy != nil && *attr.restartPolicy == v1.ContainerRestartPolicyAlways {
			cmd = numaAlignmentSleepCommand
		}
		initContainers = append(initContainers, makeContainers(cmd, []tmCtnAttribute{attr})...)
	}
	containers = makeContainers(numaAlignmentSleepCommand, tmCtnAttributes)

	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
		},
		Spec: v1.PodSpec{
			RestartPolicy:  v1.RestartPolicyNever,
			InitContainers: initContainers,
			Containers:     containers,
		},
	}
}

func findNUMANodeWithoutSRIOVDevicesFromConfigMap(configMap *v1.ConfigMap, numaNodes int) (int, bool) {
	for nodeNum := range numaNodes {
		value, ok := configMap.Annotations[fmt.Sprintf("pcidevice_node%d", nodeNum)]
		if !ok {
			framework.Logf("missing pcidevice annotation for NUMA node %d", nodeNum)
			return -1, false
		}
		v, err := strconv.Atoi(value)
		if err != nil {
			framework.Failf("error getting the PCI device count on NUMA node %d: %v", nodeNum, err)
		}
		if v == 0 {
			framework.Logf("NUMA node %d has no SRIOV devices attached", nodeNum)
			return nodeNum, true
		}
		framework.Logf("NUMA node %d has %d SRIOV devices attached", nodeNum, v)
	}
	return -1, false
}

func findNUMANodeWithoutSRIOVDevicesFromSysfs(numaNodes int) (int, bool) {
	pciDevs, err := getPCIDeviceInfo("/sys/bus/pci/devices")
	if err != nil {
		framework.Failf("error detecting the PCI device NUMA node: %v", err)
	}

	pciPerNuma := make(map[int]int)
	for _, pciDev := range pciDevs {
		if pciDev.IsVFn {
			pciPerNuma[pciDev.NUMANode]++
		}
	}

	if len(pciPerNuma) == 0 {
		framework.Logf("failed to find any VF device from %v", pciDevs)
		return -1, false
	}

	for nodeNum := range numaNodes {
		v := pciPerNuma[nodeNum]
		if v == 0 {
			framework.Logf("NUMA node %d has no SRIOV devices attached", nodeNum)
			return nodeNum, true
		}
		framework.Logf("NUMA node %d has %d SRIOV devices attached", nodeNum, v)
	}
	return -1, false
}

func findNUMANodeWithoutSRIOVDevices(configMap *v1.ConfigMap, numaNodes int) (int, bool) {
	// 如果有人为 configMap 添加了注解，就使用其中的信息。
	if nodeNum, found := findNUMANodeWithoutSRIOVDevicesFromConfigMap(configMap, numaNodes); found {
		return nodeNum, found
	}
	// 没有注解时尝试自动探测。
	// 注意：这里假设机器上的所有 VF 都可用于测试。
	return findNUMANodeWithoutSRIOVDevicesFromSysfs(numaNodes)
}

func configureTopologyManagerInKubelet(oldCfg *kubeletconfig.KubeletConfiguration, policy, scope string, topologyOptions map[string]string, configMap *v1.ConfigMap, numaNodes int) (*kubeletconfig.KubeletConfiguration, string) {
	// 按指定 policy 配置 kubelet 中的 Topology Manager。
	newCfg := oldCfg.DeepCopy()
	if newCfg.FeatureGates == nil {
		newCfg.FeatureGates = make(map[string]bool)
	}

	if topologyOptions != nil {
		newCfg.TopologyManagerPolicyOptions = topologyOptions
	}

	// 设置 Topology Manager policy。
	newCfg.TopologyManagerPolicy = policy

	newCfg.TopologyManagerScope = scope

	// 将 CPU Manager policy 设为 static。
	newCfg.CPUManagerPolicy = string(cpumanager.PolicyStatic)

	// 将 CPU Manager 的 reconcile 周期设为 1 秒。
	newCfg.CPUManagerReconcilePeriod = metav1.Duration{Duration: 1 * time.Second}

	if nodeNum, ok := findNUMANodeWithoutSRIOVDevices(configMap, numaNodes); ok {
		cpus, err := getCPUsPerNUMANode(nodeNum)
		framework.Logf("NUMA Node %d doesn't seem to have attached SRIOV devices and has cpus=%v", nodeNum, cpus)
		framework.ExpectNoError(err)
		newCfg.ReservedSystemCPUs = fmt.Sprintf("%d", cpus[len(cpus)-1])
	} else {
		// 启用 CPU Manager 时，如果没有设置 kube-reserved 或 system-reserved，
		// kubelet 会 panic。将 kube-reserved 中的 cpu 设为大于 0 以避免 panic。
		if newCfg.KubeReserved == nil {
			newCfg.KubeReserved = map[string]string{}
		}

		if _, ok := newCfg.KubeReserved["cpu"]; !ok {
			newCfg.KubeReserved["cpu"] = "200m"
		}
	}
	// 打印配置，便于调试。
	framework.Logf("New kubelet config is %s", newCfg.String())

	return newCfg, newCfg.ReservedSystemCPUs
}

// getSRIOVDevicePluginPod 返回 e2e 测试中用于 sriov 资源的 Device Plugin Pod。
func getSRIOVDevicePluginPod() *v1.Pod {
	data, err := e2etestfiles.Read(SRIOVDevicePluginDSYAML)
	if err != nil {
		framework.Fail(err.Error())
	}

	ds := readDaemonSetV1OrDie(data)
	p := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      SRIOVDevicePluginName,
			Namespace: metav1.NamespaceSystem,
		},

		Spec: ds.Spec.Template.Spec,
	}

	return p
}

func readConfigMapV1OrDie(objBytes []byte) *v1.ConfigMap {
	v1.AddToScheme(appsScheme)
	requiredObj, err := runtime.Decode(appsCodecs.UniversalDecoder(v1.SchemeGroupVersion), objBytes)
	if err != nil {
		panic(err)
	}
	return requiredObj.(*v1.ConfigMap)
}

func readServiceAccountV1OrDie(objBytes []byte) *v1.ServiceAccount {
	v1.AddToScheme(appsScheme)
	requiredObj, err := runtime.Decode(appsCodecs.UniversalDecoder(v1.SchemeGroupVersion), objBytes)
	if err != nil {
		panic(err)
	}
	return requiredObj.(*v1.ServiceAccount)
}

func findSRIOVResource(node *v1.Node) (string, int64) {
	framework.Logf("Node status allocatable: %v", node.Status.Allocatable)
	re := regexp.MustCompile(`^intel.com/.*sriov.*`)
	for key, val := range node.Status.Allocatable {
		resource := string(key)
		if re.MatchString(resource) {
			v := val.Value()
			if v > 0 {
				return resource, v
			}
		}
	}
	return "", 0
}

func validatePodAlignment(ctx context.Context, f *framework.Framework, pod *v1.Pod, envInfo *testEnvInfo) {
	for _, cnt := range pod.Spec.InitContainers {
		// 只检查可重启的 init container，跳过普通 init container。
		if cnt.RestartPolicy == nil || *cnt.RestartPolicy != v1.ContainerRestartPolicyAlways {
			continue
		}

		ginkgo.By(fmt.Sprintf("validating the init container %s on Gu pod %s", cnt.Name, pod.Name))

		logs, err := e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, pod.Name, cnt.Name)
		framework.ExpectNoError(err, "expected log not found in init container [%s] of pod [%s]", cnt.Name, pod.Name)

		framework.Logf("got init container logs: %v", logs)
		numaRes, err := checkNUMAAlignment(f, pod, &cnt, logs, envInfo)
		framework.ExpectNoError(err, "NUMA Alignment check failed for init container [%s] of pod [%s]", cnt.Name, pod.Name)
		if numaRes != nil {
			framework.Logf("NUMA resources for init container %s/%s: %s", pod.Name, cnt.Name, numaRes.String())
		}
	}

	for _, cnt := range pod.Spec.Containers {
		ginkgo.By(fmt.Sprintf("validating the container %s on Gu pod %s", cnt.Name, pod.Name))

		logs, err := e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, pod.Name, cnt.Name)
		framework.ExpectNoError(err, "expected log not found in container [%s] of pod [%s]", cnt.Name, pod.Name)

		framework.Logf("got pod logs: %v", logs)
		numaRes, err := checkNUMAAlignment(f, pod, &cnt, logs, envInfo)
		framework.ExpectNoError(err, "NUMA Alignment check failed for [%s] of pod [%s]", cnt.Name, pod.Name)
		if numaRes != nil {
			framework.Logf("NUMA resources for %s/%s: %s", pod.Name, cnt.Name, numaRes.String())
		}
	}
}

// validatePodAligmentWithPodScope 验证 Pod 的所有 CPU 是否都亲和到同一个 NUMA 节点。
func validatePodAlignmentWithPodScope(ctx context.Context, f *framework.Framework, pod *v1.Pod, envInfo *testEnvInfo) error {
	// CPU ID 到 NUMA 节点 ID 的映射。
	podsNUMA := make(map[int]int)

	ginkgo.By(fmt.Sprintf("validate pod scope alignment for %s pod", pod.Name))
	for _, cnt := range pod.Spec.InitContainers {
		// 只检查可重启的 init container，跳过普通 init container。
		if cnt.RestartPolicy == nil || *cnt.RestartPolicy != v1.ContainerRestartPolicyAlways {
			continue
		}

		logs, err := e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, pod.Name, cnt.Name)
		framework.ExpectNoError(err, "NUMA alignment failed for init container [%s] of pod [%s]", cnt.Name, pod.Name)
		envMap, err := makeEnvMap(logs)
		framework.ExpectNoError(err, "NUMA alignment failed for init container [%s] of pod [%s]", cnt.Name, pod.Name)
		cpuToNUMA, err := getCPUToNUMANodeMapFromEnv(f, pod, &cnt, envMap, envInfo.numaNodes)
		framework.ExpectNoError(err, "NUMA alignment failed for init container [%s] of pod [%s]", cnt.Name, pod.Name)
		for cpuID, numaID := range cpuToNUMA {
			podsNUMA[cpuID] = numaID
		}
	}

	for _, cnt := range pod.Spec.Containers {
		logs, err := e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, pod.Name, cnt.Name)
		framework.ExpectNoError(err, "NUMA alignment failed for container [%s] of pod [%s]", cnt.Name, pod.Name)
		envMap, err := makeEnvMap(logs)
		framework.ExpectNoError(err, "NUMA alignment failed for container [%s] of pod [%s]", cnt.Name, pod.Name)
		cpuToNUMA, err := getCPUToNUMANodeMapFromEnv(f, pod, &cnt, envMap, envInfo.numaNodes)
		framework.ExpectNoError(err, "NUMA alignment failed for container [%s] of pod [%s]", cnt.Name, pod.Name)
		for cpuID, numaID := range cpuToNUMA {
			podsNUMA[cpuID] = numaID
		}
	}

	numaRes := numaPodResources{
		CPUToNUMANode: podsNUMA,
	}
	aligned := numaRes.CheckAlignment()
	if !aligned {
		return fmt.Errorf("resources were assigned from different NUMA nodes")
	}

	framework.Logf("NUMA locality confirmed: all pod's CPUs aligned to the same NUMA node")
	return nil
}

func runTopologyManagerPolicySuiteTests(ctx context.Context, f *framework.Framework) {
	var cpuCap, cpuAlloc int64

	cpuCap, cpuAlloc, _ = getLocalNodeCPUDetails(ctx, f)
	ginkgo.By(fmt.Sprintf("checking node CPU capacity (%d) and allocatable CPUs (%d)", cpuCap, cpuAlloc))

	// 即使最弱的 CI 机器通常也有 2 个 CPU，这里仍显式检查以示谨慎。
	// 宁可跳过测试，也不愿出现误报（测试失败）。
	if cpuAlloc < 1 {
		e2eskipper.Skipf("Skipping basic CPU Manager tests since CPU capacity < 2")
	}

	ginkgo.By("running a non-Gu pod")
	runNonGuPodTest(ctx, f, cpuCap, cpuset.New())

	ginkgo.By("running a Gu pod")
	runGuPodTest(ctx, f, 1, cpuset.New())

	// 如果可分配的 CPU 少于 3 个，跳过其余测试。
	if cpuAlloc < 3 {
		e2eskipper.Skipf("Skipping rest of the CPU Manager tests since CPU capacity < 3")
	}

	ginkgo.By("running multiple Gu and non-Gu pods")
	runMultipleGuNonGuPods(ctx, f, cpuCap, cpuAlloc)

	ginkgo.By("running a Gu pod requesting multiple CPUs")
	runMultipleCPUGuPod(ctx, f)

	ginkgo.By("running a Gu pod with multiple containers requesting integer CPUs")
	runMultipleCPUContainersGuPod(ctx, f)

	ginkgo.By("running multiple Gu pods")
	runMultipleGuPods(ctx, f)
}

func runTopologyManagerPositiveTest(ctx context.Context, f *framework.Framework, numPods int, ctnAttrs, initCtnAttrs []tmCtnAttribute, envInfo *testEnvInfo) {
	podMap := make(map[string]*v1.Pod)

	for podID := range numPods {
		podName := fmt.Sprintf("gu-pod-%d", podID)
		framework.Logf("creating pod %s attrs %v", podName, ctnAttrs)
		pod := makeTopologyManagerTestPod(podName, ctnAttrs, initCtnAttrs)
		pod = e2epod.NewPodClient(f).CreateSync(ctx, pod)
		framework.Logf("created pod %s", podName)
		podMap[podName] = pod
	}

	// 根据 https://github.com/kubernetes/enhancements/blob/master/keps/sig-node/693-topology-manager/README.md#multi-numa-systems-tests
	// 只有使用 single-numa node policy 时才能做有意义的校验。
	if envInfo.policy == topologymanager.PolicySingleNumaNode {
		for _, pod := range podMap {
			validatePodAlignment(ctx, f, pod, envInfo)
		}
		if envInfo.scope == podScopeTopology {
			for _, pod := range podMap {
				err := validatePodAlignmentWithPodScope(ctx, f, pod, envInfo)
				framework.ExpectNoError(err)
			}
		}
	}

	deletePodsAsync(ctx, f, podMap)
}

func runTopologyManagerNegativeTest(ctx context.Context, f *framework.Framework, ctnAttrs, initCtnAttrs []tmCtnAttribute, envInfo *testEnvInfo) {
	podName := "gu-pod"
	framework.Logf("creating pod %s attrs %v", podName, ctnAttrs)
	pod := makeTopologyManagerTestPod(podName, ctnAttrs, initCtnAttrs)

	pod = e2epod.NewPodClient(f).Create(ctx, pod)
	err := e2epod.WaitForPodCondition(ctx, f.ClientSet, f.Namespace.Name, pod.Name, "Failed", 30*time.Second, func(pod *v1.Pod) (bool, error) {
		if pod.Status.Phase != v1.PodPending {
			return true, nil
		}
		return false, nil
	})
	framework.ExpectNoError(err)
	pod, err = e2epod.NewPodClient(f).Get(ctx, pod.Name, metav1.GetOptions{})
	framework.ExpectNoError(err)

	if pod.Status.Phase != v1.PodFailed {
		framework.Failf("pod %s not failed: %v", pod.Name, pod.Status)
	}
	if !isTopologyAffinityError(pod) {
		framework.Failf("pod %s failed for wrong reason: %q", pod.Name, pod.Status.Reason)
	}

	deletePodSyncByName(ctx, f, pod.Name)
}

func isTopologyAffinityError(pod *v1.Pod) bool {
	re := regexp.MustCompile(`Topology.*Affinity.*Error`)
	return re.MatchString(pod.Status.Reason)
}

func getSRIOVDevicePluginConfigMap(cmFile string) *v1.ConfigMap {
	data, err := e2etestfiles.Read(SRIOVDevicePluginCMYAML)
	if err != nil {
		framework.Fail(err.Error())
	}

	// SRIOVDP 配置依赖硬件，因此允许针对每台测试主机单独定制。
	framework.Logf("host-local SRIOV Device Plugin Config Map %q", cmFile)
	if cmFile != "" {
		data, err = os.ReadFile(cmFile)
		if err != nil {
			framework.Failf("unable to load the SRIOV Device Plugin ConfigMap: %v", err)
		}
	} else {
		framework.Logf("Using built-in SRIOV Device Plugin Config Map")
	}

	return readConfigMapV1OrDie(data)
}

type sriovData struct {
	configMap      *v1.ConfigMap
	serviceAccount *v1.ServiceAccount
	pod            *v1.Pod

	resourceName   string
	resourceAmount int64
}

func setupSRIOVConfigOrFail(ctx context.Context, f *framework.Framework, configMap *v1.ConfigMap) *sriovData {
	sd := createSRIOVConfigOrFail(ctx, f, configMap)

	e2enode.WaitForNodeToBeReady(ctx, f.ClientSet, framework.TestContext.NodeName, 5*time.Minute)

	sd.pod = createSRIOVPodOrFail(ctx, f)
	return sd
}

func createSRIOVConfigOrFail(ctx context.Context, f *framework.Framework, configMap *v1.ConfigMap) *sriovData {
	var err error

	ginkgo.By(fmt.Sprintf("Creating configMap %v/%v", metav1.NamespaceSystem, configMap.Name))
	if _, err = f.ClientSet.CoreV1().ConfigMaps(metav1.NamespaceSystem).Create(ctx, configMap, metav1.CreateOptions{}); err != nil {
		framework.Failf("unable to create test configMap %s: %v", configMap.Name, err)
	}

	data, err := e2etestfiles.Read(SRIOVDevicePluginSAYAML)
	if err != nil {
		framework.Fail(err.Error())
	}
	serviceAccount := readServiceAccountV1OrDie(data)
	ginkgo.By(fmt.Sprintf("Creating serviceAccount %v/%v", metav1.NamespaceSystem, serviceAccount.Name))
	if _, err = f.ClientSet.CoreV1().ServiceAccounts(metav1.NamespaceSystem).Create(ctx, serviceAccount, metav1.CreateOptions{}); err != nil {
		framework.Failf("unable to create test serviceAccount %s: %v", serviceAccount.Name, err)
	}

	return &sriovData{
		configMap:      configMap,
		serviceAccount: serviceAccount,
	}
}

func createSRIOVPodOrFail(ctx context.Context, f *framework.Framework) *v1.Pod {
	dp := getSRIOVDevicePluginPod()
	dp.Spec.NodeName = framework.TestContext.NodeName

	ginkgo.By("Create SRIOV device plugin pod")
	dpPod, err := f.ClientSet.CoreV1().Pods(metav1.NamespaceSystem).Create(ctx, dp, metav1.CreateOptions{})
	framework.ExpectNoError(err)

	if err = e2epod.WaitForPodCondition(ctx, f.ClientSet, metav1.NamespaceSystem, dp.Name, "Ready", 120*time.Second, testutils.PodRunningReady); err != nil {
		framework.Logf("SRIOV Pod %v took too long to enter running/ready: %v", dp.Name, err)
	}
	framework.ExpectNoError(err)

	return dpPod
}

// waitForSRIOVResources 等待足够的 SRIOV 资源可用，预期在超时时间内完成。
// 成功退出时会将找到的资源写入 sriovData。
func waitForSRIOVResources(ctx context.Context, f *framework.Framework, sd *sriovData) {
	sriovResourceName := ""
	var sriovResourceAmount int64
	ginkgo.By("Waiting for devices to become available on the local node")
	gomega.Eventually(ctx, func(ctx context.Context) bool {
		node := getLocalNode(ctx, f)
		sriovResourceName, sriovResourceAmount = findSRIOVResource(node)
		return sriovResourceAmount > minSriovResource
	}, 2*time.Minute, framework.Poll).Should(gomega.BeTrueBecause("expected SRIOV resources to be available within the timout"))

	sd.resourceName = sriovResourceName
	sd.resourceAmount = sriovResourceAmount
	framework.Logf("Detected SRIOV allocatable devices name=%q amount=%d", sd.resourceName, sd.resourceAmount)
}

func deleteSRIOVPodOrFail(ctx context.Context, f *framework.Framework, sd *sriovData) {
	var err error
	gp := int64(0)
	deleteOptions := metav1.DeleteOptions{
		GracePeriodSeconds: &gp,
	}

	ginkgo.By(fmt.Sprintf("Delete SRIOV device plugin pod %s/%s", sd.pod.Namespace, sd.pod.Name))
	err = f.ClientSet.CoreV1().Pods(sd.pod.Namespace).Delete(ctx, sd.pod.Name, deleteOptions)
	framework.ExpectNoError(err)
	waitForAllContainerRemoval(ctx, sd.pod.Name, sd.pod.Namespace)
}

func removeSRIOVConfigOrFail(ctx context.Context, f *framework.Framework, sd *sriovData) {
	var err error
	gp := int64(0)
	deleteOptions := metav1.DeleteOptions{
		GracePeriodSeconds: &gp,
	}

	ginkgo.By(fmt.Sprintf("Deleting configMap %v/%v", metav1.NamespaceSystem, sd.configMap.Name))
	err = f.ClientSet.CoreV1().ConfigMaps(metav1.NamespaceSystem).Delete(ctx, sd.configMap.Name, deleteOptions)
	framework.ExpectNoError(err)

	ginkgo.By(fmt.Sprintf("Deleting serviceAccount %v/%v", metav1.NamespaceSystem, sd.serviceAccount.Name))
	err = f.ClientSet.CoreV1().ServiceAccounts(metav1.NamespaceSystem).Delete(ctx, sd.serviceAccount.Name, deleteOptions)
	framework.ExpectNoError(err)
}

func teardownSRIOVConfigOrFail(ctx context.Context, f *framework.Framework, sd *sriovData) {
	deleteSRIOVPodOrFail(ctx, f, sd)
	removeSRIOVConfigOrFail(ctx, f, sd)
}

func runTMScopeResourceAlignmentTestSuite(ctx context.Context, f *framework.Framework, configMap *v1.ConfigMap, reservedSystemCPUs, policy string, numaNodes, coreCount int) {
	smtLevel := smtLevelFromSysFS()
	sd := setupSRIOVConfigOrFail(ctx, f, configMap)
	ginkgo.DeferCleanup(teardownSRIOVConfigOrFail, f, sd)
	var ctnAttrs, initCtnAttrs []tmCtnAttribute

	waitForSRIOVResources(ctx, f, sd)

	envInfo := &testEnvInfo{
		numaNodes:         numaNodes,
		sriovResourceName: sd.resourceName,
		policy:            policy,
		scope:             podScopeTopology,
	}

	ginkgo.By(fmt.Sprintf("Admit two guaranteed pods. Both consist of 2 containers, each container with 1 CPU core. Use 1 %s device.", sd.resourceName))
	ctnAttrs = []tmCtnAttribute{
		{
			ctnName:       "ps-container-0",
			cpuRequest:    "1000m",
			cpuLimit:      "1000m",
			deviceName:    sd.resourceName,
			deviceRequest: "1",
			deviceLimit:   "1",
		},
		{
			ctnName:       "ps-container-1",
			cpuRequest:    "1000m",
			cpuLimit:      "1000m",
			deviceName:    sd.resourceName,
			deviceRequest: "1",
			deviceLimit:   "1",
		},
	}
	runTopologyManagerPositiveTest(ctx, f, 2, ctnAttrs, initCtnAttrs, envInfo)

	numCores := smtLevel * coreCount
	coresReq := fmt.Sprintf("%dm", numCores*1000)
	ginkgo.By(fmt.Sprintf("Admit a guaranteed pod requesting %d CPU cores, i.e., more than can be provided at every single NUMA node. Therefore, the request should be rejected.", numCores+1))
	ctnAttrs = []tmCtnAttribute{
		{
			ctnName:       "gu-container-1",
			cpuRequest:    coresReq,
			cpuLimit:      coresReq,
			deviceRequest: "1",
			deviceLimit:   "1",
		},
		{
			ctnName:       "gu-container-2",
			cpuRequest:    "1000m",
			cpuLimit:      "1000m",
			deviceRequest: "1",
			deviceLimit:   "1",
		},
	}
	runTopologyManagerNegativeTest(ctx, f, ctnAttrs, initCtnAttrs, envInfo)

	// pod scope 下的 Topology Manager 应根据两项请求计算准入 Pod 所需的 CPU 数：
	// init container 的 CPU 需求最大值，以及应用 container 的 CPU 请求总和。
	// Topology Manager 应取两者中较大的值。因此，下面测试用例中的两个 Pod 都应获得
	// 需求最大的 init container 所请求的 CPU 数量。由于每个 Pod 的 CPU 需求略高于
	// 单个节点可用资源的一半，两个 Pod 应被放置在不同的 NUMA 节点上。
	coresReq = fmt.Sprintf("%dm", (numCores/2+1)*1000)
	ginkgo.By(fmt.Sprintf("Admit two guaranteed pods, each pod requests %d cores - the pods should be placed on different NUMA nodes", numCores/2+1))
	initCtnAttrs = []tmCtnAttribute{
		{
			ctnName:       "init-container-1",
			cpuRequest:    coresReq,
			cpuLimit:      coresReq,
			deviceRequest: "1",
			deviceLimit:   "1",
		},
		{
			ctnName:       "init-container-2",
			cpuRequest:    "1000m",
			cpuLimit:      "1000m",
			deviceRequest: "1",
			deviceLimit:   "1",
		},
	}
	ctnAttrs = []tmCtnAttribute{
		{
			ctnName:       "gu-container-0",
			cpuRequest:    "1000m",
			cpuLimit:      "1000m",
			deviceRequest: "1",
			deviceLimit:   "1",
		},
		{
			ctnName:       "gu-container-1",
			cpuRequest:    "1000m",
			cpuLimit:      "1000m",
			deviceRequest: "1",
			deviceLimit:   "1",
		},
	}
	runTopologyManagerPositiveTest(ctx, f, 2, ctnAttrs, initCtnAttrs, envInfo)

	ginkgo.By(fmt.Sprintf("Admit one guaranteed pod with restartable init container, 1 core and 1 %s device", sd.resourceName))
	initCtnAttrs = []tmCtnAttribute{
		{
			ctnName:       "restartable-init-container",
			cpuRequest:    "1000m",
			cpuLimit:      "1000m",
			deviceName:    sd.resourceName,
			deviceRequest: "1",
			deviceLimit:   "1",
			restartPolicy: &containerRestartPolicyAlways,
		},
	}
	ctnAttrs = []tmCtnAttribute{
		{
			ctnName:       "gu-container",
			cpuRequest:    "1000m",
			cpuLimit:      "1000m",
			deviceName:    sd.resourceName,
			deviceRequest: "1",
			deviceLimit:   "1",
		},
	}
	runTopologyManagerPositiveTest(ctx, f, 1, ctnAttrs, initCtnAttrs, envInfo)

	ginkgo.By(fmt.Sprintf("Admit one guaranteed pod with multiple restartable init containers, each container with 1 CPU core. Use 1 %s device", sd.resourceName))
	initCtnAttrs = []tmCtnAttribute{
		{
			ctnName:       "restartable-init-container-1",
			cpuRequest:    "1000m",
			cpuLimit:      "1000m",
			deviceName:    sd.resourceName,
			deviceRequest: "1",
			deviceLimit:   "1",
			restartPolicy: &containerRestartPolicyAlways,
		},
		{
			ctnName:       "restartable-init-container-2",
			cpuRequest:    "1000m",
			cpuLimit:      "1000m",
			deviceName:    sd.resourceName,
			deviceRequest: "1",
			deviceLimit:   "1",
			restartPolicy: &containerRestartPolicyAlways,
		},
	}
	ctnAttrs = []tmCtnAttribute{
		{
			ctnName:       "gu-container",
			cpuRequest:    "1000m",
			cpuLimit:      "1000m",
			deviceName:    sd.resourceName,
			deviceRequest: "1",
			deviceLimit:   "1",
		},
	}
	runTopologyManagerPositiveTest(ctx, f, 1, ctnAttrs, initCtnAttrs, envInfo)

	coresReq = fmt.Sprintf("%dm", (numCores/2+1)*1000)
	ginkgo.By(fmt.Sprintf("Trying to admin guaranteed pod with two restartable init containers where sum of their CPU requests (%d cores) exceeds NUMA capacity. The request should be rejected", (numCores/2+1)*2))
	initCtnAttrs = []tmCtnAttribute{
		{
			ctnName:       "restartable-init-container-1",
			cpuRequest:    coresReq,
			cpuLimit:      coresReq,
			deviceRequest: "1",
			deviceLimit:   "1",
			restartPolicy: &containerRestartPolicyAlways,
		},
		{
			ctnName:       "restartable-init-container-2",
			cpuRequest:    coresReq,
			cpuLimit:      coresReq,
			deviceRequest: "1",
			deviceLimit:   "1",
			restartPolicy: &containerRestartPolicyAlways,
		},
	}
	ctnAttrs = []tmCtnAttribute{
		{
			ctnName:       "gu-container",
			cpuRequest:    "1000m",
			cpuLimit:      "1000m",
			deviceRequest: "1",
			deviceLimit:   "1",
		},
	}
	runTopologyManagerNegativeTest(ctx, f, ctnAttrs, initCtnAttrs, envInfo)
}

func runTopologyManagerNodeAlignmentSuiteTests(ctx context.Context, f *framework.Framework, sd *sriovData, reservedSystemCPUs, policy string, numaNodes, coreCount int) {
	smtLevel := smtLevelFromSysFS()

	waitForSRIOVResources(ctx, f, sd)

	envInfo := &testEnvInfo{
		numaNodes:         numaNodes,
		sriovResourceName: sd.resourceName,
		policy:            policy,
	}

	// 本来可以用循环，这里将其展开以便说明各个测试用例。
	var ctnAttrs, initCtnAttrs []tmCtnAttribute

	// 最简单的情况。
	ginkgo.By(fmt.Sprintf("Successfully admit one guaranteed pod with 1 core, 1 %s device", sd.resourceName))
	ctnAttrs = []tmCtnAttribute{
		{
			ctnName:       "gu-container",
			cpuRequest:    "1000m",
			cpuLimit:      "1000m",
			deviceName:    sd.resourceName,
			deviceRequest: "1",
			deviceLimit:   "1",
		},
	}
	runTopologyManagerPositiveTest(ctx, f, 1, ctnAttrs, initCtnAttrs, envInfo)

	ginkgo.By(fmt.Sprintf("Successfully admit one guaranteed pod with 2 cores, 1 %s device", sd.resourceName))
	ctnAttrs = []tmCtnAttribute{
		{
			ctnName:       "gu-container",
			cpuRequest:    "2000m",
			cpuLimit:      "2000m",
			deviceName:    sd.resourceName,
			deviceRequest: "1",
			deviceLimit:   "1",
		},
	}
	runTopologyManagerPositiveTest(ctx, f, 1, ctnAttrs, initCtnAttrs, envInfo)

	if reservedSystemCPUs != "" {
		// 为避免误判，保留 CPU 的选择方式是：至少保证有一个 NUMA 节点
		// 拥有 1 个以上 SRIOV 设备且没有保留 CPU。
		numCores := smtLevel * coreCount
		allCoresReq := fmt.Sprintf("%dm", numCores*1000)
		ginkgo.By(fmt.Sprintf("Successfully admit an entire socket (%d cores), 1 %s device", numCores, sd.resourceName))
		ctnAttrs = []tmCtnAttribute{
			{
				ctnName:       "gu-container",
				cpuRequest:    allCoresReq,
				cpuLimit:      allCoresReq,
				deviceName:    sd.resourceName,
				deviceRequest: "1",
				deviceLimit:   "1",
			},
		}
		runTopologyManagerPositiveTest(ctx, f, 1, ctnAttrs, initCtnAttrs, envInfo)
	}

	if sd.resourceAmount > 1 {
		// 无论总线如何连接到 NUMA 节点、SRIOV 设备如何安装，
		// 本函数的前置条件必须保证下列场景可以成立。
		ginkgo.By(fmt.Sprintf("Successfully admit two guaranteed pods, each with 1 core, 1 %s device", sd.resourceName))
		ctnAttrs = []tmCtnAttribute{
			{
				ctnName:       "gu-container",
				cpuRequest:    "1000m",
				cpuLimit:      "1000m",
				deviceName:    sd.resourceName,
				deviceRequest: "1",
				deviceLimit:   "1",
			},
		}
		runTopologyManagerPositiveTest(ctx, f, 2, ctnAttrs, initCtnAttrs, envInfo)

		ginkgo.By(fmt.Sprintf("Successfully admit two guaranteed pods, each with 2 cores, 1 %s device", sd.resourceName))
		ctnAttrs = []tmCtnAttribute{
			{
				ctnName:       "gu-container",
				cpuRequest:    "2000m",
				cpuLimit:      "2000m",
				deviceName:    sd.resourceName,
				deviceRequest: "1",
				deviceLimit:   "1",
			},
		}
		runTopologyManagerPositiveTest(ctx, f, 2, ctnAttrs, initCtnAttrs, envInfo)

		ginkgo.By(fmt.Sprintf("Successfully admit one guaranteed pod with restartable init container - each with 1 core, 1 %s device", sd.resourceName))
		initCtnAttrs = []tmCtnAttribute{
			{
				ctnName:       "restartable-init-container",
				cpuRequest:    "1000m",
				cpuLimit:      "1000m",
				deviceName:    sd.resourceName,
				deviceRequest: "1",
				deviceLimit:   "1",
				restartPolicy: &containerRestartPolicyAlways,
			},
		}
		ctnAttrs = []tmCtnAttribute{
			{
				ctnName:       "gu-container",
				cpuRequest:    "1000m",
				cpuLimit:      "1000m",
				deviceName:    sd.resourceName,
				deviceRequest: "1",
				deviceLimit:   "1",
			},
		}
		runTopologyManagerPositiveTest(ctx, f, 1, ctnAttrs, initCtnAttrs, envInfo)

		// 测试更复杂的条件需要了解系统的 CPU 与总线拓扑。
	}

	// 多 container 测试。
	if sd.resourceAmount >= 4 {
		ginkgo.By(fmt.Sprintf("Successfully admit a guaranteed pod requesting for two containers, each with 2 cores, 1 %s device", sd.resourceName))
		ctnAttrs = []tmCtnAttribute{
			{
				ctnName:       "gu-container-0",
				cpuRequest:    "2000m",
				cpuLimit:      "2000m",
				deviceName:    sd.resourceName,
				deviceRequest: "1",
				deviceLimit:   "1",
			},
			{
				ctnName:       "gu-container-1",
				cpuRequest:    "2000m",
				cpuLimit:      "2000m",
				deviceName:    sd.resourceName,
				deviceRequest: "1",
				deviceLimit:   "1",
			},
		}
		runTopologyManagerPositiveTest(ctx, f, 1, ctnAttrs, initCtnAttrs, envInfo)

		ginkgo.By(fmt.Sprintf("Successfully admit two guaranteed pods, each with two containers, each with 1 core, 1 %s device", sd.resourceName))
		ctnAttrs = []tmCtnAttribute{
			{
				ctnName:       "gu-container-0",
				cpuRequest:    "1000m",
				cpuLimit:      "1000m",
				deviceName:    sd.resourceName,
				deviceRequest: "1",
				deviceLimit:   "1",
			},
			{
				ctnName:       "gu-container-1",
				cpuRequest:    "1000m",
				cpuLimit:      "1000m",
				deviceName:    sd.resourceName,
				deviceRequest: "1",
				deviceLimit:   "1",
			},
		}
		runTopologyManagerPositiveTest(ctx, f, 2, ctnAttrs, initCtnAttrs, envInfo)

		ginkgo.By(fmt.Sprintf("Successfully admit two guaranteed pods, each with two containers, both with with 2 cores, one with 1 %s device", sd.resourceName))
		ctnAttrs = []tmCtnAttribute{
			{
				ctnName:       "gu-container-dev",
				cpuRequest:    "2000m",
				cpuLimit:      "2000m",
				deviceName:    sd.resourceName,
				deviceRequest: "1",
				deviceLimit:   "1",
			},
			{
				ctnName:    "gu-container-nodev",
				cpuRequest: "2000m",
				cpuLimit:   "2000m",
			},
		}
		runTopologyManagerPositiveTest(ctx, f, 2, ctnAttrs, initCtnAttrs, envInfo)

		ginkgo.By(fmt.Sprintf("Successfully admit pod with multiple restartable init containers, each with 1 core, 1 %s device", sd.resourceName))
		initCtnAttrs = []tmCtnAttribute{
			{
				ctnName:       "restartable-init-container-1",
				cpuRequest:    "1000m",
				cpuLimit:      "1000m",
				deviceName:    sd.resourceName,
				deviceRequest: "1",
				deviceLimit:   "1",
				restartPolicy: &containerRestartPolicyAlways,
			},
			{
				ctnName:       "restartable-init-container-2",
				cpuRequest:    "1000m",
				cpuLimit:      "1000m",
				deviceName:    sd.resourceName,
				deviceRequest: "1",
				deviceLimit:   "1",
				restartPolicy: &containerRestartPolicyAlways,
			},
		}
		ctnAttrs = []tmCtnAttribute{
			{
				ctnName:       "gu-container",
				cpuRequest:    "1000m",
				cpuLimit:      "1000m",
				deviceName:    sd.resourceName,
				deviceRequest: "1",
				deviceLimit:   "1",
			},
		}
		runTopologyManagerPositiveTest(ctx, f, 1, ctnAttrs, initCtnAttrs, envInfo)
	}

	// 这是唯一能够可靠保证拒绝请求的 policy。
	if policy == topologymanager.PolicySingleNumaNode {
		// 超出 NUMA 节点容量：核心数。
		numCores := 1 + (smtLevel * coreCount)
		excessCoresReq := fmt.Sprintf("%dm", numCores*1000)
		ginkgo.By(fmt.Sprintf("Trying to admit a guaranteed pods, with %d cores, 1 %s device - and it should be rejected", numCores, sd.resourceName))
		ctnAttrs = []tmCtnAttribute{
			{
				ctnName:       "gu-container",
				cpuRequest:    excessCoresReq,
				cpuLimit:      excessCoresReq,
				deviceName:    sd.resourceName,
				deviceRequest: "1",
				deviceLimit:   "1",
			},
		}
		runTopologyManagerNegativeTest(ctx, f, ctnAttrs, initCtnAttrs, envInfo)

		if sd.resourceAmount >= 3 {
			ginkgo.By(fmt.Sprintf("Trying to admit a guaranteed pod with a restartable init container demanding %d cores, 1 %s device - and it should be rejected", numCores, sd.resourceName))
			initCtnAttrs = []tmCtnAttribute{
				{
					ctnName:       "restartable-init-container",
					cpuRequest:    excessCoresReq,
					cpuLimit:      excessCoresReq,
					deviceName:    sd.resourceName,
					deviceRequest: "1",
					deviceLimit:   "1",
					restartPolicy: &containerRestartPolicyAlways,
				},
			}
			ctnAttrs = []tmCtnAttribute{
				{
					ctnName:       "gu-container",
					cpuRequest:    "1000m",
					cpuLimit:      "1000m",
					deviceName:    sd.resourceName,
					deviceRequest: "1",
					deviceLimit:   "1",
				},
			}
			runTopologyManagerNegativeTest(ctx, f, ctnAttrs, initCtnAttrs, envInfo)

			ginkgo.By("Trying to admit a guaranteed pod with two restartable init containers where the second one cannot achieve NUMA alignment - and it should be rejected")
			initCtnAttrs = []tmCtnAttribute{
				{
					ctnName:       "restartable-init-container-1",
					cpuRequest:    "1000m",
					cpuLimit:      "1000m",
					deviceName:    sd.resourceName,
					deviceRequest: "1",
					deviceLimit:   "1",
					restartPolicy: &containerRestartPolicyAlways,
				},
				{
					ctnName:       "restartable-init-container-2",
					cpuRequest:    excessCoresReq,
					cpuLimit:      excessCoresReq,
					deviceName:    sd.resourceName,
					deviceRequest: "1",
					deviceLimit:   "1",
					restartPolicy: &containerRestartPolicyAlways,
				},
			}
			ctnAttrs = []tmCtnAttribute{
				{
					ctnName:       "gu-container",
					cpuRequest:    "1000m",
					cpuLimit:      "1000m",
					deviceName:    sd.resourceName,
					deviceRequest: "1",
					deviceLimit:   "1",
				},
			}
			runTopologyManagerNegativeTest(ctx, f, ctnAttrs, initCtnAttrs, envInfo)
		}
	}
}

func runPreferClosestNUMATestSuite(ctx context.Context, f *framework.Framework, numaNodes int, distances map[int][]int) {
	runPreferClosestNUMAOptimalAllocationTest(ctx, f, numaNodes, distances)
	runPreferClosestNUMASubOptimalAllocationTest(ctx, f, numaNodes, distances)
}

func runPreferClosestNUMAOptimalAllocationTest(ctx context.Context, f *framework.Framework, numaNodes int, distances map[int][]int) {
	ginkgo.By("Admit two guaranteed pods. Both consist of 1 containers, each pod asks for cpus from 2 NUMA nodes. CPUs should be assigned from closest NUMA")
	podMap := make(map[string]*v1.Pod)
	for podID := range 2 {
		numCores := 0
		for nodeNum := 0 + 2*podID; nodeNum <= 1+2*podID; nodeNum++ {
			cpus, err := getCPUsPerNUMANode(nodeNum)
			framework.ExpectNoError(err)
			// 预留一个 CPU 给 reservedCPUs。只有在每个 NUMA 节点超过 2 个 CPU 时才成立。
			cpusPerNUMA := len(cpus)
			if cpusPerNUMA < 3 {
				e2eskipper.Skipf("Less than 3 cpus per NUMA node on this system. Skipping test.")
			}
			numCores += cpusPerNUMA - 1
		}
		coresReq := fmt.Sprintf("%dm", numCores*1000)
		ctnAttrs := []tmCtnAttribute{
			{
				ctnName:    "ps-container-0",
				cpuRequest: coresReq,
				cpuLimit:   coresReq,
			},
		}
		podName := fmt.Sprintf("gu-pod-%d", podID)
		framework.Logf("creating pod %s attrs %v", podName, nil)
		pod := makeTopologyManagerTestPod(podName, ctnAttrs, nil)
		pod = e2epod.NewPodClient(f).CreateSync(ctx, pod)
		framework.Logf("created pod %s", podName)
		podMap[podName] = pod
	}

	valiidatePreferClosestNUMAOptimalAllocation(ctx, f, podMap, numaNodes, distances)

	deletePodsAsync(ctx, f, podMap)
}

func runPreferClosestNUMASubOptimalAllocationTest(ctx context.Context, f *framework.Framework, numaNodes int, distances map[int][]int) {
	ginkgo.By("Admit two guaranteed pods. Both consist of 1 containers, each pod asks for cpus from 2 NUMA nodes. CPUs should be assigned from closest NUMA")
	cntName := "ps-container-0"

	// 假设每个 NUMA 节点的 CPU 数量相同。
	cpusPerNUMA, err := getCPUsPerNUMANode(0)
	framework.ExpectNoError(err)
	if len(cpusPerNUMA) < 5 {
		e2eskipper.Skipf("Less than 5 cpus per NUMA node on this system. Skipping test.")
	}
	podMap := make(map[string]*v1.Pod)
	for podID := range 2 {
		// 从不足一半数量的 NUMA 节点中申请除一个之外的所有 CPU，另一半节点各申请一半，
		// 再加上不足一半的节点数，以容纳 reserved CPU。
		numCores := ((numaNodes/2)-1)*(len(cpusPerNUMA)-1) + (len(cpusPerNUMA) / 2) + (numaNodes/2 - 1)
		framework.ExpectNoError(err)

		coresReq := fmt.Sprintf("%dm", numCores*1000)
		ctnAttrs := []tmCtnAttribute{
			{
				ctnName:    "ps-container-0",
				cpuRequest: coresReq,
				cpuLimit:   coresReq,
			},
		}
		podName := fmt.Sprintf("gu-pod-%d", podID)
		framework.Logf("creating pod %s", podName)
		pod := makeTopologyManagerTestPod(podName, ctnAttrs, nil)
		pod = e2epod.NewPodClient(f).CreateSync(ctx, pod)
		framework.Logf("created pod %s", podName)
		podMap[podName] = pod
	}

	valiidatePreferClosestNUMAOptimalAllocation(ctx, f, podMap, numaNodes, distances)

	ginkgo.By("Admit one guaranteed pod. Asks for cpus from 2 NUMA nodes. CPUs should be assigned from non closest NUMA")
	// 申请剩余的 CPU，此时只能采用次优的 NUMA 放置。
	coresReq := fmt.Sprintf("%dm", 2*(len(cpusPerNUMA)/2)*1000)
	ctnAttrs := []tmCtnAttribute{
		{
			ctnName:    cntName,
			cpuRequest: coresReq,
			cpuLimit:   coresReq,
		},
	}
	podName := "gu-pod-2"
	framework.Logf("creating pod %s attrs %v", podName, nil)
	pod := makeTopologyManagerTestPod(podName, ctnAttrs, nil)
	pod = e2epod.NewPodClient(f).CreateSync(ctx, pod)
	framework.Logf("created pod %s", podName)

	ginkgo.By(fmt.Sprintf("validating the container %s on Gu pod %s", cntName, pod.Name))

	logs, err := e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, pod.Name, cntName)
	framework.ExpectNoError(err, "expected log not found in container [%s] of pod [%s]", cntName, pod.Name)

	framework.Logf("got pod logs: %v", logs)
	podEnv, err := makeEnvMap(logs)
	framework.ExpectNoError(err, "expected log not found in container [%s] of pod [%s]", cntName, pod.Name)

	CPUToNUMANode, err := getCPUToNUMANodeMapFromEnv(f, pod, &pod.Spec.Containers[0], podEnv, numaNodes)
	framework.ExpectNoError(err, "expected log not found in container [%s] of pod [%s]", cntName, pod.Name)

	numaUsed := sets.New[int]()
	for _, numa := range CPUToNUMANode {
		numaUsed.Insert(numa)
	}

	numaList := numaUsed.UnsortedList()
	gomega.Expect(numaList).To(gomega.HaveLen(2))

	distance := getMinRemoteDistanceForNode(distances)
	gomega.Expect(distance).NotTo(gomega.Equal(distances[numaList[0]][numaList[1]]))

	deletePodsAsync(ctx, f, podMap)
}

func valiidatePreferClosestNUMAOptimalAllocation(ctx context.Context, f *framework.Framework, podMap map[string]*v1.Pod, numaNodes int, distances map[int][]int) {
	for _, pod := range podMap {
		for _, cnt := range pod.Spec.Containers {
			ginkgo.By(fmt.Sprintf("validating the container %s on Gu pod %s", cnt.Name, pod.Name))

			logs, err := e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, pod.Name, cnt.Name)
			framework.ExpectNoError(err, "expected log not found in container [%s] of pod [%s]", cnt.Name, pod.Name)

			framework.Logf("got pod logs: %v", logs)
			podEnv, err := makeEnvMap(logs)
			framework.ExpectNoError(err, "expected log not found in container [%s] of pod [%s]", cnt.Name, pod.Name)

			CPUToNUMANode, err := getCPUToNUMANodeMapFromEnv(f, pod, &cnt, podEnv, numaNodes)
			framework.ExpectNoError(err, "expected log not found in container [%s] of pod [%s]", cnt.Name, pod.Name)

			numaUsed := sets.New[int]()
			for _, numa := range CPUToNUMANode {
				numaUsed.Insert(numa)
			}

			numaList := numaUsed.UnsortedList()
			gomega.Expect(numaList).To(gomega.HaveLen(2))

			distance := getMinRemoteDistanceForNode(distances)
			gomega.Expect(distance).To(gomega.Equal(distances[numaList[0]][numaList[1]]))
		}
	}
}

func runTopologyManagerTests(f *framework.Framework, topologyOptions map[string]string) {
	var oldCfg *kubeletconfig.KubeletConfiguration
	var err error

	var policies = []string{
		topologymanager.PolicySingleNumaNode,
		topologymanager.PolicyRestricted,
		topologymanager.PolicyBestEffort,
		topologymanager.PolicyNone,
	}

	ginkgo.It("run Topology Manager policy test suite", func(ctx context.Context) {
		oldCfg, err = getCurrentKubeletConfig(ctx)
		framework.ExpectNoError(err)

		scope := containerScopeTopology
		for _, policy := range policies {
			// 配置 Topology Manager。
			ginkgo.By(fmt.Sprintf("by configuring Topology Manager policy to %s", policy))
			framework.Logf("Configuring topology Manager policy to %s", policy)

			newCfg, _ := configureTopologyManagerInKubelet(oldCfg, policy, scope, topologyOptions, nil, 0)
			updateKubeletConfig(ctx, f, newCfg, true)
			// 运行测试。
			runTopologyManagerPolicySuiteTests(ctx, f)
		}
	})

	ginkgo.It("run Topology Manager node alignment test suite", func(ctx context.Context) {
		numaNodes, coreCount := hostPrecheck()

		configMap := getSRIOVDevicePluginConfigMap(framework.TestContext.SriovdpConfigMapFile)

		oldCfg, err = getCurrentKubeletConfig(ctx)
		framework.ExpectNoError(err)

		sd := setupSRIOVConfigOrFail(ctx, f, configMap)
		ginkgo.DeferCleanup(teardownSRIOVConfigOrFail, f, sd)

		scope := containerScopeTopology
		for _, policy := range policies {
			// 配置 Topology Manager。
			ginkgo.By(fmt.Sprintf("by configuring Topology Manager policy to %s", policy))
			framework.Logf("Configuring topology Manager policy to %s", policy)

			newCfg, reservedSystemCPUs := configureTopologyManagerInKubelet(oldCfg, policy, scope, topologyOptions, configMap, numaNodes)
			updateKubeletConfig(ctx, f, newCfg, true)

			runTopologyManagerNodeAlignmentSuiteTests(ctx, f, sd, reservedSystemCPUs, policy, numaNodes, coreCount)
		}
	})

	ginkgo.It("run the Topology Manager pod scope alignment test suite", func(ctx context.Context) {
		numaNodes, coreCount := hostPrecheck()

		configMap := getSRIOVDevicePluginConfigMap(framework.TestContext.SriovdpConfigMapFile)

		oldCfg, err = getCurrentKubeletConfig(ctx)
		framework.ExpectNoError(err)

		policy := topologymanager.PolicySingleNumaNode
		scope := podScopeTopology

		newCfg, reservedSystemCPUs := configureTopologyManagerInKubelet(oldCfg, policy, scope, topologyOptions, configMap, numaNodes)
		updateKubeletConfig(ctx, f, newCfg, true)

		runTMScopeResourceAlignmentTestSuite(ctx, f, configMap, reservedSystemCPUs, policy, numaNodes, coreCount)
	})

	ginkgo.AfterEach(func(ctx context.Context) {
		if oldCfg != nil {
			// 恢复 kubelet 配置。
			updateKubeletConfig(ctx, f, oldCfg, true)
		}
	})
}

func runPreferClosestNUMATests(f *framework.Framework) {
	var oldCfg *kubeletconfig.KubeletConfiguration
	var err error

	ginkgo.It("run the Topology Manager prefer-closest-numa policy option test suite", func(ctx context.Context) {
		numaNodes := detectNUMANodes()
		if numaNodes < minNumaNodesPreferClosestNUMA {
			e2eskipper.Skipf("this test is intended to be run on at least 4 NUMA node system")
		}

		numaDistances := detectNUMADistances(numaNodes)

		oldCfg, err = getCurrentKubeletConfig(ctx)
		framework.ExpectNoError(err)

		policy := topologymanager.PolicyBestEffort
		scope := containerScopeTopology
		options := map[string]string{topologymanager.PreferClosestNUMANodes: "true"}

		newCfg, _ := configureTopologyManagerInKubelet(oldCfg, policy, scope, options, &v1.ConfigMap{}, numaNodes)
		updateKubeletConfig(ctx, f, newCfg, true)

		runPreferClosestNUMATestSuite(ctx, f, numaNodes, numaDistances)
	})

	ginkgo.AfterEach(func(ctx context.Context) {
		if oldCfg != nil {
			// 恢复 kubelet 配置。
			updateKubeletConfig(ctx, f, oldCfg, true)
		}
	})
}

func hostPrecheck() (int, int) {
	// 这是一个粗略检查，只想排除完全没有 SRIOV 设备的系统。
	// 更严格的检查会在 runTopologyManagerPositiveTest 中进行。

	numaNodes := detectNUMANodes()
	if numaNodes < minNumaNodes {
		e2eskipper.Skipf("this test is intended to be run on a multi-node NUMA system")
	}

	coreCount := detectCoresPerSocket()
	if coreCount < minCoreCount {
		e2eskipper.Skipf("this test is intended to be run on a system with at least %d cores per socket", minCoreCount)
	}

	requireSRIOVDevices()

	return numaNodes, coreCount
}

// 标记为 Serial，因为该测试会修改 kubelet 配置。
var _ = SIGDescribe("Topology Manager", framework.WithSerial(), feature.TopologyManager, func() {
	f := framework.NewDefaultFramework("topology-manager-test")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.Context("With kubeconfig updated to static CPU Manager policy run the Topology Manager tests", func() {
		runTopologyManagerTests(f, nil)
	})
	ginkgo.Context("With kubeconfig's topologyOptions updated to static CPU Manager policy run the Topology Manager tests", ginkgo.Label("MaxAllowableNUMANodes"), func() {
		// 目前 defaultMaxAllowableNUMANode 的默认值为 8，这里用
		// 2 * defaultMaxAllowableNUMANode（16）运行相同的测试。
		// 这是验证改动后的设置不会破坏功能的最基本检查。
		doubleDefaultMaxAllowableNUMANodes := strconv.Itoa(8 * 2)
		runTopologyManagerTests(f, map[string]string{topologymanager.MaxAllowableNUMANodes: doubleDefaultMaxAllowableNUMANodes})
	})
	ginkgo.Context("With kubeconfig's prefer-closes-numa-nodes topologyOptions enabled run the Topology Manager tests", ginkgo.Label("PreferClosestNUMANodes"), func() {
		runPreferClosestNUMATests(f)
	})
})

func runGuPodTest(ctx context.Context, f *framework.Framework, cpuCount int, strictReservedCPUs cpuset.CPUSet) {
	var pod *v1.Pod

	ctnAttrs := []ctnAttribute{
		{
			ctnName:    "gu-container",
			cpuRequest: fmt.Sprintf("%dm", 1000*cpuCount),
			cpuLimit:   fmt.Sprintf("%dm", 1000*cpuCount),
		},
	}
	pod = makeCPUManagerPod("gu-pod", ctnAttrs)
	pod = e2epod.NewPodClient(f).CreateSync(ctx, pod)

	ginkgo.By("checking if the expected cpuset was assigned")
	// 任何一个完整 CPU 都可以，无法也不应预测具体是哪一个。
	for _, cnt := range pod.Spec.Containers {
		ginkgo.By(fmt.Sprintf("validating the container %s on Gu pod %s", cnt.Name, pod.Name))

		logs, err := e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, pod.Name, cnt.Name)
		framework.ExpectNoError(err, "expected log not found in container [%s] of pod [%s]", cnt.Name, pod.Name)

		cpus := getContainerAllowedCPUsFromLogs(pod.Name, cnt.Name, logs)

		gomega.Expect(cpus.Size()).To(gomega.Equal(cpuCount), "expected cpu set size == %d, got %q", cpuCount, cpus.String())
		gomega.Expect(cpus.Intersection(strictReservedCPUs).IsEmpty()).To(gomega.BeTrueBecause("cpuset %q should not contain strict reserved cpus %q", cpus.String(), strictReservedCPUs.String()))
	}

	ginkgo.By("by deleting the pods and waiting for container removal")
	deletePods(ctx, f, []string{pod.Name})
	waitForAllContainerRemoval(ctx, pod.Name, pod.Namespace)
}

func runNonGuPodTest(ctx context.Context, f *framework.Framework, cpuCap int64, strictReservedCPUs cpuset.CPUSet) {
	var ctnAttrs []ctnAttribute
	var err error
	var pod *v1.Pod
	var expAllowedCPUsListRegex string

	ctnAttrs = []ctnAttribute{
		{
			ctnName:    "non-gu-container",
			cpuRequest: "100m",
			cpuLimit:   "200m",
		},
	}
	pod = makeCPUManagerPod("non-gu-pod", ctnAttrs)
	pod = e2epod.NewPodClient(f).CreateSync(ctx, pod)

	ginkgo.By("checking if the expected cpuset was assigned")
	expAllowedCPUs, err := cpuset.Parse(fmt.Sprintf("0-%d", cpuCap-1))
	framework.ExpectNoError(err)
	expAllowedCPUs = expAllowedCPUs.Difference(strictReservedCPUs)
	expAllowedCPUsListRegex = fmt.Sprintf("^%s\n$", expAllowedCPUs.String())
	gomega.Eventually(ctx, func() error {
		return e2epod.NewPodClient(f).MatchContainerOutput(ctx, pod.Name, pod.Spec.Containers[0].Name, expAllowedCPUsListRegex)
	}).WithTimeout(30*time.Second).WithPolling(2*time.Second).Should(gomega.Succeed(),
		"expected log not found in container [%s] of pod [%s]", pod.Spec.Containers[0].Name, pod.Name)

	ginkgo.By("by deleting the pods and waiting for container removal")
	deletePods(ctx, f, []string{pod.Name})
	waitForContainerRemoval(ctx, pod.Spec.Containers[0].Name, pod.Name, pod.Namespace)
}

func runMultipleGuNonGuPods(ctx context.Context, f *framework.Framework, cpuCap int64, cpuAlloc int64) {
	var cpuListString, expAllowedCPUsListRegex string
	var cpuList []int
	var cpu1 int
	var cset cpuset.CPUSet
	var err error
	var ctnAttrs []ctnAttribute
	var pod1, pod2 *v1.Pod

	ctnAttrs = []ctnAttribute{
		{
			ctnName:    "gu-container",
			cpuRequest: "1000m",
			cpuLimit:   "1000m",
		},
	}
	pod1 = makeCPUManagerPod("gu-pod", ctnAttrs)
	pod1 = e2epod.NewPodClient(f).CreateSync(ctx, pod1)

	ctnAttrs = []ctnAttribute{
		{
			ctnName:    "non-gu-container",
			cpuRequest: "200m",
			cpuLimit:   "300m",
		},
	}
	pod2 = makeCPUManagerPod("non-gu-pod", ctnAttrs)
	pod2 = e2epod.NewPodClient(f).CreateSync(ctx, pod2)

	ginkgo.By("checking if the expected cpuset was assigned")
	cpu1 = 1
	if isHTEnabled() {
		cpuList = mustParseCPUSet(getCPUSiblingList(0)).List()
		cpu1 = cpuList[1]
	} else if isMultiNUMA() {
		cpuList = mustParseCPUSet(getCoreSiblingList(0)).List()
		if len(cpuList) > 1 {
			cpu1 = cpuList[1]
		}
	}
	expAllowedCPUsListRegex = fmt.Sprintf("^%d\n$", cpu1)
	err = e2epod.NewPodClient(f).MatchContainerOutput(ctx, pod1.Name, pod1.Spec.Containers[0].Name, expAllowedCPUsListRegex)
	framework.ExpectNoError(err, "expected log not found in container [%s] of pod [%s]",
		pod1.Spec.Containers[0].Name, pod1.Name)

	cpuListString = "0"
	if cpuAlloc > 2 {
		cset = mustParseCPUSet(fmt.Sprintf("0-%d", cpuCap-1))
		cpuListString = cset.Difference(cpuset.New(cpu1)).String()
	}
	expAllowedCPUsListRegex = fmt.Sprintf("^%s\n$", cpuListString)
	err = e2epod.NewPodClient(f).MatchContainerOutput(ctx, pod2.Name, pod2.Spec.Containers[0].Name, expAllowedCPUsListRegex)
	framework.ExpectNoError(err, "expected log not found in container [%s] of pod [%s]",
		pod2.Spec.Containers[0].Name, pod2.Name)
	ginkgo.By("by deleting the pods and waiting for container removal")
	deletePods(ctx, f, []string{pod1.Name, pod2.Name})
	waitForContainerRemoval(ctx, pod1.Spec.Containers[0].Name, pod1.Name, pod1.Namespace)
	waitForContainerRemoval(ctx, pod2.Spec.Containers[0].Name, pod2.Name, pod2.Namespace)
}

func runMultipleCPUGuPod(ctx context.Context, f *framework.Framework) {
	var cpuListString, expAllowedCPUsListRegex string
	var cpuList []int
	var cset cpuset.CPUSet
	var err error
	var ctnAttrs []ctnAttribute
	var pod *v1.Pod

	ctnAttrs = []ctnAttribute{
		{
			ctnName:    "gu-container",
			cpuRequest: "2000m",
			cpuLimit:   "2000m",
		},
	}
	pod = makeCPUManagerPod("gu-pod", ctnAttrs)
	pod = e2epod.NewPodClient(f).CreateSync(ctx, pod)

	ginkgo.By("checking if the expected cpuset was assigned")
	cpuListString = "1-2"
	if isMultiNUMA() {
		cpuList = mustParseCPUSet(getCoreSiblingList(0)).List()
		if len(cpuList) > 1 {
			cset = mustParseCPUSet(getCPUSiblingList(int64(cpuList[1])))
			if !isHTEnabled() && len(cpuList) > 2 {
				cset = mustParseCPUSet(fmt.Sprintf("%d-%d", cpuList[1], cpuList[2]))
			}
			cpuListString = cset.String()
		}
	} else if isHTEnabled() {
		cpuListString = "2-3"
		cpuList = mustParseCPUSet(getCPUSiblingList(0)).List()
		if cpuList[1] != 1 {
			cset = mustParseCPUSet(getCPUSiblingList(1))
			cpuListString = cset.String()
		}
	}
	expAllowedCPUsListRegex = fmt.Sprintf("^%s\n$", cpuListString)
	err = e2epod.NewPodClient(f).MatchContainerOutput(ctx, pod.Name, pod.Spec.Containers[0].Name, expAllowedCPUsListRegex)
	framework.ExpectNoError(err, "expected log not found in container [%s] of pod [%s]",
		pod.Spec.Containers[0].Name, pod.Name)

	ginkgo.By("by deleting the pods and waiting for container removal")
	deletePods(ctx, f, []string{pod.Name})
	waitForContainerRemoval(ctx, pod.Spec.Containers[0].Name, pod.Name, pod.Namespace)
}

func runMultipleCPUContainersGuPod(ctx context.Context, f *framework.Framework) {
	var expAllowedCPUsListRegex string
	var cpuList []int
	var cpu1, cpu2 int
	var err error
	var ctnAttrs []ctnAttribute
	var pod *v1.Pod
	ctnAttrs = []ctnAttribute{
		{
			ctnName:    "gu-container1",
			cpuRequest: "1000m",
			cpuLimit:   "1000m",
		},
		{
			ctnName:    "gu-container2",
			cpuRequest: "1000m",
			cpuLimit:   "1000m",
		},
	}
	pod = makeCPUManagerPod("gu-pod", ctnAttrs)
	pod = e2epod.NewPodClient(f).CreateSync(ctx, pod)

	ginkgo.By("checking if the expected cpuset was assigned")
	cpu1, cpu2 = 1, 2
	if isHTEnabled() {
		cpuList = mustParseCPUSet(getCPUSiblingList(0)).List()
		if cpuList[1] != 1 {
			cpu1, cpu2 = cpuList[1], 1
		}
		if isMultiNUMA() {
			cpuList = mustParseCPUSet(getCoreSiblingList(0)).List()
			if len(cpuList) > 1 {
				cpu2 = cpuList[1]
			}
		}
	} else if isMultiNUMA() {
		cpuList = mustParseCPUSet(getCoreSiblingList(0)).List()
		if len(cpuList) > 2 {
			cpu1, cpu2 = cpuList[1], cpuList[2]
		}
	}
	expAllowedCPUsListRegex = fmt.Sprintf("^%d|%d\n$", cpu1, cpu2)
	err = e2epod.NewPodClient(f).MatchContainerOutput(ctx, pod.Name, pod.Spec.Containers[0].Name, expAllowedCPUsListRegex)
	framework.ExpectNoError(err, "expected log not found in container [%s] of pod [%s]",
		pod.Spec.Containers[0].Name, pod.Name)

	err = e2epod.NewPodClient(f).MatchContainerOutput(ctx, pod.Name, pod.Spec.Containers[1].Name, expAllowedCPUsListRegex)
	framework.ExpectNoError(err, "expected log not found in container [%s] of pod [%s]",
		pod.Spec.Containers[1].Name, pod.Name)

	ginkgo.By("by deleting the pods and waiting for container removal")
	deletePods(ctx, f, []string{pod.Name})
	waitForContainerRemoval(ctx, pod.Spec.Containers[0].Name, pod.Name, pod.Namespace)
	waitForContainerRemoval(ctx, pod.Spec.Containers[1].Name, pod.Name, pod.Namespace)
}

func runMultipleGuPods(ctx context.Context, f *framework.Framework) {
	var expAllowedCPUsListRegex string
	var cpuList []int
	var cpu1, cpu2 int
	var err error
	var ctnAttrs []ctnAttribute
	var pod1, pod2 *v1.Pod

	ctnAttrs = []ctnAttribute{
		{
			ctnName:    "gu-container1",
			cpuRequest: "1000m",
			cpuLimit:   "1000m",
		},
	}
	pod1 = makeCPUManagerPod("gu-pod1", ctnAttrs)
	pod1 = e2epod.NewPodClient(f).CreateSync(ctx, pod1)

	ctnAttrs = []ctnAttribute{
		{
			ctnName:    "gu-container2",
			cpuRequest: "1000m",
			cpuLimit:   "1000m",
		},
	}
	pod2 = makeCPUManagerPod("gu-pod2", ctnAttrs)
	pod2 = e2epod.NewPodClient(f).CreateSync(ctx, pod2)

	ginkgo.By("checking if the expected cpuset was assigned")
	cpu1, cpu2 = 1, 2
	if isHTEnabled() {
		cpuList = mustParseCPUSet(getCPUSiblingList(0)).List()
		if cpuList[1] != 1 {
			cpu1, cpu2 = cpuList[1], 1
		}
		if isMultiNUMA() {
			cpuList = mustParseCPUSet(getCoreSiblingList(0)).List()
			if len(cpuList) > 1 {
				cpu2 = cpuList[1]
			}
		}
	} else if isMultiNUMA() {
		cpuList = mustParseCPUSet(getCoreSiblingList(0)).List()
		if len(cpuList) > 2 {
			cpu1, cpu2 = cpuList[1], cpuList[2]
		}
	}
	expAllowedCPUsListRegex = fmt.Sprintf("^%d\n$", cpu1)
	err = e2epod.NewPodClient(f).MatchContainerOutput(ctx, pod1.Name, pod1.Spec.Containers[0].Name, expAllowedCPUsListRegex)
	framework.ExpectNoError(err, "expected log not found in container [%s] of pod [%s]",
		pod1.Spec.Containers[0].Name, pod1.Name)

	expAllowedCPUsListRegex = fmt.Sprintf("^%d\n$", cpu2)
	err = e2epod.NewPodClient(f).MatchContainerOutput(ctx, pod2.Name, pod2.Spec.Containers[0].Name, expAllowedCPUsListRegex)
	framework.ExpectNoError(err, "expected log not found in container [%s] of pod [%s]",
		pod2.Spec.Containers[0].Name, pod2.Name)
	ginkgo.By("by deleting the pods and waiting for container removal")
	deletePods(ctx, f, []string{pod1.Name, pod2.Name})
	waitForContainerRemoval(ctx, pod1.Spec.Containers[0].Name, pod1.Name, pod1.Namespace)
	waitForContainerRemoval(ctx, pod2.Spec.Containers[0].Name, pod2.Name, pod2.Namespace)
}

func mustParseCPUSet(s string) cpuset.CPUSet {
	res, err := cpuset.Parse(s)
	framework.ExpectNoError(err)
	return res
}
