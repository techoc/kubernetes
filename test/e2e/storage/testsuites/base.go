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

package testsuites

import (
	"context"
	"flag"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/component-base/metrics/testutil"
	csitrans "k8s.io/csi-translation-lib"
	"k8s.io/kubernetes/test/e2e/framework"
	e2emetrics "k8s.io/kubernetes/test/e2e/framework/metrics"
	storageframework "k8s.io/kubernetes/test/e2e/storage/framework"
)

var migratedPlugins *string

func init() {
	migratedPlugins = flag.String("storage.migratedPlugins", "", "comma separated list of in-tree plugin names of form 'kubernetes.io/{pluginName}' migrated to CSI")
}

type opCounts map[string]int64

// migrationOpCheck 校验迁移后的指标。
type migrationOpCheck struct {
	cs         clientset.Interface
	config     *rest.Config
	pluginName string
	skipCheck  bool

	// skipCheck 为 true 时不设置旧 ops。
	oldInTreeOps   opCounts
	oldMigratedOps opCounts
}

// BaseSuites 是同时适用于 in-tree 和 CSI driver 的存储测试套件列表。
var BaseSuites = []func() storageframework.TestSuite{
	InitCapacityTestSuite,
	InitVolumesTestSuite,
	InitVolumeIOTestSuite,
	InitVolumeModeTestSuite,
	InitSubPathTestSuite,
	InitProvisioningTestSuite,
	InitMultiVolumeTestSuite,
	InitVolumeExpandTestSuite,
	InitDisruptiveTestSuite,
	InitVolumeLimitsTestSuite,
	InitTopologyTestSuite,
	InitVolumeStressTestSuite,
	InitFsGroupChangePolicyTestSuite,
	InitVolumeGroupSnapshottableTestSuite,
	InitVolumeGroupSnapshotClassTestSuite,
	func() storageframework.TestSuite {
		return InitCustomEphemeralTestSuite(GenericEphemeralTestPatterns())
	},
}

// CSISuites 是只适用于 CSI driver 的存储测试套件列表。
var CSISuites = append(BaseSuites,
	// 下面的测试套件不得已在 BaseSuites 中，
	// 否则同一配置会被测试多次。
	func() storageframework.TestSuite {
		return InitCustomEphemeralTestSuite(CSIEphemeralTestPatterns())
	},
	InitSnapshottableTestSuite,
	InitSnapshotMetadataTestSuite,
	InitSnapshottableStressTestSuite,
	InitVolumePerformanceTestSuite,
	InitPvcDeletionPerformanceTestSuite,
	InitReadWriteOncePodTestSuite,
	InitVolumeModifyTestSuite,
	InitVolumeModifyStressTestSuite,
	InitSELinuxMountTestSuite,
	InitVolumeGroupSnapshottableStressTestSuite,
)

func getVolumeOpsFromMetricsForPlugin(ms testutil.Metrics, pluginName string) opCounts {
	totOps := opCounts{}

	for method, samples := range ms {
		switch method {
		case "storage_operation_status_count":
			for _, sample := range samples {
				plugin := string(sample.Metric["volume_plugin"])
				if pluginName != plugin {
					continue
				}
				opName := string(sample.Metric["operation_name"])
				if opName == "verify_controller_attached_volume" {
					// 忽略 verify_controller_attached_volume，因为它不调用
					// plugin，只监听 Node API 并更新 Actual State of World 缓存
					continue
				}
				totOps[opName] = totOps[opName] + int64(sample.Value)
			}
		}
	}
	return totOps
}

func getVolumeOpCounts(ctx context.Context, c clientset.Interface, config *rest.Config, pluginName string) opCounts {
	if !framework.ProviderIs("gce", "aws") {
		return opCounts{}
	}

	nodeLimit := 25

	metricsGrabber, err := e2emetrics.NewMetricsGrabber(ctx, c, nil, config, true, false, true, false, false, false)

	if err != nil {
		framework.ExpectNoError(err, "Error creating metrics grabber: %v", err)
	}

	if !metricsGrabber.HasControlPlanePods() {
		framework.Logf("Warning: Environment does not support getting controller-manager metrics")
		return opCounts{}
	}

	controllerMetrics, err := metricsGrabber.GrabFromControllerManager(ctx)
	framework.ExpectNoError(err, "Error getting c-m metrics : %v", err)
	totOps := getVolumeOpsFromMetricsForPlugin(testutil.Metrics(controllerMetrics), pluginName)

	framework.Logf("Node name not specified for getVolumeOpCounts, falling back to listing nodes from API Server")
	nodes, err := c.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	framework.ExpectNoError(err, "Error listing nodes: %v", err)
	if len(nodes.Items) <= nodeLimit {
		// 节点数超过 nodeLimit 的大集群中，
		// 收集所有节点的指标太耗时，因此这类集群
		// 忽略节点指标。
		for _, node := range nodes.Items {
			nodeMetrics, err := metricsGrabber.GrabFromKubelet(ctx, node.GetName())
			framework.ExpectNoError(err, "Error getting Kubelet %v metrics: %v", node.GetName(), err)
			totOps = addOpCounts(totOps, getVolumeOpsFromMetricsForPlugin(testutil.Metrics(nodeMetrics), pluginName))
		}
	} else {
		framework.Logf("Skipping operation metrics gathering from nodes in getVolumeOpCounts, greater than %v nodes", nodeLimit)
	}

	return totOps
}

func addOpCounts(o1 opCounts, o2 opCounts) opCounts {
	totOps := opCounts{}
	seen := sets.NewString()
	for op, count := range o1 {
		seen.Insert(op)
		totOps[op] = totOps[op] + count + o2[op]
	}
	for op, count := range o2 {
		if !seen.Has(op) {
			totOps[op] = totOps[op] + count
		}
	}
	return totOps
}

func getMigrationVolumeOpCounts(ctx context.Context, cs clientset.Interface, config *rest.Config, pluginName string) (opCounts, opCounts) {
	if len(pluginName) > 0 {
		var migratedOps opCounts
		l := csitrans.New()
		csiName, err := l.GetCSINameFromInTreeName(pluginName)
		if err != nil {
			framework.Logf("Could not find CSI Name for in-tree plugin %v", pluginName)
			migratedOps = opCounts{}
		} else {
			csiName = "kubernetes.io/csi:" + csiName
			migratedOps = getVolumeOpCounts(ctx, cs, config, csiName)
		}
		return getVolumeOpCounts(ctx, cs, config, pluginName), migratedOps
	}
	// 不是 in-tree driver
	framework.Logf("Test running for native CSI Driver, not checking metrics")
	return opCounts{}, opCounts{}
}

func newMigrationOpCheck(ctx context.Context, cs clientset.Interface, config *rest.Config, pluginName string) *migrationOpCheck {
	moc := migrationOpCheck{
		cs:         cs,
		config:     config,
		pluginName: pluginName,
	}
	if len(pluginName) == 0 {
		// 这是原生 CSI Driver，不检查 ops
		moc.skipCheck = true
		return &moc
	}

	if !sets.NewString(strings.Split(*migratedPlugins, ",")...).Has(pluginName) {
		// in-tree plugin 未迁移
		framework.Logf("In-tree plugin %v is not migrated, not validating any metrics", pluginName)

		// 不检查 in-tree plugin 指标，因为某些 negative 测试
		// 用例可能不做任何卷操作，因此不会上报任何
		// 指标

		// 不检查 driver 迁移版本的计数，因为并行运行测试时，某个测试
		// 可能原生使用 CSI Driver，从而增加
		// 指标计数

		// TODO(dyzz): 给 OperationGenerator 指标加一个维度，
		// 标记 "migrated"->true/false，以便区分迁移后的指标
		// 和原生 CSI Driver 指标。这样就能检查 driver 迁移版本的计数，
		// 为 negative 测试用例提供更强保证
		//（同时让指标更有信息量）。
		moc.skipCheck = true
		return &moc
	}

	// TODO: 因 issue #[102893](https://github.com/kubernetes/kubernetes/issues/102893) 临时跳过指标检查
	// 问题修复后移除
	if framework.NodeOSDistroIs("windows") {
		moc.skipCheck = true
		return &moc
	}

	moc.oldInTreeOps, moc.oldMigratedOps = getMigrationVolumeOpCounts(ctx, cs, config, pluginName)
	return &moc
}

func (moc *migrationOpCheck) validateMigrationVolumeOpCounts(ctx context.Context) {
	if moc.skipCheck {
		return
	}

	newInTreeOps, _ := getMigrationVolumeOpCounts(ctx, moc.cs, moc.config, moc.pluginName)

	for op, count := range newInTreeOps {
		if count != moc.oldInTreeOps[op] {
			framework.Failf("In-tree plugin %v migrated to CSI Driver, however found %v %v metrics for in-tree plugin", moc.pluginName, count-moc.oldInTreeOps[op], op)
		}
	}
	// 不检查迁移后的指标，因为某些 negative 测试用例
	// 可能不做任何卷操作，因此不会上报任何指标
}

// driver 支持动态配置时跳过 skipVolTypes 模式
func checkVolTypePatterns(pattern storageframework.TestPattern, driver storageframework.TestDriver, skipVolTypes map[storageframework.TestVolType]bool) string {
	_, supportsProvisioning := driver.(storageframework.DynamicPVTestDriver)
	if supportsProvisioning && skipVolTypes[pattern.VolType] {
		return fmt.Sprintf("Driver supports dynamic provisioning, skipping %s pattern", pattern.VolType)
	}
	return ""
}
