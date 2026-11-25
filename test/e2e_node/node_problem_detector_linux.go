//go:build cgo && linux

/*
Copyright 2016 The Kubernetes Authors.

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
	"path"
	"path/filepath"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	clientset "k8s.io/client-go/kubernetes"
	coreclientset "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/klog/v2"
	admissionapi "k8s.io/pod-security-admission/api"

	"k8s.io/kubernetes/pkg/kubelet/util"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	testutils "k8s.io/kubernetes/test/utils"
)

var _ = SIGDescribe("NodeProblemDetector", feature.NodeProblemDetector, framework.WithSerial(), func() {
	const (
		pollInterval   = 1 * time.Second
		pollConsistent = 5 * time.Second
		pollTimeout    = 5 * time.Minute
	)
	f := framework.NewDefaultFramework("node-problem-detector")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	var c clientset.Interface
	var uid string
	var ns, name, configName, eventNamespace string
	var bootTime, nodeTime time.Time
	var image string

	ginkgo.BeforeEach(func() {
		c = f.ClientSet
		ns = f.Namespace.Name
		uid = string(uuid.NewUUID())
		name = "node-problem-detector-" + uid
		configName = "node-problem-detector-config-" + uid
		// Node 没有 namespace，event recorder 会为 node event 设置默认 namespace。
		eventNamespace = metav1.NamespaceDefault
		image = getNodeProblemDetectorImage()
		ginkgo.By(fmt.Sprintf("Using node-problem-detector image: %s", image))
	})

	// 测试 system log monitor。将来如果有更多 problem daemon，可以在这里添加其他测试。
	ginkgo.Describe("SystemLogMonitor", func() {
		const (
			// 使用测试用的 condition，避免修改实际正在使用的 node condition。
			// TODO(random-liu): 现在 node condition 可以是任意字符串，等切换到预定义的 condition 列表时，
			// 考虑是否需要添加 TestCondition。
			condition = v1.NodeConditionType("TestCondition")

			// 测试中使用的文件路径。
			logFile        = "/log/test.log"
			configFile     = "/config/testconfig.json"
			kubeConfigFile = "/config/kubeconfig"
			etcLocaltime   = "/etc/localtime"

			// 测试中使用的 volume。
			configVolume    = "config"
			logVolume       = "log"
			localtimeVolume = "localtime"

			// 测试中使用的 reason 和 message。
			defaultReason  = "Default"
			defaultMessage = "default message"
			tempReason     = "Temporary"
			tempMessage    = "temporary error"
			permReason1    = "Permanent1"
			permMessage1   = "permanent error 1"
			permReason2    = "Permanent2"
			permMessage2   = "permanent error 2"
		)
		var source, config, hostLogFile string
		var lookback time.Duration
		var eventListOptions metav1.ListOptions

		ginkgo.BeforeEach(func(ctx context.Context) {
			ginkgo.By("Calculate Lookback duration")
			var err error

			nodeTime = time.Now()
			bootTime, err = util.GetBootTime(klog.FromContext(ctx))
			framework.ExpectNoError(err)

			// 将 lookback 时长设置为大于节点已运行时间。
			// 假设测试不会超过 1 小时，实际上通常只需要 90 秒。
			lookback = nodeTime.Sub(bootTime) + time.Hour

			// 随机化 source 名称
			source = "kernel-monitor-" + uid
			config = `
			{
				"plugin": "filelog",
				"pluginConfig": {
					"timestamp": "^.{15}",
					"message": "kernel: \\[.*\\] (.*)",
					"timestampFormat": "` + time.Stamp + `"
				},
				"logPath": "` + logFile + `",
				"lookback": "` + lookback.String() + `",
				"bufferSize": 10,
				"source": "` + source + `",
				"conditions": [
				{
					"type": "` + string(condition) + `",
					"reason": "` + defaultReason + `",
					"message": "` + defaultMessage + `"
				}
				],
				"rules": [
				{
					"type": "temporary",
					"reason": "` + tempReason + `",
					"pattern": "` + tempMessage + `"
				},
				{
					"type": "permanent",
					"condition": "` + string(condition) + `",
					"reason": "` + permReason1 + `",
					"pattern": "` + permMessage1 + ".*" + `"
				},
				{
					"type": "permanent",
					"condition": "` + string(condition) + `",
					"reason": "` + permReason2 + `",
					"pattern": "` + permMessage2 + ".*" + `"
				}
				]
			}`

			// apiserver 已知这个 token，它属于 `system:masters` 组。
			// 另见 `test/e2e_node/services/apiserver.go` 中的 `generateTokenFile` 函数。
			kubeConfig := fmt.Sprintf(`
apiVersion: v1
kind: Config
users:
- name: node-problem-detector
  user:
    token: %s
clusters:
- cluster:
    server: %s
    insecure-skip-tls-verify: true
  name: local
contexts:
- context:
    cluster: local
    user: node-problem-detector
  name: local-context
current-context: local-context
`, framework.TestContext.BearerToken, framework.TestContext.Host)

			ginkgo.By("Generate event list options")
			selector := fields.Set{
				"involvedObject.kind":      "Node",
				"involvedObject.name":      framework.TestContext.NodeName,
				"involvedObject.namespace": metav1.NamespaceAll,
				"source":                   source,
			}.AsSelector().String()
			eventListOptions = metav1.ListOptions{FieldSelector: selector}

			ginkgo.By("Create config map for the node problem detector")
			_, err = c.CoreV1().ConfigMaps(ns).Create(ctx, &v1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: configName},
				Data: map[string]string{
					path.Base(configFile):     config,
					path.Base(kubeConfigFile): kubeConfig,
				},
			}, metav1.CreateOptions{})
			framework.ExpectNoError(err)

			ginkgo.By("Create the node problem detector")
			hostPathType := new(v1.HostPathType)
			*hostPathType = v1.HostPathFileOrCreate
			pod := e2epod.NewPodClient(f).CreateSync(ctx, &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: name,
				},
				Spec: v1.PodSpec{
					HostNetwork:        true,
					SecurityContext:    &v1.PodSecurityContext{},
					ServiceAccountName: name,
					Volumes: []v1.Volume{
						{
							Name: configVolume,
							VolumeSource: v1.VolumeSource{
								ConfigMap: &v1.ConfigMapVolumeSource{
									LocalObjectReference: v1.LocalObjectReference{Name: configName},
								},
							},
						},
						{
							Name: logVolume,
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{},
							},
						},
						{
							Name: localtimeVolume,
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: etcLocaltime,
									Type: hostPathType,
								},
							},
						},
					},
					InitContainers: []v1.Container{
						{
							Name:    "init-log-file",
							Image:   "debian",
							Command: []string{"/bin/sh"},
							Args: []string{
								"-c",
								fmt.Sprintf("touch %s", logFile),
							},
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      logVolume,
									MountPath: path.Dir(logFile),
								},
								{
									Name:      localtimeVolume,
									MountPath: etcLocaltime,
								},
							},
						},
					},
					Containers: []v1.Container{
						{
							Name:    name,
							Image:   image,
							Command: []string{"/node-problem-detector"},
							Args: []string{
								"--logtostderr",
								fmt.Sprintf("--system-log-monitors=%s", configFile),
								// node e2e 测试中禁用了 `ServiceAccount` admission controller，
								// 因此这里不能使用 inClusterConfig。
								fmt.Sprintf("--apiserver-override=%s?inClusterConfig=false&auth=%s", framework.TestContext.Host, kubeConfigFile),
							},
							Env: []v1.EnvVar{
								{
									Name: "NODE_NAME",
									ValueFrom: &v1.EnvVarSource{
										FieldRef: &v1.ObjectFieldSelector{
											APIVersion: "v1",
											FieldPath:  "spec.nodeName",
										},
									},
								},
							},
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      logVolume,
									MountPath: path.Dir(logFile),
								},
								{
									Name:      localtimeVolume,
									MountPath: etcLocaltime,
								},
								{
									Name:      configVolume,
									MountPath: path.Dir(configFile),
								},
							},
						},
					},
				},
			})
			hostLogFile = filepath.Join(framework.TestContext.KubeletRootDir, "pods", string(pod.UID), "volumes/kubernetes.io~empty-dir") + logFile
		})

		ginkgo.It("should generate node condition and events for corresponding errors", func(ctx context.Context) {
			for _, test := range []struct {
				description      string
				timestamp        time.Time
				message          string
				messageNum       int
				tempEvents       int // 临时错误产生的 event
				totalEvents      int // 临时错误和 condition 变化产生的 event 总数
				conditionReason  string
				conditionMessage string
				conditionType    v1.ConditionStatus
			}{
				{
					description:      "should generate default node condition",
					conditionReason:  defaultReason,
					conditionMessage: defaultMessage,
					conditionType:    v1.ConditionFalse,
				},
				{
					description:      "should not generate events for too old log",
					timestamp:        bootTime.Add(-1 * time.Minute),
					message:          tempMessage,
					messageNum:       3,
					conditionReason:  defaultReason,
					conditionMessage: defaultMessage,
					conditionType:    v1.ConditionFalse,
				},
				{
					description:      "should not change node condition for too old log",
					timestamp:        bootTime.Add(-1 * time.Minute),
					message:          permMessage1,
					messageNum:       1,
					conditionReason:  defaultReason,
					conditionMessage: defaultMessage,
					conditionType:    v1.ConditionFalse,
				},
				{
					description:      "should generate event for old log within lookback duration",
					timestamp:        nodeTime,
					message:          tempMessage,
					messageNum:       3,
					tempEvents:       3,
					totalEvents:      3,
					conditionReason:  defaultReason,
					conditionMessage: defaultMessage,
					conditionType:    v1.ConditionFalse,
				},
				{
					description:      "should change node condition for old log within lookback duration",
					timestamp:        nodeTime,
					message:          permMessage1,
					messageNum:       1,
					tempEvents:       3, // 临时错误的 event 数量不应改变
					totalEvents:      4, // condition 变化新增 1 个 event
					conditionReason:  permReason1,
					conditionMessage: permMessage1,
					conditionType:    v1.ConditionTrue,
				},
				{
					description:      "should generate event for new log",
					timestamp:        nodeTime.Add(5 * time.Minute),
					message:          tempMessage,
					messageNum:       3,
					tempEvents:       6, // 临时错误新增 3 个 event
					totalEvents:      7, // 临时错误新增 3 个 event
					conditionReason:  permReason1,
					conditionMessage: permMessage1,
					conditionType:    v1.ConditionTrue,
				},
				{
					description:      "should not update node condition with the same reason",
					timestamp:        nodeTime.Add(5 * time.Minute),
					message:          permMessage1 + "different message",
					messageNum:       1,
					tempEvents:       6, // event 数量不应改变
					totalEvents:      7, // event 数量不应改变
					conditionReason:  permReason1,
					conditionMessage: permMessage1,
					conditionType:    v1.ConditionTrue,
				},
				{
					description:      "should change node condition for new log",
					timestamp:        nodeTime.Add(5 * time.Minute),
					message:          permMessage2,
					messageNum:       1,
					tempEvents:       6, // 临时错误的 event 数量不应改变
					totalEvents:      8, // condition 变化新增 1 个 event
					conditionReason:  permReason2,
					conditionMessage: permMessage2,
					conditionType:    v1.ConditionTrue,
				},
			} {
				ginkgo.By(test.description)
				if test.messageNum > 0 {
					ginkgo.By(fmt.Sprintf("Inject %d logs: %q", test.messageNum, test.message))
					err := injectLog(hostLogFile, test.timestamp, test.message, test.messageNum)
					framework.ExpectNoError(err)
				}

				ginkgo.By(fmt.Sprintf("Wait for %d temp events generated", test.tempEvents))
				gomega.Eventually(ctx, func(ctx context.Context) error {
					return verifyEvents(ctx, c.CoreV1().Events(eventNamespace), eventListOptions, test.tempEvents, tempReason, tempMessage)
				}, pollTimeout, pollInterval).Should(gomega.Succeed())
				ginkgo.By(fmt.Sprintf("Wait for %d total events generated", test.totalEvents))
				gomega.Eventually(ctx, func(ctx context.Context) error {
					return verifyTotalEvents(ctx, c.CoreV1().Events(eventNamespace), eventListOptions, test.totalEvents)
				}, pollTimeout, pollInterval).Should(gomega.Succeed())
				ginkgo.By(fmt.Sprintf("Make sure only %d total events generated", test.totalEvents))
				gomega.Consistently(ctx, func(ctx context.Context) error {
					return verifyTotalEvents(ctx, c.CoreV1().Events(eventNamespace), eventListOptions, test.totalEvents)
				}, pollConsistent, pollInterval).Should(gomega.Succeed())

				ginkgo.By(fmt.Sprintf("Make sure node condition %q is set", condition))
				gomega.Eventually(ctx, func(ctx context.Context) error {
					return verifyNodeCondition(ctx, c.CoreV1().Nodes(), condition, test.conditionType, test.conditionReason, test.conditionMessage)
				}, pollTimeout, pollInterval).Should(gomega.Succeed())
				ginkgo.By(fmt.Sprintf("Make sure node condition %q is stable", condition))
				gomega.Consistently(ctx, func(ctx context.Context) error {
					return verifyNodeCondition(ctx, c.CoreV1().Nodes(), condition, test.conditionType, test.conditionReason, test.conditionMessage)
				}, pollConsistent, pollInterval).Should(gomega.Succeed())
			}
		})

		ginkgo.AfterEach(func(ctx context.Context) {
			if ginkgo.CurrentSpecReport().Failed() && framework.TestContext.DumpLogsOnFailure {
				ginkgo.By("Get node problem detector log")
				log, err := e2epod.GetPodLogs(ctx, c, ns, name, name)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				framework.Logf("Node Problem Detector logs:\n %s", log)
			}
			ginkgo.By("Delete the node problem detector")
			framework.ExpectNoError(e2epod.NewPodClient(f).Delete(ctx, name, *metav1.NewDeleteOptions(0)))
			ginkgo.By("Wait for the node problem detector to disappear")
			gomega.Expect(e2epod.WaitForPodNotFoundInNamespace(ctx, c, name, ns, pollTimeout)).To(gomega.Succeed())
			ginkgo.By("Delete the config map")
			framework.ExpectNoError(c.CoreV1().ConfigMaps(ns).Delete(ctx, configName, metav1.DeleteOptions{}))
			ginkgo.By("Clean up the events")
			gomega.Expect(c.CoreV1().Events(eventNamespace).DeleteCollection(ctx, *metav1.NewDeleteOptions(0), eventListOptions)).To(gomega.Succeed())
			ginkgo.By("Clean up the node condition")
			patch := []byte(fmt.Sprintf(`{"status":{"conditions":[{"$patch":"delete","type":"%s"}]}}`, condition))
			c.CoreV1().RESTClient().Patch(types.StrategicMergePatchType).Resource("nodes").Name(framework.TestContext.NodeName).SubResource("status").Body(patch).Do(ctx)
		})
	})
})

// injectLog 将 kernel log 注入指定文件。
func injectLog(file string, timestamp time.Time, log string, num int) error {
	f, err := os.OpenFile(file, os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	defer f.Close()
	for range num {
		_, err := f.WriteString(fmt.Sprintf("%s kernel: [0.000000] %s\n", timestamp.Format(time.Stamp), log))
		if err != nil {
			return err
		}
	}
	return nil
}

// verifyEvents 验证是否生成了 num 个具有指定 reason 和 message 的 event。
func verifyEvents(ctx context.Context, e coreclientset.EventInterface, options metav1.ListOptions, num int, reason, message string) error {
	events, err := e.List(ctx, options)
	if err != nil {
		return err
	}
	count := 0
	for _, event := range events.Items {
		if event.Reason != reason || event.Message != message {
			continue
		}
		count += int(event.Count)
	}
	if count != num {
		return fmt.Errorf("expected %d events with reason set to %s and message set to %s\nbut %d actual events occurred. Events : %v", num, reason, message, count, events.Items)
	}
	return nil
}

// verifyTotalEvents 验证 event 总数是否为 num。
func verifyTotalEvents(ctx context.Context, e coreclientset.EventInterface, options metav1.ListOptions, num int) error {
	events, err := e.List(ctx, options)
	if err != nil {
		return err
	}
	count := 0
	for _, event := range events.Items {
		count += int(event.Count)
	}
	if count != num {
		return fmt.Errorf("expected total number of events was %d, actual events counted was %d\nEvents  : %v", num, count, events.Items)
	}
	return nil
}

// verifyNodeCondition 验证是否生成了指定的 node condition；如果 reason 和 message 为空，则不检查它们
func verifyNodeCondition(ctx context.Context, n coreclientset.NodeInterface, condition v1.NodeConditionType, status v1.ConditionStatus, reason, message string) error {
	node, err := n.Get(ctx, framework.TestContext.NodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	_, c := testutils.GetNodeCondition(&node.Status, condition)
	if c == nil {
		return fmt.Errorf("node condition %q not found", condition)
	}
	if c.Status != status || c.Reason != reason || c.Message != message {
		return fmt.Errorf("unexpected node condition %q: %+v", condition, c)
	}
	return nil
}
