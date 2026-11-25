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

package utils

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/uuid"
	clientset "k8s.io/client-go/kubernetes"
	scaleclient "k8s.io/client-go/scale"
	"k8s.io/utils/ptr"

	"k8s.io/klog/v2"
)

const (
	// 用于标记 pod 已被删除的字符串
	nonExist = "NonExist"
)

func waitUntilPodIsScheduled(ctx context.Context, c clientset.Interface, name, namespace string, timeout time.Duration) (*v1.Pod, error) {
	// 等待 pod 被调度
	p, err := c.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{ResourceVersion: "0"})
	if err == nil && p.Spec.NodeName != "" {
		return p, nil
	}
	pollingPeriod := 200 * time.Millisecond
	startTime := time.Now()
	for startTime.Add(timeout).After(time.Now()) && ctx.Err() == nil {
		time.Sleep(pollingPeriod)
		p, err := c.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{ResourceVersion: "0"})
		if err == nil && p.Spec.NodeName != "" {
			return p, nil
		}
	}
	return nil, fmt.Errorf("timed out after %v when waiting for pod %v/%v to start", timeout, namespace, name)
}

func RunPodAndGetNodeName(ctx context.Context, c clientset.Interface, pod *v1.Pod, timeout time.Duration) (string, error) {
	name := pod.Name
	namespace := pod.Namespace
	if err := CreatePodWithRetries(c, namespace, pod); err != nil {
		return "", err
	}
	p, err := waitUntilPodIsScheduled(ctx, c, name, namespace, timeout)
	if err != nil {
		return "", err
	}
	return p.Spec.NodeName, nil
}

type RunObjectConfig interface {
	Run() error
	GetName() string
	GetNamespace() string
	GetKind() schema.GroupKind
	GetClient() clientset.Interface
	GetScalesGetter() scaleclient.ScalesGetter
	SetClient(clientset.Interface)
	SetScalesClient(scaleclient.ScalesGetter)
	GetReplicas() int
	GetLabelValue(string) (string, bool)
	GetGroupResource() schema.GroupResource
	GetGroupVersionResource() schema.GroupVersionResource
}

type RCConfig struct {
	Affinity                      *v1.Affinity
	Client                        clientset.Interface
	ScalesGetter                  scaleclient.ScalesGetter
	Image                         string
	Command                       []string
	Name                          string
	Namespace                     string
	PollInterval                  time.Duration
	Timeout                       time.Duration
	PodStatusFile                 *os.File
	Replicas                      int
	CPURequest                    int64                    // 毫核（millicores）
	CPULimit                      int64                    // 毫核（millicores）
	MemRequest                    int64                    // 字节
	MemLimit                      int64                    // 字节
	GpuLimit                      int64                    // 数量
	PodResources                  *v1.ResourceRequirements // Pod 级别的资源
	ReadinessProbe                *v1.Probe
	DNSPolicy                     *v1.DNSPolicy
	PriorityClassName             string
	TerminationGracePeriodSeconds *int64
	Lifecycle                     *v1.Lifecycle
	SchedulerName                 string

	// 环境变量，每个 pod 都设置为相同的值。
	Env map[string]string

	// 添加到每个 pod 上的额外标签和注解。
	Labels      map[string]string
	Annotations map[string]string

	// RC 中 pod 的 node selector。
	NodeSelector map[string]string

	// RC 中 pod 的 toleration。
	Tolerations []v1.Toleration

	// 要在容器中声明的端口（名称到 containerPort 的映射）。
	Ports map[string]int
	// 要在容器中声明的端口，同时作为 host 端口和容器端口。
	HostPorts map[string]int

	Volumes      []v1.Volume
	VolumeMounts []v1.VolumeMount

	// 指向 pod 列表的指针；如果不为 nil，RunRC 会将其设置为
	// 由该 RC 创建的 pod 列表。
	CreatedPods *[]*v1.Pod

	// 允许的容器失败次数上限。超过时 RunRC 返回错误。
	// 未指定时默认为 replicas*0.1。
	MaxContainerFailures *int
	// 允许的 pod 删除次数上限。超过时 RunRC 返回错误。
	// 默认为 0。
	MaxAllowedPodDeletions int

	// 设为 false 时启动 RC 会打印进度，否则只打印错误。
	Silent bool

	// 如果设置了该函数，将用它代替 klog 打印日志行。
	LogFunc func(fmt string, args ...interface{})
	// 如果设置了这些函数，将用它们从 Node 收集数据——在没有运行 kubelet 的集成测试中，
	// 这些变量应当为 nil。
	NodeDumpFunc      func(ctx context.Context, c clientset.Interface, nodeNames []string, logFunc func(fmt string, args ...interface{}))
	ContainerDumpFunc func(ctx context.Context, c clientset.Interface, ns string, logFunc func(ftm string, args ...interface{}))

	// 要挂载的 secret 和 configmap 的名称。
	SecretNames    []string
	ConfigMapNames []string

	ServiceAccountTokenProjections int

	// 要在 pod 中运行的额外容器
	AdditionalContainers []v1.Container

	// 所创建 pod 的安全上下文
	SecurityContext *v1.SecurityContext
}

func (rc *RCConfig) RCConfigLog(fmt string, args ...interface{}) {
	if rc.LogFunc != nil {
		rc.LogFunc(fmt, args...)
	}
	klog.Infof(fmt, args...)
}

func (config *RCConfig) getTerminationGracePeriodSeconds(defaultGrace *int64) *int64 {
	if config.TerminationGracePeriodSeconds == nil || *config.TerminationGracePeriodSeconds < 0 {
		return defaultGrace
	}
	return config.TerminationGracePeriodSeconds
}

type DeploymentConfig struct {
	RCConfig
}

type ReplicaSetConfig struct {
	RCConfig
}

type JobConfig struct {
	RCConfig
}

// podInfo 包含对调试 e2e 测试有用的 pod 信息。
type podInfo struct {
	oldHostname string
	oldPhase    string
	hostname    string
	phase       string
}

// podDiff 是 pod 名称到 podInfo 的映射
type podDiff map[string]*podInfo

// Print 格式化并打印给定的 podDiff。
func (p podDiff) String(ignorePhases sets.String) string {
	ret := ""
	for name, info := range p {
		if ignorePhases.Has(info.phase) {
			continue
		}
		if info.phase == nonExist {
			ret += fmt.Sprintf("Pod %v was deleted, had phase %v and host %v\n", name, info.oldPhase, info.oldHostname)
			continue
		}
		phaseChange, hostChange := false, false
		msg := fmt.Sprintf("Pod %v ", name)
		if info.oldPhase != info.phase {
			phaseChange = true
			if info.oldPhase == nonExist {
				msg += fmt.Sprintf("in phase %v ", info.phase)
			} else {
				msg += fmt.Sprintf("went from phase: %v -> %v ", info.oldPhase, info.phase)
			}
		}
		if info.oldHostname != info.hostname {
			hostChange = true
			if info.oldHostname == nonExist || info.oldHostname == "" {
				msg += fmt.Sprintf("assigned host %v ", info.hostname)
			} else {
				msg += fmt.Sprintf("went from host: %v -> %v ", info.oldHostname, info.hostname)
			}
		}
		if phaseChange || hostChange {
			ret += msg + "\n"
		}
	}
	return ret
}

// DeletedPods 返回一开始存在、随后消失的 pod 的切片。
func (p podDiff) DeletedPods() []string {
	var deletedPods []string
	for podName, podInfo := range p {
		if podInfo.hostname == nonExist {
			deletedPods = append(deletedPods, podName)
		}
	}
	return deletedPods
}

// diff 根据两个 pod 列表计算 podDiff。
func diff(oldPods []*v1.Pod, curPods []*v1.Pod) podDiff {
	podInfoMap := podDiff{}

	// 新 pod 会出现在 curPods 列表中，但不在 oldPods 中。它们的 oldHostname/phase 为 nonexist。
	for _, pod := range curPods {
		podInfoMap[pod.Name] = &podInfo{hostname: pod.Spec.NodeName, phase: string(pod.Status.Phase), oldHostname: nonExist, oldPhase: nonExist}
	}

	// 被删除的 pod 会出现在 oldPods 列表中，但不在 curPods 中。它们的 hostname/phase 为 nonexist。
	for _, pod := range oldPods {
		if info, ok := podInfoMap[pod.Name]; ok {
			info.oldHostname, info.oldPhase = pod.Spec.NodeName, string(pod.Status.Phase)
		} else {
			podInfoMap[pod.Name] = &podInfo{hostname: nonExist, phase: nonExist, oldHostname: pod.Spec.NodeName, oldPhase: string(pod.Status.Phase)}
		}
	}
	return podInfoMap
}

// RunDeployment 启动一个 Deployment（并验证其正确性），
// 并等待它创建的所有 pod 进入 "Running" 状态。
// 清理工作由调用方在外部负责（例如通过 namespace 的生命周期来处理清理）。
func RunDeployment(ctx context.Context, config DeploymentConfig) error {
	err := config.create()
	if err != nil {
		return err
	}
	return config.start(ctx)
}

func (config *DeploymentConfig) create() error {
	deployment := &apps.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: config.Name,
		},
		Spec: apps.DeploymentSpec{
			Replicas: ptr.To[int32](int32(config.Replicas)),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"name": config.Name,
				},
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      map[string]string{"name": config.Name},
					Annotations: config.Annotations,
				},
				Spec: v1.PodSpec{
					Affinity:                      config.Affinity,
					TerminationGracePeriodSeconds: config.getTerminationGracePeriodSeconds(nil),
					Containers: []v1.Container{
						{
							Name:            config.Name,
							Image:           config.Image,
							Command:         config.Command,
							Ports:           []v1.ContainerPort{{ContainerPort: 80}},
							Lifecycle:       config.Lifecycle,
							SecurityContext: config.SecurityContext,
						},
					},
				},
			},
		},
	}

	if config.PodResources != nil {
		deployment.Spec.Template.Spec.Resources = config.PodResources.DeepCopy()
	}

	if len(config.AdditionalContainers) > 0 {
		deployment.Spec.Template.Spec.Containers = append(deployment.Spec.Template.Spec.Containers, config.AdditionalContainers...)
	}

	if len(config.SecretNames) > 0 {
		attachSecrets(&deployment.Spec.Template, config.SecretNames)
	}
	if len(config.ConfigMapNames) > 0 {
		attachConfigMaps(&deployment.Spec.Template, config.ConfigMapNames)
	}

	for i := 0; i < config.ServiceAccountTokenProjections; i++ {
		attachServiceAccountTokenProjection(&deployment.Spec.Template, fmt.Sprintf("tok-%d", i))
	}

	config.applyTo(&deployment.Spec.Template)

	if err := CreateDeploymentWithRetries(config.Client, config.Namespace, deployment); err != nil {
		return fmt.Errorf("error creating deployment: %v", err)
	}
	config.RCConfigLog("Created deployment with name: %v, namespace: %v, replica count: %v", deployment.Name, config.Namespace, ptr.Deref(deployment.Spec.Replicas, 0))
	return nil
}

// RunReplicaSet 启动一个 ReplicaSet（并验证其正确性），
// 并等待它创建的所有 pod 进入 "Running" 状态。
// 清理工作由调用方在外部负责（例如通过 namespace 的生命周期来处理清理）。
func RunReplicaSet(ctx context.Context, config ReplicaSetConfig) error {
	err := config.create()
	if err != nil {
		return err
	}
	return config.start(ctx)
}

func (config *ReplicaSetConfig) create() error {
	rs := &apps.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: config.Name,
		},
		Spec: apps.ReplicaSetSpec{
			Replicas: ptr.To[int32](int32(config.Replicas)),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"name": config.Name,
				},
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      map[string]string{"name": config.Name},
					Annotations: config.Annotations,
				},
				Spec: v1.PodSpec{
					Affinity:                      config.Affinity,
					TerminationGracePeriodSeconds: config.getTerminationGracePeriodSeconds(nil),
					Containers: []v1.Container{
						{
							Name:            config.Name,
							Image:           config.Image,
							Command:         config.Command,
							Ports:           []v1.ContainerPort{{ContainerPort: 80}},
							Lifecycle:       config.Lifecycle,
							SecurityContext: config.SecurityContext,
						},
					},
				},
			},
		},
	}

	if config.PodResources != nil {
		rs.Spec.Template.Spec.Resources = config.PodResources.DeepCopy()
	}

	if len(config.AdditionalContainers) > 0 {
		rs.Spec.Template.Spec.Containers = append(rs.Spec.Template.Spec.Containers, config.AdditionalContainers...)
	}

	if len(config.SecretNames) > 0 {
		attachSecrets(&rs.Spec.Template, config.SecretNames)
	}
	if len(config.ConfigMapNames) > 0 {
		attachConfigMaps(&rs.Spec.Template, config.ConfigMapNames)
	}

	config.applyTo(&rs.Spec.Template)

	if err := CreateReplicaSetWithRetries(config.Client, config.Namespace, rs); err != nil {
		return fmt.Errorf("error creating replica set: %v", err)
	}
	config.RCConfigLog("Created replica set with name: %v, namespace: %v, replica count: %v", rs.Name, config.Namespace, ptr.Deref(rs.Spec.Replicas, 0))
	return nil
}

// RunRC 启动一个 Replication Controller（并验证其正确性），
// 并等待它创建的所有 pod 进入 "Running" 状态。
// 清理工作由调用方在外部负责（例如通过 namespace 的生命周期来处理清理）。
func RunRC(ctx context.Context, config RCConfig) error {
	err := config.create()
	if err != nil {
		return err
	}
	return config.start(ctx)
}

func (config *RCConfig) create() error {
	dnsDefault := v1.DNSDefault
	if config.DNSPolicy == nil {
		config.DNSPolicy = &dnsDefault
	}
	one := int64(1)
	rc := &v1.ReplicationController{
		ObjectMeta: metav1.ObjectMeta{
			Name: config.Name,
		},
		Spec: v1.ReplicationControllerSpec{
			Replicas: ptr.To[int32](int32(config.Replicas)),
			Selector: map[string]string{
				"name": config.Name,
			},
			Template: &v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      map[string]string{"name": config.Name},
					Annotations: config.Annotations,
				},
				Spec: v1.PodSpec{
					SchedulerName: config.SchedulerName,
					Affinity:      config.Affinity,
					Containers: []v1.Container{
						{
							Name:            config.Name,
							Image:           config.Image,
							Command:         config.Command,
							Ports:           []v1.ContainerPort{{ContainerPort: 80}},
							ReadinessProbe:  config.ReadinessProbe,
							Lifecycle:       config.Lifecycle,
							SecurityContext: config.SecurityContext,
						},
					},
					DNSPolicy:                     *config.DNSPolicy,
					NodeSelector:                  config.NodeSelector,
					Tolerations:                   config.Tolerations,
					TerminationGracePeriodSeconds: config.getTerminationGracePeriodSeconds(&one),
					PriorityClassName:             config.PriorityClassName,
				},
			},
		},
	}

	if config.PodResources != nil {
		rc.Spec.Template.Spec.Resources = config.PodResources.DeepCopy()
	}

	if len(config.AdditionalContainers) > 0 {
		rc.Spec.Template.Spec.Containers = append(rc.Spec.Template.Spec.Containers, config.AdditionalContainers...)
	}

	if len(config.SecretNames) > 0 {
		attachSecrets(rc.Spec.Template, config.SecretNames)
	}
	if len(config.ConfigMapNames) > 0 {
		attachConfigMaps(rc.Spec.Template, config.ConfigMapNames)
	}

	config.applyTo(rc.Spec.Template)

	if err := CreateRCWithRetries(config.Client, config.Namespace, rc); err != nil {
		return fmt.Errorf("error creating replication controller: %v", err)
	}
	config.RCConfigLog("Created replication controller with name: %v, namespace: %v, replica count: %v", rc.Name, config.Namespace, ptr.Deref(rc.Spec.Replicas, 0))
	return nil
}

func (config *RCConfig) applyTo(template *v1.PodTemplateSpec) {
	for k, v := range config.Env {
		c := &template.Spec.Containers[0]
		c.Env = append(c.Env, v1.EnvVar{Name: k, Value: v})
	}
	for k, v := range config.Labels {
		template.ObjectMeta.Labels[k] = v
	}
	template.Spec.NodeSelector = make(map[string]string)
	for k, v := range config.NodeSelector {
		template.Spec.NodeSelector[k] = v
	}
	if config.Tolerations != nil {
		template.Spec.Tolerations = append([]v1.Toleration{}, config.Tolerations...)
	}
	for k, v := range config.Ports {
		c := &template.Spec.Containers[0]
		c.Ports = append(c.Ports, v1.ContainerPort{Name: k, ContainerPort: int32(v)})
	}
	for k, v := range config.HostPorts {
		c := &template.Spec.Containers[0]
		c.Ports = append(c.Ports, v1.ContainerPort{Name: k, ContainerPort: int32(v), HostPort: int32(v)})
	}
	if config.CPULimit > 0 || config.MemLimit > 0 || config.GpuLimit > 0 {
		template.Spec.Containers[0].Resources.Limits = v1.ResourceList{}
	}
	if config.CPULimit > 0 {
		template.Spec.Containers[0].Resources.Limits[v1.ResourceCPU] = *resource.NewMilliQuantity(config.CPULimit, resource.DecimalSI)
	}
	if config.MemLimit > 0 {
		template.Spec.Containers[0].Resources.Limits[v1.ResourceMemory] = *resource.NewQuantity(config.MemLimit, resource.DecimalSI)
	}
	if config.CPURequest > 0 || config.MemRequest > 0 {
		template.Spec.Containers[0].Resources.Requests = v1.ResourceList{}
	}
	if config.CPURequest > 0 {
		template.Spec.Containers[0].Resources.Requests[v1.ResourceCPU] = *resource.NewMilliQuantity(config.CPURequest, resource.DecimalSI)
	}
	if config.MemRequest > 0 {
		template.Spec.Containers[0].Resources.Requests[v1.ResourceMemory] = *resource.NewQuantity(config.MemRequest, resource.DecimalSI)
	}
	if config.GpuLimit > 0 {
		template.Spec.Containers[0].Resources.Limits["nvidia.com/gpu"] = *resource.NewQuantity(config.GpuLimit, resource.DecimalSI)
	}
	if config.Lifecycle != nil {
		template.Spec.Containers[0].Lifecycle = config.Lifecycle
	}
	if len(config.Volumes) > 0 {
		template.Spec.Volumes = config.Volumes
	}
	if len(config.VolumeMounts) > 0 {
		template.Spec.Containers[0].VolumeMounts = config.VolumeMounts
	}
	if config.PriorityClassName != "" {
		template.Spec.PriorityClassName = config.PriorityClassName
	}
}

type RCStartupStatus struct {
	Expected              int
	Terminating           int
	Running               int
	RunningButNotReady    int
	Waiting               int
	Pending               int
	Scheduled             int
	Unknown               int
	Inactive              int
	FailedContainers      int
	Created               []*v1.Pod
	ContainerRestartNodes sets.String
}

func (s *RCStartupStatus) String(name string) string {
	return fmt.Sprintf("%v Pods: %d out of %d created, %d running, %d pending, %d waiting, %d inactive, %d terminating, %d unknown, %d runningButNotReady ",
		name, len(s.Created), s.Expected, s.Running, s.Pending, s.Waiting, s.Inactive, s.Terminating, s.Unknown, s.RunningButNotReady)
}

func computeRCStartupStatus(pods []*v1.Pod, expected int) RCStartupStatus {
	startupStatus := RCStartupStatus{
		Expected:              expected,
		Created:               make([]*v1.Pod, 0, expected),
		ContainerRestartNodes: sets.NewString(),
	}
	for _, p := range pods {
		if p.DeletionTimestamp != nil {
			startupStatus.Terminating++
			continue
		}
		startupStatus.Created = append(startupStatus.Created, p)
		if p.Status.Phase == v1.PodRunning {
			ready := false
			for _, c := range p.Status.Conditions {
				if c.Type == v1.PodReady && c.Status == v1.ConditionTrue {
					ready = true
					break
				}
			}
			if ready {
				// 只有当 pod 同时处于 ready 状态时才计为 running。
				startupStatus.Running++
			} else {
				startupStatus.RunningButNotReady++
			}
			for _, v := range FailedContainers(p) {
				startupStatus.FailedContainers = startupStatus.FailedContainers + v.Restarts
				startupStatus.ContainerRestartNodes.Insert(p.Spec.NodeName)
			}
		} else if p.Status.Phase == v1.PodPending {
			if p.Spec.NodeName == "" {
				startupStatus.Waiting++
			} else {
				startupStatus.Pending++
			}
		} else if p.Status.Phase == v1.PodSucceeded || p.Status.Phase == v1.PodFailed {
			startupStatus.Inactive++
		} else if p.Status.Phase == v1.PodUnknown {
			startupStatus.Unknown++
		}
		// 记录已调度 pod 的数量（用于计算 scheduler 吞吐量）。
		if p.Spec.NodeName != "" {
			startupStatus.Scheduled++
		}
	}
	return startupStatus
}

func (config *RCConfig) start(ctx context.Context) error {
	// 如果测试不关心容器重启，就不要强制让测试失败。
	var maxContainerFailures int
	if config.MaxContainerFailures == nil {
		maxContainerFailures = int(math.Max(1.0, float64(config.Replicas)*.01))
	} else {
		maxContainerFailures = *config.MaxContainerFailures
	}

	label := labels.SelectorFromSet(labels.Set(map[string]string{"name": config.Name}))

	ps, err := NewPodStore(config.Client, config.Namespace, label, fields.Everything())
	if err != nil {
		return err
	}
	defer ps.Stop()

	interval := config.PollInterval
	if interval <= 0 {
		interval = 10 * time.Second
	}
	timeout := config.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	oldPods := make([]*v1.Pod, 0)
	oldRunning := 0
	lastChange := time.Now()
	podDeletionsCount := 0
	for oldRunning != config.Replicas {
		time.Sleep(interval)

		pods := ps.List()
		startupStatus := computeRCStartupStatus(pods, config.Replicas)

		if config.CreatedPods != nil {
			*config.CreatedPods = startupStatus.Created
		}
		if !config.Silent {
			config.RCConfigLog("%s", startupStatus.String(config.Name))
		}

		if config.PodStatusFile != nil {
			fmt.Fprintf(config.PodStatusFile, "%d, running, %d, pending, %d, waiting, %d, inactive, %d, unknown, %d, runningButNotReady\n", startupStatus.Running, startupStatus.Pending, startupStatus.Waiting, startupStatus.Inactive, startupStatus.Unknown, startupStatus.RunningButNotReady)
		}

		if startupStatus.FailedContainers > maxContainerFailures {
			if config.NodeDumpFunc != nil {
				config.NodeDumpFunc(ctx, config.Client, startupStatus.ContainerRestartNodes.List(), config.RCConfigLog)
			}
			if config.ContainerDumpFunc != nil {
				// 获取失败容器的日志，以帮助诊断失败原因
				config.ContainerDumpFunc(ctx, config.Client, config.Namespace, config.RCConfigLog)
			}
			return fmt.Errorf("%d containers failed which is more than allowed %d", startupStatus.FailedContainers, maxContainerFailures)
		}

		diff := diff(oldPods, pods)
		deletedPods := diff.DeletedPods()
		podDeletionsCount += len(deletedPods)
		if podDeletionsCount > config.MaxAllowedPodDeletions {
			// 消失的 pod 数量超过了阈值
			err := fmt.Errorf("%d pods disappeared for %s: %v", podDeletionsCount, config.Name, strings.Join(deletedPods, ", "))
			config.RCConfigLog("%s", err.Error())
			config.RCConfigLog("%s", diff.String(sets.NewString()))
			return err
		}

		if len(pods) > len(oldPods) || startupStatus.Running > oldRunning {
			lastChange = time.Now()
		}
		oldPods = pods
		oldRunning = startupStatus.Running

		if time.Since(lastChange) > timeout {
			break
		}
	}

	if oldRunning != config.Replicas {
		// 只列出来自指定 replication controller 的 pod。
		options := metav1.ListOptions{LabelSelector: label.String()}
		if pods, err := config.Client.CoreV1().Pods(config.Namespace).List(ctx, options); err == nil {
			for _, pod := range pods.Items {
				config.RCConfigLog("Pod %s\t%s\t%s\t%s", pod.Name, pod.Spec.NodeName, pod.Status.Phase, pod.DeletionTimestamp)
			}
		} else {
			config.RCConfigLog("Can't list pod debug info: %v", err)
		}
		return fmt.Errorf("only %d pods started out of %d", oldRunning, config.Replicas)
	}
	return nil
}

// RunRC 的简化版本，它不创建 RC，而是直接创建普通 Pod。
// 可以选择等待 pod 开始运行（当 waitForRunning == true 时）。
// 副本数必须非零。
func StartPods(c clientset.Interface, replicas int, namespace string, podNamePrefix string,
	pod v1.Pod, waitForRunning bool, logFunc func(fmt string, args ...interface{})) error {
	// 没有要启动的 pod
	if replicas < 1 {
		panic("StartPods: number of replicas must be non-zero")
	}
	startPodsID := string(uuid.NewUUID()) // 以便为它们打标签并查找
	for i := range replicas {
		podName := fmt.Sprintf("%v-%v", podNamePrefix, i)
		pod.ObjectMeta.Name = podName
		pod.ObjectMeta.Labels["name"] = podName
		pod.ObjectMeta.Labels["startPodsID"] = startPodsID
		pod.Spec.Containers[0].Name = podName
		if err := CreatePodWithRetries(c, namespace, &pod); err != nil {
			return err
		}
	}
	logFunc("Waiting for running...")
	if waitForRunning {
		label := labels.SelectorFromSet(labels.Set(map[string]string{"startPodsID": startPodsID}))
		err := WaitForPodsWithLabelRunning(c, namespace, label)
		if err != nil {
			return fmt.Errorf("error waiting for %d pods to be running - probably a timeout: %v", replicas, err)
		}
	}
	return nil
}

// 最多等待 10 分钟，直到所有匹配的 pod 都进入 Running 状态，
// 并且至少存在一个匹配的 pod。
func WaitForPodsWithLabelRunning(c clientset.Interface, ns string, label labels.Selector) error {
	return WaitForEnoughPodsWithLabelRunning(c, ns, label, -1)
}

// 最多等待 10 分钟，直到至少 'replicas' 个 pod 处于 Running 状态，
// 并且至少存在一个匹配的 pod。如果 'replicas' < 0，则等待所有匹配的 pod 运行。
func WaitForEnoughPodsWithLabelRunning(c clientset.Interface, ns string, label labels.Selector, replicas int) error {
	running := false
	ps, err := NewPodStore(c, ns, label, fields.Everything())
	if err != nil {
		return err
	}
	defer ps.Stop()

	for start := time.Now(); time.Since(start) < 10*time.Minute; time.Sleep(5 * time.Second) {
		pods := ps.List()
		if len(pods) == 0 {
			continue
		}
		runningPodsCount := 0
		for _, p := range pods {
			if p.Status.Phase == v1.PodRunning {
				runningPodsCount++
			}
		}
		if (replicas < 0 && runningPodsCount < len(pods)) || (runningPodsCount < replicas) {
			continue
		}
		running = true
		break
	}
	if !running {
		return fmt.Errorf("timeout while waiting for pods with labels %q to be running", label.String())
	}
	return nil
}

// TODO：用其他方式挂载 secret：环境变量、image pull secret。
func attachSecrets(template *v1.PodTemplateSpec, secretNames []string) {
	volumes := make([]v1.Volume, 0, len(secretNames))
	mounts := make([]v1.VolumeMount, 0, len(secretNames))
	for _, name := range secretNames {
		volumes = append(volumes, v1.Volume{
			Name: name,
			VolumeSource: v1.VolumeSource{
				Secret: &v1.SecretVolumeSource{
					SecretName: name,
				},
			},
		})
		mounts = append(mounts, v1.VolumeMount{
			Name:      name,
			MountPath: fmt.Sprintf("/%v", name),
		})
	}

	template.Spec.Volumes = volumes
	template.Spec.Containers[0].VolumeMounts = mounts
}

// TODO：用其他方式挂载 configmap：环境变量。
func attachConfigMaps(template *v1.PodTemplateSpec, configMapNames []string) {
	volumes := make([]v1.Volume, 0, len(configMapNames))
	mounts := make([]v1.VolumeMount, 0, len(configMapNames))
	for _, name := range configMapNames {
		volumes = append(volumes, v1.Volume{
			Name: name,
			VolumeSource: v1.VolumeSource{
				ConfigMap: &v1.ConfigMapVolumeSource{
					LocalObjectReference: v1.LocalObjectReference{
						Name: name,
					},
				},
			},
		})
		mounts = append(mounts, v1.VolumeMount{
			Name:      name,
			MountPath: fmt.Sprintf("/%v", name),
		})
	}

	template.Spec.Volumes = volumes
	template.Spec.Containers[0].VolumeMounts = mounts
}

func attachServiceAccountTokenProjection(template *v1.PodTemplateSpec, name string) {
	template.Spec.Containers[0].VolumeMounts = append(template.Spec.Containers[0].VolumeMounts,
		v1.VolumeMount{
			Name:      name,
			MountPath: "/var/service-account-tokens/" + name,
		})

	template.Spec.Volumes = append(template.Spec.Volumes,
		v1.Volume{
			Name: name,
			VolumeSource: v1.VolumeSource{
				Projected: &v1.ProjectedVolumeSource{
					Sources: []v1.VolumeProjection{
						{
							ServiceAccountToken: &v1.ServiceAccountTokenProjection{
								Path:     "token",
								Audience: name,
							},
						},
						{
							ConfigMap: &v1.ConfigMapProjection{
								LocalObjectReference: v1.LocalObjectReference{
									Name: "kube-root-ca-crt",
								},
								Items: []v1.KeyToPath{
									{
										Key:  "ca.crt",
										Path: "ca.crt",
									},
								},
							},
						},
						{
							DownwardAPI: &v1.DownwardAPIProjection{
								Items: []v1.DownwardAPIVolumeFile{
									{
										Path: "namespace",
										FieldRef: &v1.ObjectFieldSelector{
											APIVersion: "v1",
											FieldPath:  "metadata.namespace",
										},
									},
								},
							},
						},
					},
				},
			},
		})
}
