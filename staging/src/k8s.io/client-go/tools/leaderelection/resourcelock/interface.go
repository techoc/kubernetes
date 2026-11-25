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

package resourcelock

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	coordinationv1 "k8s.io/client-go/kubernetes/typed/coordination/v1"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	restclient "k8s.io/client-go/rest"
)

const (
	// LeaderElectionRecordAnnotationKey 是旧版 endpoints/configmaps 锁存放记录用的注解键；
	// 现在仅用于兼容读取历史数据。
	LeaderElectionRecordAnnotationKey = "control-plane.alpha.kubernetes.io/leader"
	// 以下旧锁类型已移除，仅保留常量用于给出迁移提示。
	endpointsResourceLock        = "endpoints"
	configMapsResourceLock       = "configmaps"
	LeasesResourceLock           = "leases"
	endpointsLeasesResourceLock  = "endpointsleases"
	configMapsLeasesResourceLock = "configmapsleases"
)

// LeaderElectionRecord 是存储在领导者选举注解中的记录。
// 该信息只应用于观察目的，稍作改动本代码即可将其替换为随机字符串（如 UUID）。
// TODO(mikedanese): 这个结构可能需要做版本化
type LeaderElectionRecord struct {
	// HolderIdentity 是持有该租约的 ID。为空时表示无人持有此租约，所有调用者
	// 均可获取。Kubernetes 1.14 之前的本库版本不会尝试获取 identity 为空的租约，
	// 而是等待整个租约周期过期后再重新获取。客户端主动卸任时该值会被置空。
	HolderIdentity       string      `json:"holderIdentity"`
	LeaseDurationSeconds int         `json:"leaseDurationSeconds"`
	AcquireTime          metav1.Time `json:"acquireTime"`
	RenewTime            metav1.Time `json:"renewTime"`
	// LeaderTransitions 统计 leader 变更次数，用于观测与调试。
	LeaderTransitions int `json:"leaderTransitions"`
	// Strategy 与 PreferredHolder 仅在协调式选举（alpha）中使用。
	Strategy        v1.CoordinatedLeaseStrategy `json:"strategy"`
	PreferredHolder string                      `json:"preferredHolder"`
}

// EventRecorder 记录 ResourceLock 中的变化。
// 实现方通常传入 events 客户端；为 nil 时不应记录事件。
type EventRecorder interface {
	Eventf(obj runtime.Object, eventType, reason, message string, args ...interface{})
}

// ResourceLockConfig 是跨不同资源锁共用的公共数据
type ResourceLockConfig struct {
	// Identity 是在选举的所有参与者中唯一标识租约持有者的字符串。
	Identity string
	// EventRecorder 是可选的。
	EventRecorder EventRecorder
}

// Interface 为领导者选举中任意资源的加锁提供统一接口，
// 用它屏蔽具体实现细节，以便实现能随时间演进。
// 该接口仅供 leaderelection 代码使用。
// Get 同时返回原始字节，供调用方判断记录是否变化而无需深比较。
type Interface interface {
	// Get 返回 LeaderElectionRecord
	Get(ctx context.Context) (*LeaderElectionRecord, []byte, error)

	// Create 尝试创建 LeaderElectionRecord
	Create(ctx context.Context, ler LeaderElectionRecord) error

	// Update 更新已有的 LeaderElectionRecord
	Update(ctx context.Context, ler LeaderElectionRecord) error

	// RecordEvent 用于记录事件
	RecordEvent(string)

	// Identity 返回锁的 Identity
	Identity() string

	// Describe 把当前资源锁的细节转换为字符串
	Describe() string
}

// new 根据输入参数创建指定类型的锁
// 目前只支持 Lease 锁；旧类型返回错误以引导用户迁移。
func new(lockType string, ns string, name string, coreClient corev1.CoreV1Interface, coordinationClient coordinationv1.CoordinationV1Interface, rlc ResourceLockConfig, labels map[string]string) (Interface, error) {
	leaseLock := &LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      name,
		},
		Client:     coordinationClient,
		LockConfig: rlc,
		Labels:     labels,
	}
	switch lockType {
	case endpointsResourceLock:
		return nil, fmt.Errorf("endpoints lock is removed, migrate to %s", LeasesResourceLock)
	case configMapsResourceLock:
		return nil, fmt.Errorf("configmaps lock is removed, migrate to %s", LeasesResourceLock)
	case LeasesResourceLock:
		return leaseLock, nil
	case endpointsLeasesResourceLock:
		return nil, fmt.Errorf("endpointsleases lock is removed, migrate to %s", LeasesResourceLock)
	case configMapsLeasesResourceLock:
		return nil, fmt.Errorf("configmapsleases lock is removed, migrated to %s", LeasesResourceLock)
	default:
		return nil, fmt.Errorf("Invalid lock-type %s", lockType)
	}
}

// New 根据输入参数创建指定类型的锁
func New(lockType string, ns string, name string, coreClient corev1.CoreV1Interface, coordinationClient coordinationv1.CoordinationV1Interface, rlc ResourceLockConfig) (Interface, error) {
	return new(lockType, ns, name, coreClient, coordinationClient, rlc, nil)
}

// NewWithLabels 根据输入参数创建指定类型的锁。
// 当锁的持有者变更时，新持有者会应用自己的 labels。
func NewWithLabels(lockType string, ns string, name string, coreClient corev1.CoreV1Interface, coordinationClient coordinationv1.CoordinationV1Interface, rlc ResourceLockConfig, labels map[string]string) (Interface, error) {
	return new(lockType, ns, name, coreClient, coordinationClient, rlc, labels)
}

// NewFromKubeconfig 根据输入参数创建指定类型的锁。
// 连接 Kubernetes 的客户端超时应低于 RenewDeadline，
// 避免单个挂起的请求导致 leader 丢失。
// 这里取 max(time.Second, RenewDeadline/2) 作为合理的启发式值。
func NewFromKubeconfig(lockType string, ns string, name string, rlc ResourceLockConfig, kubeconfig *restclient.Config, renewDeadline time.Duration) (Interface, error) {
	// 浅拷贝，不修改传入的 kubeconfig
	// 仅复制结构体，不修改调用方传入的配置。
	config := *kubeconfig
	timeout := renewDeadline / 2
	if timeout < time.Second {
		timeout = time.Second
	}
	config.Timeout = timeout
	leaderElectionClient := clientset.NewForConfigOrDie(restclient.AddUserAgent(&config, "leader-election"))
	return New(lockType, ns, name, leaderElectionClient.CoreV1(), leaderElectionClient.CoordinationV1(), rlc)
}
