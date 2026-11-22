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

// LeaderElectionRecord is the record that is stored in the leader election annotation.
// This information should be used for observational purposes only and could be replaced
// with a random string (e.g. UUID) with only slight modification of this code.
// TODO(mikedanese): this should potentially be versioned
type LeaderElectionRecord struct {
	// HolderIdentity is the ID that owns the lease. If empty, no one owns this lease and
	// all callers may acquire. Versions of this library prior to Kubernetes 1.14 will not
	// attempt to acquire leases with empty identities and will wait for the full lease
	// interval to expire before attempting to reacquire. This value is set to empty when
	// a client voluntarily steps down.
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

// EventRecorder records a change in the ResourceLock.
// 实现方通常传入 events 客户端；为 nil 时不应记录事件。
type EventRecorder interface {
	Eventf(obj runtime.Object, eventType, reason, message string, args ...interface{})
}

// ResourceLockConfig common data that exists across different
// resource locks
type ResourceLockConfig struct {
	// Identity is the unique string identifying a lease holder across
	// all participants in an election.
	Identity string
	// EventRecorder is optional.
	EventRecorder EventRecorder
}

// Interface offers a common interface for locking on arbitrary
// resources used in leader election.  The Interface is used
// to hide the details on specific implementations in order to allow
// them to change over time.  This interface is strictly for use
// by the leaderelection code.
//
// Get 同时返回原始字节，供调用方判断记录是否变化而无需深比较。
type Interface interface {
	// Get returns the LeaderElectionRecord
	Get(ctx context.Context) (*LeaderElectionRecord, []byte, error)

	// Create attempts to create a LeaderElectionRecord
	Create(ctx context.Context, ler LeaderElectionRecord) error

	// Update will update and existing LeaderElectionRecord
	Update(ctx context.Context, ler LeaderElectionRecord) error

	// RecordEvent is used to record events
	RecordEvent(string)

	// Identity will return the locks Identity
	Identity() string

	// Describe is used to convert details on current resource lock
	// into a string
	Describe() string
}

// new will create a lock of a given type according to the input parameters
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

// New will create a lock of a given type according to the input parameters
func New(lockType string, ns string, name string, coreClient corev1.CoreV1Interface, coordinationClient coordinationv1.CoordinationV1Interface, rlc ResourceLockConfig) (Interface, error) {
	return new(lockType, ns, name, coreClient, coordinationClient, rlc, nil)
}

// NewWithLabels will create a lock of a given type according to the input parameters
// When the holder of the lock changes, that holder will apply their labels
func NewWithLabels(lockType string, ns string, name string, coreClient corev1.CoreV1Interface, coordinationClient coordinationv1.CoordinationV1Interface, rlc ResourceLockConfig, labels map[string]string) (Interface, error) {
	return new(lockType, ns, name, coreClient, coordinationClient, rlc, labels)
}

// NewFromKubeconfig will create a lock of a given type according to the input parameters.
// Timeout set for a client used to contact to Kubernetes should be lower than
// RenewDeadline to keep a single hung request from forcing a leader loss.
// Setting it to max(time.Second, RenewDeadline/2) as a reasonable heuristic.
func NewFromKubeconfig(lockType string, ns string, name string, rlc ResourceLockConfig, kubeconfig *restclient.Config, renewDeadline time.Duration) (Interface, error) {
	// shallow copy, do not modify the kubeconfig
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
