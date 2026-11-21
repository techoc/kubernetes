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

// ### 注意 ###
//
// ReplicationManager 现在只是 ReplicaSetController 的一个包装层，通过适配层把
// ReplicationController 当作 ReplicaSet 的旧版 API 来处理。
//
// 但 RC 与 RS 仍使用各自的存储，并分别实例化各自的 ReplicaSetController 对象。

package replication

import (
	"context"

	v1 "k8s.io/api/core/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/replicaset"
	consistencyutil "k8s.io/kubernetes/pkg/controller/util/consistency"
)

const (
	BurstReplicas = replicaset.BurstReplicas
)

// ReplicationManager 负责将系统中的 ReplicationController 对象与实际运行的 Pod 保持同步，
// 实际上只是 ReplicaSetController 的一层包装。
type ReplicationManager struct {
	replicaset.ReplicaSetController
}

// NewReplicationManager 用给定的 Informer 与客户端构造 ReplicationManager。
func NewReplicationManager(ctx context.Context, podInformer coreinformers.PodInformer, rcInformer coreinformers.ReplicationControllerInformer, kubeClient clientset.Interface, burstReplicas int) *ReplicationManager {
	logger := klog.FromContext(ctx)
	eventBroadcaster := record.NewBroadcaster(record.WithContext(ctx))
	// 复用 ReplicaSetController 的实现，通过 informer/client/podControl 适配器屏蔽 RC 与 RS 的 API 差异。
	return &ReplicationManager{
		*replicaset.NewBaseController(logger, informerAdapter{rcInformer}, podInformer, clientsetAdapter{kubeClient}, burstReplicas,
			v1.SchemeGroupVersion.WithKind("ReplicationController"),
			"replication_controller",
			"replicationmanager",
			podControlAdapter{controller.RealPodControl{
				KubeClient: kubeClient,
				Recorder:   eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "replication-controller"}),
			}},
			eventBroadcaster,
			replicaset.ReplicaSetControllerFeatures{
				// RC API 不支持 .status.terminatingReplicas 字段；RS 支持该字段，并向上传播给 Deployment 以支持上层功能，因此这里必须显式关闭。
				EnableStatusTerminatingReplicas: false,
			},
			// TODO: Replication controller does not currently support stale controller consistency.
			// In order to support stale controller consistency, we would need to parameterize the metrics
			// and resource types passed to the consistency store.
			consistencyutil.NewNoopConsistencyStore(),
		),
	}
}
