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

package config

import (
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// WaitForAPIServerSyncPeriod 是检查节点 list/watch 初次同步是否完成的间隔。
const WaitForAPIServerSyncPeriod = 1 * time.Second

// NewSourceApiserver 创建从 apiserver 监听并拉取 Pod 配置的数据源。
func NewSourceApiserver(logger klog.Logger, c clientset.Interface, nodeName types.NodeName, nodeHasSynced func() bool, updates chan<- sourceUpdate) {
	lw := cache.NewListWatchFromClient(c.CoreV1().RESTClient(), "pods", metav1.NamespaceAll, fields.OneTermEqualSelector("spec.nodeName", string(nodeName)))

	// 负责监听 apiserver Pod 的 Reflector 只能在节点与 apiserver 同步完成后启动。
	logger.Info("Waiting for node sync before watching apiserver pods")
	// 必须先等节点与 apiserver 同步完成，否则 Reflector 可能拿不到完整 Pod 列表。
	go func() {
		for {
			if nodeHasSynced() {
				logger.V(4).Info("node sync completed")
				break
			}
			time.Sleep(WaitForAPIServerSyncPeriod)
			logger.V(4).Info("node sync has not completed yet")
		}
		logger.Info("Watching apiserver")
		newSourceApiserverFromLW(lw, updates)
	}()
}

// newSourceApiserverFromLW 基于 ListerWatcher 创建从 apiserver 监听并拉取配置的数据源。
func newSourceApiserverFromLW(lw cache.ListerWatcher, updates chan<- sourceUpdate) {
	send := func(objs []interface{}) {
		var pods []*v1.Pod
		for _, o := range objs {
			pods = append(pods, o.(*v1.Pod))
		}
		updates <- sourceUpdate{Pods: pods}
	}
	r := cache.NewReflector(lw, &v1.Pod{}, cache.NewUndeltaStore(send, cache.MetaNamespaceKeyFunc), 0)
	go r.Run(wait.NeverStop)
}
