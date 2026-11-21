/*
Copyright 2023 The Kubernetes Authors.

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

package systemnamespaces

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"

	"k8s.io/klog/v2"
)

// Controller 确保系统命名空间存在。
type Controller struct {
	client kubernetes.Interface

	namespaceLister listers.NamespaceLister
	namespaceSynced cache.InformerSynced

	systemNamespaces []string
	interval         time.Duration
}

// NewController 创建用于确保系统命名空间存在的 Controller。
func NewController(systemNamespaces []string, clientset kubernetes.Interface, namespaceInformer coreinformers.NamespaceInformer) *Controller {
	interval := 1 * time.Minute

	return &Controller{
		client:           clientset,
		namespaceLister:  namespaceInformer.Lister(),
		namespaceSynced:  namespaceInformer.Informer().HasSynced,
		systemNamespaces: systemNamespaces,
		interval:         interval,
	}
}

// Run 启动一个 worker。
func (c *Controller) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer klog.Infof("Shutting down system namespaces controller")

	klog.Infof("Starting system namespaces controller")

	if !cache.WaitForCacheSync(stopCh, c.namespaceSynced) {
		utilruntime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
		return
	}

	go wait.Until(c.sync, c.interval, stopCh)

	<-stopCh
}

// sync 检查并创建所有系统命名空间。
func (c *Controller) sync() {
	// 遍历系统命名空间列表，不存在则创建。
	for _, ns := range c.systemNamespaces {
		if err := c.createNamespaceIfNeeded(ns); err != nil {
			utilruntime.HandleError(fmt.Errorf("unable to create required kubernetes system Namespace %s: %v", ns, err))
		}
	}
}

// createNamespaceIfNeeded 在命名空间不存在时创建它。
func (c *Controller) createNamespaceIfNeeded(ns string) error {
	if _, err := c.namespaceLister.Get(ns); err == nil {
		// 命名空间已存在。
		return nil
	}
	newNs := &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ns,
			Namespace: "",
		},
	}
	_, err := c.client.CoreV1().Namespaces().Create(context.TODO(), newNs, metav1.CreateOptions{})
	if err != nil && errors.IsAlreadyExists(err) {
		err = nil
	}
	return err
}
