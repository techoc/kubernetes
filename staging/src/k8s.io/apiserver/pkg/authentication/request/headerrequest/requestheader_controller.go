/*
Copyright 2020 The Kubernetes Authors.

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

package headerrequest

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

const (
	authenticationRoleName = "extension-apiserver-authentication-reader"
)

// RequestHeaderAuthRequestProvider a provider that knows how to dynamically fill parts of RequestHeaderConfig struct
type RequestHeaderAuthRequestProvider interface {
	// UsernameHeaders lists the headers searched, in order, for the user name.
	UsernameHeaders() []string
	// UIDHeaders lists the headers searched, in order, for the user UID.
	UIDHeaders() []string
	// GroupHeaders lists the headers whose values are treated as groups.
	GroupHeaders() []string
	// ExtraHeaderPrefixes lists prefixes of headers mapped into user extras.
	ExtraHeaderPrefixes() []string
	// AllowedClientNames restricts which client certs may send these headers.
	AllowedClientNames() []string
}

var _ RequestHeaderAuthRequestProvider = &RequestHeaderAuthRequestController{}

// requestHeaderBundle is an immutable snapshot of the configmap data. It is
// stored wholesale via atomic.Value so readers never observe a partial update.
type requestHeaderBundle struct {
	UsernameHeaders     []string
	UIDHeaders          []string
	GroupHeaders        []string
	ExtraHeaderPrefixes []string
	AllowedClientNames  []string
}

// RequestHeaderAuthRequestController a controller that exposes a set of methods for dynamically filling parts of RequestHeaderConfig struct.
// The methods are sourced from the config map which is being monitored by this controller.
// The controller is primed from the server at the construction time for components that don't want to dynamically react to changes
// in the config map.
type RequestHeaderAuthRequestController struct {
	name string

	configmapName      string
	configmapNamespace string

	client                  kubernetes.Interface
	configmapLister         corev1listers.ConfigMapNamespaceLister
	configmapInformer       cache.SharedIndexInformer
	configmapInformerSynced cache.InformerSynced

	queue workqueue.TypedRateLimitingInterface[string]

	// exportedRequestHeaderBundle is a requestHeaderBundle that contains the last read, non-zero length content of the configmap
	//
	// 使用 atomic.Value 而非互斥锁：请求鉴权路径上的读取远多于写入，
	// 且读者总是看到完整的、不可变的快照。
	exportedRequestHeaderBundle atomic.Value

	usernameHeadersKey     string
	uidHeadersKey          string
	groupHeadersKey        string
	extraHeaderPrefixesKey string
	allowedClientNamesKey  string
}

// NewRequestHeaderAuthRequestController creates a new controller that implements RequestHeaderAuthRequestController
//
// 注意：controller 创建时不读取任何数据，调用方必须随后调用 RunOnce 或 Run，
// 否则所有 getter 都会返回 nil，等同于关闭了 request header 认证。
func NewRequestHeaderAuthRequestController(
	cmName string,
	cmNamespace string,
	client kubernetes.Interface,
	usernameHeadersKey, uidHeadersKey, groupHeadersKey, extraHeaderPrefixesKey, allowedClientNamesKey string) *RequestHeaderAuthRequestController {
	c := &RequestHeaderAuthRequestController{
		name: "RequestHeaderAuthRequestController",

		client: client,

		configmapName:      cmName,
		configmapNamespace: cmNamespace,

		usernameHeadersKey:     usernameHeadersKey,
		uidHeadersKey:          uidHeadersKey,
		groupHeadersKey:        groupHeadersKey,
		extraHeaderPrefixesKey: extraHeaderPrefixesKey,
		allowedClientNamesKey:  allowedClientNamesKey,

		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "RequestHeaderAuthRequestController"},
		),
	}

	// we construct our own informer because we need such a small subset of the information available.  Just one namespace.
	// 用字段选择器把关注范围缩到一个 ConfigMap，避免缓存整个命名空间；
	// 12 小时的 resync 只是兜底，实际变更由 watch 驱动。
	c.configmapInformer = coreinformers.NewFilteredConfigMapInformer(client, c.configmapNamespace, 12*time.Hour, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}, func(listOptions *metav1.ListOptions) {
		listOptions.FieldSelector = fields.OneTermEqualSelector("metadata.name", c.configmapName).String()
	})

	// 过滤函数对 DeletedFinalStateUnknown 也要解包判断，否则删除事件的
	// tombstone 会被误判为不相关而丢掉最后一次更新。
	c.configmapInformer.AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: func(obj interface{}) bool {
			if cast, ok := obj.(*corev1.ConfigMap); ok {
				return cast.Name == c.configmapName && cast.Namespace == c.configmapNamespace
			}
			if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				if cast, ok := tombstone.Obj.(*corev1.ConfigMap); ok {
					return cast.Name == c.configmapName && cast.Namespace == c.configmapNamespace
				}
			}
			return true // always return true just in case.  The checks are fairly cheap
		},
		Handler: cache.ResourceEventHandlerFuncs{
			// we have a filter, so any time we're called, we may as well queue. We only ever check one configmap
			// so we don't have to be choosy about our key.
			AddFunc: func(obj interface{}) {
				c.queue.Add(c.keyFn())
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				c.queue.Add(c.keyFn())
			},
			DeleteFunc: func(obj interface{}) {
				c.queue.Add(c.keyFn())
			},
		},
	})

	c.configmapLister = corev1listers.NewConfigMapLister(c.configmapInformer.GetIndexer()).ConfigMaps(c.configmapNamespace)
	c.configmapInformerSynced = c.configmapInformer.HasSynced

	return c
}

// UsernameHeaders returns the configured username header list, or nil before
// the configmap has been loaded.
func (c *RequestHeaderAuthRequestController) UsernameHeaders() []string {
	return c.loadRequestHeaderFor(c.usernameHeadersKey)
}

// UIDHeaders returns the configured UID header list, or nil before load.
func (c *RequestHeaderAuthRequestController) UIDHeaders() []string {
	return c.loadRequestHeaderFor(c.uidHeadersKey)
}

// GroupHeaders returns the configured group header list, or nil before load.
func (c *RequestHeaderAuthRequestController) GroupHeaders() []string {
	return c.loadRequestHeaderFor(c.groupHeadersKey)
}

// ExtraHeaderPrefixes returns the configured extra header prefixes, or nil before load.
func (c *RequestHeaderAuthRequestController) ExtraHeaderPrefixes() []string {
	return c.loadRequestHeaderFor(c.extraHeaderPrefixesKey)
}

// AllowedClientNames returns the configured allowed client names, or nil before load.
func (c *RequestHeaderAuthRequestController) AllowedClientNames() []string {
	return c.loadRequestHeaderFor(c.allowedClientNamesKey)
}

// Run starts RequestHeaderAuthRequestController controller and blocks until stopCh is closed.
//
// 只启动一个 worker：全部工作都是同步同一个 ConfigMap，多 worker 只会增加
// 无谓的锁竞争。workers 参数仅为满足通用 controller 签名而保留。
func (c *RequestHeaderAuthRequestController) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrashWithContext(ctx)
	defer c.queue.ShutDown()

	klog.Infof("Starting %s", c.name)
	defer klog.Infof("Shutting down %s", c.name)

	go c.configmapInformer.Run(ctx.Done())

	// wait for caches to fill before starting your work
	if !cache.WaitForNamedCacheSyncWithContext(ctx, c.configmapInformerSynced) {
		return
	}

	// doesn't matter what workers say, only start one.
	go wait.Until(c.runWorker, time.Second, ctx.Done())

	<-ctx.Done()
}

// // RunOnce runs a single sync loop
//
// 供不关心动态更新的组件在启动时做一次同步；NotFound 视为正常（尚未部署
// ConfigMap），Forbidden 则给出可操作的 RBAC 提示后返回错误。
func (c *RequestHeaderAuthRequestController) RunOnce(ctx context.Context) error {
	configMap, err := c.client.CoreV1().ConfigMaps(c.configmapNamespace).Get(ctx, c.configmapName, metav1.GetOptions{})
	switch {
	case errors.IsNotFound(err):
		// ignore, authConfigMap is nil now
		return nil
	case errors.IsForbidden(err):
		// 权限不足几乎总是 RBAC 配置问题，打印可直接照抄的修复命令。
		klog.Warningf("Unable to get configmap/%s in %s.  Usually fixed by "+
			"'kubectl create rolebinding -n %s ROLEBINDING_NAME --role=%s --serviceaccount=YOUR_NS:YOUR_SA'",
			c.configmapName, c.configmapNamespace, c.configmapNamespace, authenticationRoleName)
		return err
	case err != nil:
		return err
	}
	return c.syncConfigMap(configMap)
}

// runWorker drains the work queue until it is shut down.
func (c *RequestHeaderAuthRequestController) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem handles one queue entry, requeueing with rate limiting
// on error so a transient API failure does not wedge the controller.
func (c *RequestHeaderAuthRequestController) processNextWorkItem() bool {
	dsKey, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(dsKey)

	err := c.sync()
	if err == nil {
		c.queue.Forget(dsKey)
		return true
	}

	utilruntime.HandleError(fmt.Errorf("%v failed with : %v", dsKey, err))
	c.queue.AddRateLimited(dsKey)

	return true
}

// sync reads the config and propagates the changes to exportedRequestHeaderBundle
// which is exposed by the set of methods that are used to fill RequestHeaderConfig struct
//
// 从 lister 读取而非直接访问 API server，保证与 informer 缓存视图一致，
// 避免因 API 抖动读到比已处理事件更旧的数据。
func (c *RequestHeaderAuthRequestController) sync() error {
	configMap, err := c.configmapLister.Get(c.configmapName)
	if err != nil {
		return err
	}
	return c.syncConfigMap(configMap)
}

// syncConfigMap parses the configmap and atomically swaps the published bundle
// only when the parsed content actually changed.
func (c *RequestHeaderAuthRequestController) syncConfigMap(configMap *corev1.ConfigMap) error {
	hasChanged, newRequestHeaderBundle, err := c.hasRequestHeaderBundleChanged(configMap)
	if err != nil {
		return err
	}
	if hasChanged {
		c.exportedRequestHeaderBundle.Store(newRequestHeaderBundle)
		klog.V(2).Infof("Loaded a new request header values for %v", c.name)
	}
	return nil
}

// hasRequestHeaderBundleChanged compares the configmap against the currently
// published bundle. A parse error aborts the whole sync: we never publish a
// partially deserialized configuration.
func (c *RequestHeaderAuthRequestController) hasRequestHeaderBundleChanged(cm *corev1.ConfigMap) (bool, *requestHeaderBundle, error) {
	currentHeadersBundle, err := c.getRequestHeaderBundleFromConfigMap(cm)
	if err != nil {
		return false, nil, err
	}

	rawHeaderBundle := c.exportedRequestHeaderBundle.Load()
	if rawHeaderBundle == nil {
		return true, currentHeadersBundle, nil
	}

	// check to see if we have a change. If the values are the same, do nothing.
	loadedHeadersBundle, ok := rawHeaderBundle.(*requestHeaderBundle)
	if !ok {
		return true, currentHeadersBundle, nil
	}

	if !equality.Semantic.DeepEqual(loadedHeadersBundle, currentHeadersBundle) {
		return true, currentHeadersBundle, nil
	}
	return false, nil, nil
}

// getRequestHeaderBundleFromConfigMap deserializes each configured key. Missing
// keys deserialize to nil rather than an error, which is the documented way to
// disable a header class.
func (c *RequestHeaderAuthRequestController) getRequestHeaderBundleFromConfigMap(cm *corev1.ConfigMap) (*requestHeaderBundle, error) {
	usernameHeaderCurrentValue, err := deserializeStrings(cm.Data[c.usernameHeadersKey])
	if err != nil {
		return nil, err
	}

	uidHeaderCurrentValue, err := deserializeStrings(cm.Data[c.uidHeadersKey])
	if err != nil {
		return nil, err
	}

	groupHeadersCurrentValue, err := deserializeStrings(cm.Data[c.groupHeadersKey])
	if err != nil {
		return nil, err
	}

	extraHeaderPrefixesCurrentValue, err := deserializeStrings(cm.Data[c.extraHeaderPrefixesKey])
	if err != nil {
		return nil, err

	}

	allowedClientNamesCurrentValue, err := deserializeStrings(cm.Data[c.allowedClientNamesKey])
	if err != nil {
		return nil, err
	}

	return &requestHeaderBundle{
		UsernameHeaders:     usernameHeaderCurrentValue,
		UIDHeaders:          uidHeaderCurrentValue,
		GroupHeaders:        groupHeadersCurrentValue,
		ExtraHeaderPrefixes: extraHeaderPrefixesCurrentValue,
		AllowedClientNames:  allowedClientNamesCurrentValue,
	}, nil
}

// loadRequestHeaderFor reads the requested field out of the current snapshot.
// It is called on the request path, so it must stay allocation-light and
// lock-free.
func (c *RequestHeaderAuthRequestController) loadRequestHeaderFor(key string) []string {
	rawHeaderBundle := c.exportedRequestHeaderBundle.Load()
	if rawHeaderBundle == nil {
		return nil // this can happen if we've been unable load data from the apiserver for some reason
	}
	headerBundle := rawHeaderBundle.(*requestHeaderBundle)

	switch key {
	case c.usernameHeadersKey:
		return headerBundle.UsernameHeaders
	case c.uidHeadersKey:
		return headerBundle.UIDHeaders
	case c.groupHeadersKey:
		return headerBundle.GroupHeaders
	case c.extraHeaderPrefixesKey:
		return headerBundle.ExtraHeaderPrefixes
	case c.allowedClientNamesKey:
		return headerBundle.AllowedClientNames
	default:
		return nil
	}
}

// keyFn produces the queue key for our single ConfigMap.
func (c *RequestHeaderAuthRequestController) keyFn() string {
	// this format matches DeletionHandlingMetaNamespaceKeyFunc for our single key
	return c.configmapNamespace + "/" + c.configmapName
}

// deserializeStrings decodes a JSON string array. An empty string means the
// key is absent and yields nil with no error.
func deserializeStrings(in string) ([]string, error) {
	if len(in) == 0 {
		return nil, nil
	}
	var ret []string
	if err := json.Unmarshal([]byte(in), &ret); err != nil {
		return nil, err
	}
	return ret, nil
}
