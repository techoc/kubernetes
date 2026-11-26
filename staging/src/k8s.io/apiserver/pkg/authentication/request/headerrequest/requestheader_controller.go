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

// RequestHeaderAuthRequestProvider 能动态填充 RequestHeaderConfig 的部分字段。
type RequestHeaderAuthRequestProvider interface {
	// UsernameHeaders 列出按顺序查找用户名的 header。
	UsernameHeaders() []string
	// UIDHeaders 列出按顺序查找用户 UID 的 header。
	UIDHeaders() []string
	// GroupHeaders 列出其值被视为用户组的 header。
	GroupHeaders() []string
	// ExtraHeaderPrefixes 列出被映射到 user extra 的 header 前缀。
	ExtraHeaderPrefixes() []string
	// AllowedClientNames 限制哪些客户端证书可以发送这些 header。
	AllowedClientNames() []string
}

var _ RequestHeaderAuthRequestProvider = &RequestHeaderAuthRequestController{}

// requestHeaderBundle 是 ConfigMap 数据的不可变快照，
// 通过 atomic.Value 整体存储，读者不会看到部分更新。
type requestHeaderBundle struct {
	UsernameHeaders     []string
	UIDHeaders          []string
	GroupHeaders        []string
	ExtraHeaderPrefixes []string
	AllowedClientNames  []string
}

// RequestHeaderAuthRequestController 提供动态填充 RequestHeaderConfig 部分字段的方法。
// 这些方法的数据来自本控制器监听的 ConfigMap。
// 对不想动态响应配置变化的组件，构造时会从服务端预取一次初始数据。
// 来自 ConfigMap。
type RequestHeaderAuthRequestController struct {
	name string

	configmapName      string
	configmapNamespace string

	client                  kubernetes.Interface
	configmapLister         corev1listers.ConfigMapNamespaceLister
	configmapInformer       cache.SharedIndexInformer
	configmapInformerSynced cache.InformerSynced

	queue workqueue.TypedRateLimitingInterface[string]

	// exportedRequestHeaderBundle 保存最近一次读取到的非空 ConfigMap 内容。
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

// NewRequestHeaderAuthRequestController 创建一个实现 RequestHeaderAuthRequestController 的控制器。
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

	// 自建 informer 是因为只需要很小一部分信息：单个命名空间中的单个 ConfigMap。
	//
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
			return true //  conservatively always return true；检查成本很低。
		},
		Handler: cache.ResourceEventHandlerFuncs{
			// 既然设有过滤函数，被调用时直接入队即可。只关注一个 ConfigMap，
			// 无需对 key 挑三拣四。
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

// UsernameHeaders 返回配置的用户名 header 列表；ConfigMap 尚未加载时返回 nil。
// ConfigMap 已加载。
func (c *RequestHeaderAuthRequestController) UsernameHeaders() []string {
	return c.loadRequestHeaderFor(c.usernameHeadersKey)
}

// UIDHeaders 返回配置的 UID header 列表；加载前返回 nil。
func (c *RequestHeaderAuthRequestController) UIDHeaders() []string {
	return c.loadRequestHeaderFor(c.uidHeadersKey)
}

// GroupHeaders 返回配置的组 header 列表；加载前返回 nil。
func (c *RequestHeaderAuthRequestController) GroupHeaders() []string {
	return c.loadRequestHeaderFor(c.groupHeadersKey)
}

// ExtraHeaderPrefixes 返回配置的 extra header 前缀；加载前返回 nil。
func (c *RequestHeaderAuthRequestController) ExtraHeaderPrefixes() []string {
	return c.loadRequestHeaderFor(c.extraHeaderPrefixesKey)
}

// AllowedClientNames 返回配置的允许客户端名；加载前返回 nil。
func (c *RequestHeaderAuthRequestController) AllowedClientNames() []string {
	return c.loadRequestHeaderFor(c.allowedClientNamesKey)
}

// Run 启动 RequestHeaderAuthRequestController，并阻塞直到 stopCh 关闭。
//
// 只启动一个 worker：全部工作都是同步同一个 ConfigMap，多 worker 只会增加
// 无谓的锁竞争。workers 参数仅为满足通用 controller 签名而保留。
func (c *RequestHeaderAuthRequestController) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrashWithContext(ctx)
	defer c.queue.ShutDown()

	klog.Infof("Starting %s", c.name)
	defer klog.Infof("Shutting down %s", c.name)

	go c.configmapInformer.Run(ctx.Done())

	// 开始前等待缓存填充完毕。
	if !cache.WaitForNamedCacheSyncWithContext(ctx, c.configmapInformerSynced) {
		return
	}

	//  worker 数量无关紧要，只启动一个。
	go wait.Until(c.runWorker, time.Second, ctx.Done())

	<-ctx.Done()
}

// // RunOnce 执行一次同步循环
//
// 供不关心动态更新的组件在启动时做一次同步；NotFound 视为正常（尚未部署
// ConfigMap），Forbidden 则给出可操作的 RBAC 提示后返回错误。
func (c *RequestHeaderAuthRequestController) RunOnce(ctx context.Context) error {
	configMap, err := c.client.CoreV1().ConfigMaps(c.configmapNamespace).Get(ctx, c.configmapName, metav1.GetOptions{})
	switch {
	case errors.IsNotFound(err):
		// 忽略：authConfigMap 现在为 nil。
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

// runWorker 消费工作队列，直到队列被关闭。
func (c *RequestHeaderAuthRequestController) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem 处理一个队列条目；出错时按速率限制重新入队，
// 避免短暂的 API 故障把控制器卡死。
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

// sync 读取配置并把变更传播到 exportedRequestHeaderBundle，
// 后者由一组用于填充 RequestHeaderConfig 的方法暴露。
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

// syncConfigMap 解析 ConfigMap，仅在解析内容确实变化时原子替换已发布的 bundle。
// 仅在解析内容确实变化时。
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

// hasRequestHeaderBundleChanged 比较 ConfigMap 与当前已发布 bundle。
// 解析错误会中止整个同步：绝不发布只反序列化了一半的配置。
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

	// 检查是否有变更。若值相同则不处理。
	loadedHeadersBundle, ok := rawHeaderBundle.(*requestHeaderBundle)
	if !ok {
		return true, currentHeadersBundle, nil
	}

	if !equality.Semantic.DeepEqual(loadedHeadersBundle, currentHeadersBundle) {
		return true, currentHeadersBundle, nil
	}
	return false, nil, nil
}

// getRequestHeaderBundleFromConfigMap 反序列化每个配置的 key。
// 缺失的 key 反序列化为 nil 而不是错误，这是禁用某类 header 的约定方式。
// 禁用某类 header。
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

// loadRequestHeaderFor 从当前快照读取请求的字段。
// 它在请求路径上调用，因此必须尽量少分配内存且无锁。
// lock-free.
func (c *RequestHeaderAuthRequestController) loadRequestHeaderFor(key string) []string {
	rawHeaderBundle := c.exportedRequestHeaderBundle.Load()
	if rawHeaderBundle == nil {
		return nil // 当因某种原因一直无法从 apiserver 加载数据时会出现这种情况。
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

// keyFn 为唯一的 ConfigMap 生成队列 key。
func (c *RequestHeaderAuthRequestController) keyFn() string {
	// 这个格式与 DeletionHandlingMetaNamespaceKeyFunc 对单个 key 的处理一致。
	return c.configmapNamespace + "/" + c.configmapName
}

// deserializeStrings 解码一个 JSON 字符串数组。空字符串表示 key 不存在，
// 此时返回 nil 且无错误。
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
