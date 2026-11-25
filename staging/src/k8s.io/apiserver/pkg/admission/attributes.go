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

// Package admission 定义准入控制插件链、插件配置以及请求属性（Attributes）等核心抽象。
package admission

import (
	"fmt"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/apiserver/pkg/authentication/user"
)

// attributesRecord 是 Attributes 的默认实现，保存一次准入请求的只读上下文。
type attributesRecord struct {
	// 请求标识信息在整条准入链中不可变，因此无需加锁。
	kind        schema.GroupVersionKind
	namespace   string
	name        string
	resource    schema.GroupVersionResource
	subresource string
	operation   Operation
	options     runtime.Object
	dryRun      bool
	object      runtime.Object
	oldObject   runtime.Object
	userInfo    user.Info

	// 其余字段只在单个 goroutine 中访问；但 ValidatingAdmissionWebhook 会并发写 annotations，
	// 因此只有 annotations 需要加锁。
	annotations     map[string]annotation
	annotationsLock sync.RWMutex

	// reinvocationContext 记录本次请求的重入状态，并承载插件间共享值。
	reinvocationContext ReinvocationContext
}

// annotation 是带审计级别的一条准入注解。
type annotation struct {
	level auditinternal.Level
	value string
}

// NewAttributesRecord 构造一次准入请求的 Attributes；返回记录的主体在链中只读，
// 可变部分仅有 annotations 与 reinvocationContext。
func NewAttributesRecord(object runtime.Object, oldObject runtime.Object, kind schema.GroupVersionKind, namespace, name string, resource schema.GroupVersionResource, subresource string, operation Operation, operationOptions runtime.Object, dryRun bool, userInfo user.Info) Attributes {
	return &attributesRecord{
		kind:                kind,
		namespace:           namespace,
		name:                name,
		resource:            resource,
		subresource:         subresource,
		operation:           operation,
		options:             operationOptions,
		dryRun:              dryRun,
		object:              object,
		oldObject:           oldObject,
		userInfo:            userInfo,
		reinvocationContext: &reinvocationContext{},
	}
}

// GetKind 返回被准入对象的 GroupVersionKind。
func (record *attributesRecord) GetKind() schema.GroupVersionKind {
	return record.kind
}

// GetNamespace 返回对象所在命名空间；集群级资源为空。
func (record *attributesRecord) GetNamespace() string {
	return record.namespace
}

// GetName 返回对象名称。
func (record *attributesRecord) GetName() string {
	return record.name
}

// GetResource 返回对象对应的 GroupVersionResource。
func (record *attributesRecord) GetResource() schema.GroupVersionResource {
	return record.resource
}

// GetSubresource 返回子资源名，例如 "status"；无子资源时为空。
func (record *attributesRecord) GetSubresource() string {
	return record.subresource
}

// GetOperation 返回本次请求的操作类型（Create、Update、Delete 等）。
func (record *attributesRecord) GetOperation() Operation {
	return record.operation
}

// GetOperationOptions 返回操作附带的选项对象（如 DeleteOptions）。
func (record *attributesRecord) GetOperationOptions() runtime.Object {
	return record.options
}

// IsDryRun 返回是否为 dry-run 请求；dry-run 不应产生持久副作用。
func (record *attributesRecord) IsDryRun() bool {
	return record.dryRun
}

// GetObject 返回请求中的新对象；Delete 等操作可能为 nil。
func (record *attributesRecord) GetObject() runtime.Object {
	return record.object
}

// GetOldObject 返回更新前的旧对象；Create 时为 nil。
func (record *attributesRecord) GetOldObject() runtime.Object {
	return record.oldObject
}

// GetUserInfo 返回发起请求的用户信息。
func (record *attributesRecord) GetUserInfo() user.Info {
	return record.userInfo
}

// getAnnotations 实现 privateAnnotationsGetter，是供 WithAudit 装饰器使用的私有方法；
// 返回审计级别不高于 maxLevel 的注解副本；返回副本使调用方可在锁外安全读取。
func (record *attributesRecord) getAnnotations(maxLevel auditinternal.Level) map[string]string {
	// 读锁足以保护 map 的并发读，复制成本换来调用方无需持锁。
	record.annotationsLock.RLock()
	defer record.annotationsLock.RUnlock()

	if record.annotations == nil {
		// 没有注解时快速返回，避免分配空 map。
		return nil
	}
	cp := make(map[string]string, len(record.annotations))
	for key, value := range record.annotations {
		if value.level.Less(maxLevel) || value.level == maxLevel {
			// 级别过滤：只暴露调用方有权审计的注解。
			cp[key] = value.value
		}
	}
	return cp
}

// AddAnnotation 以 Metadata 级别添加注解，是大多数插件的默认入口。
func (record *attributesRecord) AddAnnotation(key, value string) error {
	return record.AddAnnotationWithLevel(key, value, auditinternal.LevelMetadata)
}

// AddAnnotationWithLevel 以指定审计级别添加注解；低于 Metadata 的级别会被拒绝，
// 且同一 key 不允许被不同值覆盖，避免插件之间静默互相覆盖。
func (record *attributesRecord) AddAnnotationWithLevel(key, value string, level auditinternal.Level) error {
	if err := checkKeyFormat(key); err != nil {
		// 先校验 key 格式，避免把非法 key 写进审计输出。
		return err
	}
	if level.Less(auditinternal.LevelMetadata) {
		// 注解本身属于审计数据，低于 Metadata 的级别没有意义。
		return fmt.Errorf("admission annotations are not allowed to be set at audit level lower than Metadata, key: %q, level: %s", key, level)
	}
	record.annotationsLock.Lock()
	defer record.annotationsLock.Unlock()

	if record.annotations == nil {
		// 懒初始化：绝大多数请求不会产生注解。
		record.annotations = make(map[string]annotation)
	}
	annotation := annotation{level: level, value: value}
	if v, ok := record.annotations[key]; ok && v != annotation {
		// 同名不同值意味着插件冲突；显式报错而不是后写覆盖。
		return fmt.Errorf("admission annotations are not allowd to be overwritten, key:%q, old value: %v, new value: %v", key, record.annotations[key], annotation)
	}
	record.annotations[key] = annotation
	return nil
}

// GetReinvocationContext 返回本次请求的重入上下文，供插件协调重入。
func (record *attributesRecord) GetReinvocationContext() ReinvocationContext {
	return record.reinvocationContext
}

// reinvocationContext 记录准入链的重入状态与插件间共享值。
type reinvocationContext struct {
	// isReinvoke 为 true 表示当前正处于重入执行。
	isReinvoke bool
	// reinvokeRequested 为 true 表示某个插件请求再次执行整条链。
	reinvokeRequested bool
	// values 按插件名保存重入时需要传递的值。
	values map[string]interface{}
}

// IsReinvoke 返回当前是否为重入执行。
func (rc *reinvocationContext) IsReinvoke() bool {
	return rc.isReinvoke
}

// SetIsReinvoke 标记当前进入重入执行。
func (rc *reinvocationContext) SetIsReinvoke() {
	rc.isReinvoke = true
}

// ShouldReinvoke 返回是否有插件请求重入整条链。
func (rc *reinvocationContext) ShouldReinvoke() bool {
	return rc.reinvokeRequested
}

// SetShouldReinvoke 由插件调用，以请求重入整条链。
func (rc *reinvocationContext) SetShouldReinvoke() {
	rc.reinvokeRequested = true
}

// SetValue 为指定插件保存重入值；values 按需懒初始化。
func (rc *reinvocationContext) SetValue(plugin string, v interface{}) {
	if rc.values == nil {
		rc.values = map[string]interface{}{}
	}
	rc.values[plugin] = v
}

// Value 返回指定插件之前保存的重入值。
func (rc *reinvocationContext) Value(plugin string) interface{} {
	return rc.values[plugin]
}

// checkKeyFormat 校验注解 key 必须是“DNS 子域/名称”形式的限定名，
// 与 Kubernetes 注解 key 的约定保持一致。
func checkKeyFormat(key string) error {
	parts := strings.Split(key, "/")
	if len(parts) != 2 {
		// 必须且只能有一个 '/'，用于分隔域名前缀与名称。
		return fmt.Errorf("annotation key has invalid format, the right format is a DNS subdomain prefix and '/' and key name. (e.g. 'podsecuritypolicy.admission.k8s.io/admit-policy')")
	}
	if msgs := validation.IsQualifiedName(key); len(msgs) != 0 {
		// 复用通用限定名校验，覆盖字符集、长度等规则。
		return fmt.Errorf("annotation key has invalid format %s. A qualified name like 'podsecuritypolicy.admission.k8s.io/admit-policy' is required.", strings.Join(msgs, ","))
	}
	return nil
}
