/*
Copyright 2018 The Kubernetes Authors.

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

package truncate

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/apiserver/pkg/audit"
)

const (
	// PluginName 是错误指标中报告的名称。
	PluginName = "truncate"

	// annotationKey 定义表示截断的注解名。
	annotationKey = "audit.k8s.io/truncated"
	// annotationValue 定义表示截断的注解值。
	annotationValue = "true"
)

// Config 表示截断后端的配置。
type Config struct {
	// MaxEventSize 定义事件允许的最大大小，超过则
	// 执行截断。
	MaxEventSize int64

	// MaxBatchSize 定义传给后端的事件批次的最大大小。
	// 批次总大小超过此值时会被拆分。实际序列化请求的大小
	// 可能略大，量级在几百字节。
	MaxBatchSize int64
}

// backend 是一个装饰器：在把事件交给真正的后端之前，先按大小进行截断和分批。
type backend struct {
	// 实际导出事件的委托后端。
	delegateBackend audit.Backend

	// 截断所用配置。
	c Config

	// 用于计算审计事件大小的编码器。
	e runtime.Encoder
}

var _ audit.Backend = &backend{}

// NewBackend 根据传入配置创建截断后端。
// 截断后端会自动运行与关停委托后端。
//
// groupVersion 决定事件大小以哪个 API 版本序列化计算——必须与后端实际
// 使用的编码一致，否则大小估算会失真。
func NewBackend(delegateBackend audit.Backend, config Config, groupVersion schema.GroupVersion) audit.Backend {
	return &backend{
		delegateBackend: delegateBackend,
		c:               config,
		e:               audit.Codecs.LegacyCodec(groupVersion),
	}
}

// ProcessEvents 截断超大事件、拆分成批次，然后
// 把每批转发给委托后端。
//
// 返回值采用“与”语义：只要任一批次失败，整体就报告失败，让上层重试/告警。
func (b *backend) ProcessEvents(events ...*auditinternal.Event) bool {
	var errors []error
	var impacted []*auditinternal.Event
	var batch []*auditinternal.Event
	var batchSize int64
	success := true
	for _, event := range events {
		size, err := b.calcSize(event)
		// 若事件正常序列化但超过限制，且裁剪有意义（存在请求和/或响应），
		// 则尝试去掉请求与响应。
		//
		// 低于 Request 级别的事件本就不含请求/响应体，截断无意义，直接报错。
		if err == nil && size > b.c.MaxEventSize && event.Level.GreaterOrEqual(auditinternal.LevelRequest) {
			event = truncate(event)
			size, err = b.calcSize(event)
		}
		if err != nil {
			errors = append(errors, err)
			impacted = append(impacted, event)
			continue
		}
		if size > b.c.MaxEventSize {
			errors = append(errors, fmt.Errorf("event is too large even after truncating"))
			impacted = append(impacted, event)
			continue
		}

		// 达到批次上限就先冲刷当前批次，再开始新批次。
		if len(batch) > 0 && batchSize+size > b.c.MaxBatchSize {
			success = b.delegateBackend.ProcessEvents(batch...) && success
			batch = []*auditinternal.Event{}
			batchSize = 0
		}

		batchSize += size
		batch = append(batch, event)
	}

	if len(batch) > 0 {
		success = b.delegateBackend.ProcessEvents(batch...) && success
	}

	if len(impacted) > 0 {
		audit.HandlePluginError(PluginName, utilerrors.NewAggregate(errors), impacted...)
	}
	return success
}

// truncate 从审计事件中移除请求与响应对象，
// 尽量至少保留元数据。
//
// 浅拷贝而非深拷贝：被丢弃的正是 RequestObject/ResponseObject 这两个大字段，
// 深拷贝它们纯属浪费。但注意因此不能修改其余共享字段。
func truncate(e *auditinternal.Event) *auditinternal.Event {
	// 浅拷贝以避免复制请求/响应对象。
	newEvent := &auditinternal.Event{}
	*newEvent = *e

	newEvent.RequestObject = nil
	newEvent.ResponseObject = nil

	// 打上截断标记，让审计消费者知道这份事件不完整。
	if newEvent.Annotations == nil {
		newEvent.Annotations = make(map[string]string)
	}
	newEvent.Annotations[annotationKey] = annotationValue

	return newEvent
}

// Run 委托给被包装的后端；截断层自身没有循环。
func (b *backend) Run(stopCh <-chan struct{}) error {
	return b.delegateBackend.Run(stopCh)
}

// Shutdown 停止被包装的后端。
func (b *backend) Shutdown() {
	b.delegateBackend.Shutdown()
}

// calcSize 把事件编码进计数 writer 以测量序列化大小。
func (b *backend) calcSize(e *auditinternal.Event) (int64, error) {
	s := &sizer{}
	if err := b.e.Encode(e, s); err != nil {
		return 0, err
	}
	return s.Size, nil
}

// String 在日志与指标中把本后端标识为 truncate<delegate>。
func (b *backend) String() string {
	return fmt.Sprintf("%s<%s>", PluginName, b.delegateBackend)
}

// sizer 是只统计字节数的 io.Writer，避免为测量大小而分配完整缓冲区。
type sizer struct {
	Size int64
}

func (s *sizer) Write(p []byte) (n int, err error) {
	s.Size += int64(len(p))
	return len(p), nil
}
