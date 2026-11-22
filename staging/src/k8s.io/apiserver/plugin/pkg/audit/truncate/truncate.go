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
	// PluginName is the name reported in error metrics.
	PluginName = "truncate"

	// annotationKey defines the name of the annotation used to indicate truncation.
	annotationKey = "audit.k8s.io/truncated"
	// annotationValue defines the value of the annotation used to indicate truncation.
	annotationValue = "true"
)

// Config represents truncating backend configuration.
type Config struct {
	// MaxEventSize defines max allowed size of the event. If the event is larger,
	// truncating will be performed.
	MaxEventSize int64

	// MaxBatchSize defined max allowed size of the batch of events, passed to the backend.
	// If the total size of the batch is larger than this number, batch will be split. Actual
	// size of the serialized request might be slightly higher, on the order of hundreds of bytes.
	MaxBatchSize int64
}

// backend 是一个装饰器：在把事件交给真正的后端之前，先按大小进行截断和分批。
type backend struct {
	// The delegate backend that actually exports events.
	delegateBackend audit.Backend

	// Configuration used for truncation.
	c Config

	// Encoder used to calculate audit event sizes.
	e runtime.Encoder
}

var _ audit.Backend = &backend{}

// NewBackend returns a new truncating backend, using configuration passed in the parameters.
// Truncate backend automatically runs and shut downs the delegate backend.
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

// ProcessEvents truncates oversized events, splits them into batches, and
// forwards each batch to the delegate backend.
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
		// If event was correctly serialized, but the size is more than allowed
		// and it makes sense to do trimming, i.e. there's a request and/or
		// response present, try to strip away request and response.
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

// truncate removed request and response objects from the audit events,
// to try and keep at least metadata.
//
// 浅拷贝而非深拷贝：被丢弃的正是 RequestObject/ResponseObject 这两个大字段，
// 深拷贝它们纯属浪费。但注意因此不能修改其余共享字段。
func truncate(e *auditinternal.Event) *auditinternal.Event {
	// Make a shallow copy to avoid copying response/request objects.
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

// Run delegates to the wrapped backend; the truncating layer itself has no loop.
func (b *backend) Run(stopCh <-chan struct{}) error {
	return b.delegateBackend.Run(stopCh)
}

// Shutdown stops the wrapped backend.
func (b *backend) Shutdown() {
	b.delegateBackend.Shutdown()
}

// calcSize encodes the event into a counting writer to measure its wire size.
func (b *backend) calcSize(e *auditinternal.Event) (int64, error) {
	s := &sizer{}
	if err := b.e.Encode(e, s); err != nil {
		return 0, err
	}
	return s.Size, nil
}

// String identifies this backend as truncate<delegate> in logs and metrics.
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
