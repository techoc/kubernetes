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

package v1

import (
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

// Event 表示针对被 watch 资源的单个事件。
// Type 取 Added/Modified/Deleted/Error 等值，Object 携带对应的对象快照。
//
// +protobuf=true
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type WatchEvent struct {
	Type string `json:"type" protobuf:"bytes,1,opt,name=type"`

	// Object is:
	//  * If Type is Added or Modified: the new state of the object.
	//  * If Type is Deleted: the state of the object immediately before deletion.
	//  * If Type is Error: *Status is recommended; other types may make sense
	//    depending on context.
	Object runtime.RawExtension `json:"object" protobuf:"bytes,2,opt,name=object"`
}

// Convert_watch_Event_To_v1_WatchEvent 把内部 watch.Event 转换为外部 WatchEvent。
// 对 *runtime.Unknown 只保留原始字节，避免在此处做额外的解码；
// 对具体对象则直接挂到 RawExtension.Object 上，由后续编码器处理。
func Convert_watch_Event_To_v1_WatchEvent(in *watch.Event, out *WatchEvent, s conversion.Scope) error {
	out.Type = string(in.Type)
	switch t := in.Object.(type) {
	case *runtime.Unknown:
		// TODO: handle other fields on Unknown and detect type
		out.Object.Raw = t.Raw
	case nil:
	default:
		out.Object.Object = in.Object
	}
	return nil
}

// Convert_v1_InternalEvent_To_v1_WatchEvent 将内部事件类型转换为外部 WatchEvent。
// InternalEvent 是 watch.Event 的具名别名，因此可直接做指针转换后复用同一逻辑。
func Convert_v1_InternalEvent_To_v1_WatchEvent(in *InternalEvent, out *WatchEvent, s conversion.Scope) error {
	return Convert_watch_Event_To_v1_WatchEvent((*watch.Event)(in), out, s)
}

// Convert_v1_WatchEvent_To_watch_Event 把外部 WatchEvent 转换为内部 watch.Event。
// 优先使用已解码的对象；只有原始字节时，包成 runtime.Unknown 并假定为 JSON
// （这是 watch 流中最常见的编码形式）。
func Convert_v1_WatchEvent_To_watch_Event(in *WatchEvent, out *watch.Event, s conversion.Scope) error {
	out.Type = watch.EventType(in.Type)
	if in.Object.Object != nil {
		out.Object = in.Object.Object
	} else if in.Object.Raw != nil {
		// TODO: handle other fields on Unknown and detect type
		out.Object = &runtime.Unknown{
			Raw:         in.Object.Raw,
			ContentType: runtime.ContentTypeJSON,
		}
	}
	return nil
}

// Convert_v1_WatchEvent_To_v1_InternalEvent 将外部 WatchEvent 转换为内部事件类型。
// 同样复用 watch.Event 的转换逻辑，避免两套实现产生分歧。
func Convert_v1_WatchEvent_To_v1_InternalEvent(in *WatchEvent, out *InternalEvent, s conversion.Scope) error {
	return Convert_v1_WatchEvent_To_watch_Event(in, (*watch.Event)(out), s)
}

// InternalEvent 使 watch.Event 具备版本化类型的形式，便于参与 scheme 转换。
// +protobuf=false
type InternalEvent watch.Event

// GetObjectKind 返回空 ObjectKind：事件本身没有独立的 kind，其类型由 scheme 注册决定。
func (e *InternalEvent) GetObjectKind() schema.ObjectKind { return schema.EmptyObjectKind }

// GetObjectKind 返回空 ObjectKind，原因同上。
func (e *WatchEvent) GetObjectKind() schema.ObjectKind { return schema.EmptyObjectKind }

func (e *InternalEvent) DeepCopyObject() runtime.Object {
	if c := e.DeepCopy(); c != nil {
		return c
	} else {
		return nil
	}
}
