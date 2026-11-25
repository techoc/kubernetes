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

package componentstatus

import (
	"context"
	"sync"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/storage"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/rest"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/printers"
	printersinternal "k8s.io/kubernetes/pkg/printers/internalversion"
	printerstorage "k8s.io/kubernetes/pkg/printers/storage"
	"k8s.io/kubernetes/pkg/probe"
)

// REST 不使用 etcd Store；每次 Get/List 都从服务器回调实时探测组件，并将探测结果组装成 ComponentStatus。
type REST struct {
	GetServersToValidate func() map[string]Server
	rest.TableConvertor
}

// NewStorage 接收组件探测器提供函数，延迟获取实际需要检查的服务列表。
func NewStorage(serverRetriever func() map[string]Server) *REST {
	return &REST{
		GetServersToValidate: serverRetriever,
		TableConvertor:       printerstorage.TableConvertor{TableGenerator: printers.NewTableGenerator().With(printersinternal.AddHandlers)},
	}
}

func (*REST) NamespaceScoped() bool {
	return false
}

func (rs *REST) New() runtime.Object {
	return &api.ComponentStatus{}
}

var _ rest.SingularNameProvider = &REST{}

func (rs *REST) GetSingularName() string {
	return "componentstatus"
}

// Destroy 无需清理资源，因为 REST 没有底层存储。
func (r *REST) Destroy() {
}

func (rs *REST) NewList() runtime.Object {
	return &api.ComponentStatusList{}
}

// List 并发探测所有组件，等待全部结果后再根据请求中的 Label/Field Selector 过滤并返回列表。
func (rs *REST) List(ctx context.Context, options *metainternalversion.ListOptions) (runtime.Object, error) {
	servers := rs.GetServersToValidate()

	wait := sync.WaitGroup{}
	wait.Add(len(servers))
	statuses := make(chan api.ComponentStatus, len(servers))
	// 每个组件独立 goroutine 执行检查，避免一个慢探测串行阻塞其他组件的结果返回。
	for k, v := range servers {
		go func(name string, server Server) {
			defer wait.Done()
			status := rs.getComponentStatus(name, server)
			statuses <- *status
		}(k, v)
	}
	wait.Wait()
	close(statuses)

	pred := componentStatusPredicate(options)

	reply := []api.ComponentStatus{}
	for status := range statuses {
		// ComponentStatus 自 v1.14 起不支持标签，但仍执行过滤；携带 Label/Field Selector 的请求会过滤掉全部结果。
		if matched := matchesPredicate(status, &pred); matched {
			reply = append(reply, status)
		}
	}
	return &api.ComponentStatusList{Items: reply}, nil
}

// componentStatusPredicate 读取请求选择器；ComponentStatus 不支持标签/字段写入，但仍按通用过滤规则处理请求。
func componentStatusPredicate(options *metainternalversion.ListOptions) storage.SelectionPredicate {
	pred := storage.SelectionPredicate{
		Label:    labels.Everything(),
		Field:    fields.Everything(),
		GetAttrs: nil,
	}
	if options != nil {
		if options.LabelSelector != nil {
			pred.Label = options.LabelSelector
		}
		if options.FieldSelector != nil {
			pred.Field = options.FieldSelector
		}
	}
	return pred
}

// matchesPredicate 目前只提供通用 ObjectMeta 字段，组件自身的 Conditions 尚不能作为选择器字段。
func matchesPredicate(status api.ComponentStatus, pred *storage.SelectionPredicate) bool {
	fieldsSet := generic.AddObjectMetaFieldsSet(make(fields.Set, 2), &status.ObjectMeta, true)
	return pred.MatchesObjectAttributes(
		status.ObjectMeta.Labels,
		fieldsSet,
	)
}

func (rs *REST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	servers := rs.GetServersToValidate()

	if server, ok := servers[name]; !ok {
		return nil, apierrors.NewNotFound(api.Resource("componentstatus"), name)
	} else {
		return rs.getComponentStatus(name, server), nil
	}
}

// ToConditionStatus 将探针 Success/Failure/Unknown 映射为 Kubernetes Condition 的 True/False/Unknown。
func ToConditionStatus(s probe.Result) api.ConditionStatus {
	switch s {
	case probe.Success:
		return api.ConditionTrue
	case probe.Failure:
		return api.ConditionFalse
	default:
		return api.ConditionUnknown
	}
}

// getComponentStatus 执行单个组件探测，并把探针结果、消息和错误转换成 Healthy Condition。
func (rs *REST) getComponentStatus(name string, server Server) *api.ComponentStatus {
	status, msg, err := server.DoServerCheck()
	errorMsg := ""
	if err != nil {
		errorMsg = err.Error()
	}

	c := &api.ComponentCondition{
		Type:    api.ComponentHealthy,
		Status:  ToConditionStatus(status),
		Message: msg,
		Error:   errorMsg,
	}

	retVal := &api.ComponentStatus{
		Conditions: []api.ComponentCondition{*c},
	}
	retVal.Name = name

	return retVal
}

// 实现 ShortNamesProvider。
var _ rest.ShortNamesProvider = &REST{}

// ShortNames 实现 ShortNamesProvider 接口，返回资源的短名称列表。
func (r *REST) ShortNames() []string {
	return []string{"cs"}
}
