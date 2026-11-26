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

package resource

import (
	"fmt"
	"reflect"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
)

// ErrMatchFunc 用于过滤那些未必是真正失败的错误。
type ErrMatchFunc func(error) bool

// Result 提供处理 Builder 结果的辅助方法。
//
// Result 是 Builder.Do() 的产物，持有可执行的 Visitor 与原始来源；
// 同一个 Result 通常只能遍历一次（stream 类输入不可重放），需要多次使用时
// 应通过 Infos() 缓存结果。
type Result struct {
	err     error
	visitor Visitor

	// sources 保留原始 Visitor，供 ResourceMapping/Watch 等能力探测使用。
	sources            []Visitor
	singleItemImplied  bool
	targetsSingleItems bool

	mapper       *mapper
	ignoreErrors []utilerrors.Matcher

	// 由 Infos() 调用填充。
	info []*Info
}

// withError 便于内部以链式写法设置错误。
func (r *Result) withError(err error) *Result {
	r.err = err
	return r
}

// 任一 builder 参数指向非列表调用（用户按名称指定对象）时返回 true；
// 目录、流、URL 和资源名元组都算。
func (r *Result) TargetsSingleItems() bool {
	return r.targetsSingleItems
}

// 过滤遍历 Result 时产生的错误（不含创建 Result 时的错误），
// 丢弃匹配 fns 的项。最好与 Builder.ContinueOnError() 配合使用：
// Visitor 会累积错误并在遍历结束后以切片返回。过滤后无错误时，
// Result 的各 Visitor 方法返回 nil。
func (r *Result) IgnoreErrors(fns ...ErrMatchFunc) *Result {
	for _, fn := range fns {
		r.ignoreErrors = append(r.ignoreErrors, utilerrors.Matcher(fn))
	}
	return r
}

// Mapper 返回 builder 的 mapper 副本。
func (r *Result) Mapper() *mapper {
	return r.mapper
}

// Err 返回遍历前发生的一个或多个错误（util.ErrorList）。
// 如需包含遍历期间的错误，请调用 Infos()。
//
// 仅返回构造阶段（Builder 参数校验、映射解析）的错误，不包含遍历期间的错误。
func (r *Result) Err() error {
	return r.err
}

// Visit 对 Builder 描述的对象实现 Visitor。
// 某些来源只能遍历一次或每次结果不同；需要多次操作同一批资源时请使用 Infos()。
func (r *Result) Visit(fn VisitorFunc) error {
	if r.err != nil {
		return r.err
	}
	err := r.visitor.Visit(fn)
	return utilerrors.FilterOut(err, r.ignoreErrors...)
}

// 若 Builder 输入隐含单个对象，IntoSingleItemImplied 把给定布尔指针设为 true。
func (r *Result) IntoSingleItemImplied(b *bool) *Result {
	*b = r.singleItemImplied
	return r
}

// Infos 返回遍历得到的所有资源 Info；
// 只遍历一次，后续调用返回缓存结果。
//
// 缓存 info 和 err：避免重复遍历不可重放的 stream 输入，也保证多次调用结果一致。
func (r *Result) Infos() ([]*Info, error) {
	if r.err != nil {
		return nil, r.err
	}
	if r.info != nil {
		return r.info, nil
	}

	infos := []*Info{}
	err := r.visitor.Visit(func(info *Info, err error) error {
		if err != nil {
			return err
		}
		infos = append(infos, info)
		return nil
	})
	err = utilerrors.FilterOut(err, r.ignoreErrors...)

	r.info, r.err = infos, err
	return infos, err
}

// Object 把一次遍历的结果汇总为单个对象。
// 若 Builder 按用户输入只期望一个对象且确实只找到一个，则原样返回；
// 否则包装为 v1.List。仅当所有 Info 的 ResourceVersion 一致时才设置 List 的版本。
func (r *Result) Object() (runtime.Object, error) {
	infos, err := r.Infos()
	if err != nil {
		return nil, err
	}

	// 只有所有 info 的 resourceVersion 一致时才把它提升到 List 上，否则留空。
	versions := sets.New[string]()
	objects := []runtime.Object{}
	for _, info := range infos {
		if info.Object != nil {
			objects = append(objects, info.Object)
			versions.Insert(info.ResourceVersion)
		}
	}

	if len(objects) == 1 {
		if r.singleItemImplied {
			return objects[0], nil
		}
		// 对象本身已是 List，不要再套一层。
		if meta.IsListType(objects[0]) {
			return objects[0], nil
		}
	}

	version := ""
	if len(versions) == 1 {
		version = versions.UnsortedList()[0]
	}

	return toV1List(objects, version), err
}

// 编译期检查：确保 list 实现了所需接口。
var _ metav1.ListInterface = &v1.List{}
var _ metav1.ListMetaAccessor = &v1.List{}

// 把对象切片及其版本打包成 Items 字段中的 v1.List。
func toV1List(objects []runtime.Object, version string) runtime.Object {
	// 用 RawExtension 包装保留原始对象，避免额外的序列化往返。
	raw := []runtime.RawExtension{}
	for _, o := range objects {
		raw = append(raw, runtime.RawExtension{Object: o})
	}
	return &v1.List{
		ListMeta: metav1.ListMeta{
			ResourceVersion: version,
		},
		Items: raw,
	}
}

// 返回 builder 定位资源对应的唯一 RESTMapping；多个映射时返回错误。
//
// 用 GVR 去重：同一资源通过 plural/shortname 重复指定时仍视为单一映射。
func (r *Result) ResourceMapping() (*meta.RESTMapping, error) {
	if r.err != nil {
		return nil, r.err
	}
	mappings := map[schema.GroupVersionResource]*meta.RESTMapping{}
	for i := range r.sources {
		m, ok := r.sources[i].(ResourceMapping)
		if !ok {
			return nil, fmt.Errorf("a resource mapping could not be loaded from %v", reflect.TypeOf(r.sources[i]))
		}
		mapping := m.ResourceMapping()
		mappings[mapping.Resource] = mapping
	}
	if len(mappings) != 1 {
		return nil, fmt.Errorf("expected only a single resource type")
	}
	for _, mapping := range mappings {
		return mapping, nil
	}
	return nil, nil
}

// Watch 获取指定资源在服务端的变化。
// 只支持单一来源：来源（选择器或纯类型）可直接 watch 时直接 watch；
// 否则先遍历列表（等同 Infos()），只有一个资源时才 watch，多个则报错。
func (r *Result) Watch(resourceVersion string) (watch.Interface, error) {
	if r.err != nil {
		return nil, r.err
	}
	// watch 只对单一来源有意义；多来源无法合并成一个 watch 流。
	if len(r.sources) != 1 {
		return nil, fmt.Errorf("you may only watch a single resource or type of resource at a time")
	}
	// 来源本身支持 watch（selector/类型）时直接使用，否则退回 Info 级别 watch。
	w, ok := r.sources[0].(Watchable)
	if !ok {
		info, err := r.Infos()
		if err != nil {
			return nil, err
		}
		if len(info) != 1 {
			return nil, fmt.Errorf("watch is only supported on individual resources and resource collections - %d resources were found", len(info))
		}
		return info[0].Watch(resourceVersion)
	}
	return w.Watch(resourceVersion)
}
