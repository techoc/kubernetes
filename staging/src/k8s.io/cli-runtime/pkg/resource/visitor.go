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
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/apimachinery/pkg/watch"
)

const (
	constSTDINstr       = "STDIN"
	stopValidateMessage = "if you choose to ignore these errors, turn validation off with --validate=false"
)

// Watchable 描述可从服务端监听变化的资源，
// 从给定的 resourceVersion 之后开始。
type Watchable interface {
	Watch(resourceVersion string) (watch.Interface, error)
}

// ResourceMapping 让对象返回其所代表资源对应的
// 资源映射。
type ResourceMapping interface {
	ResourceMapping() *meta.RESTMapping
}

// Info 保存执行 REST 调用所需的临时信息，
// 或已完成的 REST 调用结果。
//
// Info 是一次遍历中“单个资源”的上下文：既可用于延迟发的 Get/List 调用，
// 也可承载已加载的对象。Visitor 链上的装饰器会就地修改它。
type Info struct {
	// 仅非本地 builder 才有 Client。
	Client RESTClient
	// 仅非本地 builder 才有 Mapping。
	Mapping *meta.RESTMapping

	// 对象属于命名空间且有明确取值时设置 Namespace。
	Namespace string
	Name      string

	// Source 可选，指向模板文件（.json 或 .yaml）的文件名或 URL，
	// 或表示使用 stdin 处理该资源。
	Source string
	// Object 可选，为服务端返回的最新值（若有）。
	// 其形式取决于 Builder 的配置，通常是 unstructured 或 internal。
	// 从服务端取回时，Builder 期望映射客户端
	// 决定最终形态；可用 AsVersioned、AsUnstructured 和 AsInternal
	// 辅助方法转换对象版本。
	// 指定 Subresource 时，这里保存的是该子资源对象。
	Object runtime.Object
	// ResourceVersion 可选，为服务端所知的该类型资源最新版本；
	// 它可能与对象自身的版本不同，
	// 但设置后应等于或新于对象的 resourceVersion
	// （以服务端对版本的定义为准）。
	ResourceVersion string
	// Subresource 可选，为服务端返回的子资源最新值
	// （若有）。
	Subresource string
}

// Visit 实现 Visitor 接口。
func (i *Info) Visit(fn VisitorFunc) error {
	return fn(i, nil)
}

// Get 按 Namespace 和 Name 字段取回对象。
//
// 这是延迟加载的核心：只给 Info 配好 Client/Mapping 后，真正取对象发生在遍历时。
func (i *Info) Get() (err error) {
	obj, err := NewHelper(i.Client, i.Mapping).WithSubresource(i.Subresource).Get(i.Namespace, i.Name)
	if err != nil {
		// 对象 not found 时进一步检查 namespace 是否存在；若 namespace 也不存在，
		// 返回更贴近根因的错误（否则用户会误以为对象名写错）。
		if errors.IsNotFound(err) && len(i.Namespace) > 0 && i.Namespace != metav1.NamespaceDefault && i.Namespace != metav1.NamespaceAll {
			err2 := i.Client.Get().AbsPath("api", "v1", "namespaces", i.Namespace).Do(context.TODO()).Error()
			if err2 != nil && errors.IsNotFound(err2) {
				return err2
			}
		}
		return err
	}
	i.Object = obj
	i.ResourceVersion, _ = metadataAccessor.ResourceVersion(obj)
	return nil
}

// Refresh 用另一个对象更新当前对象；ignoreError 为真时，
// 即使无法从对象读取 name、namespace 或 resourceVersion
// 也会执行更新。
func (i *Info) Refresh(obj runtime.Object, ignoreError bool) error {
	// 三个字段独立读取：默认严格，ignoreError 时尽量保留能取到的部分。
	name, err := metadataAccessor.Name(obj)
	if err != nil {
		if !ignoreError {
			return err
		}
	} else {
		i.Name = name
	}
	namespace, err := metadataAccessor.Namespace(obj)
	if err != nil {
		if !ignoreError {
			return err
		}
	} else {
		i.Namespace = namespace
	}
	version, err := metadataAccessor.ResourceVersion(obj)
	if err != nil {
		if !ignoreError {
			return err
		}
	} else {
		i.ResourceVersion = version
	}
	i.Object = obj
	return nil
}

// ObjectName 返回资源 kind/name 的近似形式。
func (i *Info) ObjectName() string {
	// 有 mapping 时用规范资源名；否则退回从对象 GVK 推断。
	if i.Mapping != nil {
		return fmt.Sprintf("%s/%s", i.Mapping.Resource.Resource, i.Name)
	}
	gvk := i.Object.GetObjectKind().GroupVersionKind()
	if len(gvk.Group) == 0 {
		return fmt.Sprintf("%s/%s", strings.ToLower(gvk.Kind), i.Name)
	}
	return fmt.Sprintf("%s.%s/%s\n", strings.ToLower(gvk.Kind), gvk.Group, i.Name)
}

// String 返回通用字符串表示。
func (i *Info) String() string {
	basicInfo := fmt.Sprintf("Name: %q, Namespace: %q", i.Name, i.Namespace)
	if i.Mapping != nil {
		mappingInfo := fmt.Sprintf("Resource: %q, GroupVersionKind: %q", i.Mapping.Resource.String(),
			i.Mapping.GroupVersionKind.String())
		return fmt.Sprint(mappingInfo, "\n", basicInfo)
	}
	return basicInfo
}

// 对象属于命名空间时 Namespaced 返回 true。
func (i *Info) Namespaced() bool {
	if i.Mapping != nil {
		// 有 RESTMapper 信息时优先使用它判断。
		return i.Mapping.Scope.Name() == meta.RESTScopeNameNamespace
	}
	// 否则以 Info 中是否存在 namespace 作为依据。
	return len(i.Namespace) > 0
}

// Watch 返回取回该对象后服务端发生的变化。
//
// 单对象 watch：以 Info 的 namespace/name 作为目标。
func (i *Info) Watch(resourceVersion string) (watch.Interface, error) {
	return NewHelper(i.Client, i.Mapping).WatchSingle(i.Namespace, i.Name, resourceVersion)
}

// 返回该资源的映射，实现 ResourceMapping 接口。
func (i *Info) ResourceMapping() *meta.RESTMapping {
	return i.Mapping
}

// VisitorList 对其中的子 Visitor 实现 Visit；
// 任一子 Visitor 返回错误即终止遍历。
type VisitorList []Visitor

// Visit 实现 Visitor 接口。
//
// 顺序遍历；遇到第一个错误立即返回，适合要求严格顺序或快速失败的场景。
func (l VisitorList) Visit(fn VisitorFunc) error {
	for i := range l {
		if err := l[i].Visit(fn); err != nil {
			return err
		}
	}
	return nil
}

// ConcurrentVisitorList 并发遍历多个 visitor；concurrency 为 0/1 时退化为串行。
type ConcurrentVisitorList struct {
	visitors    []Visitor
	concurrency int
}

// Visit 用 errgroup 控制并发上限，并等待全部 visitor 结束（出错也会等）。
func (l ConcurrentVisitorList) Visit(fn VisitorFunc) error {
	g := errgroup.Group{}

	// 并发度为 1 时顺序执行各 Visitor。这是默认值，
	// 既保持旧行为，也允许各组件按需启用并发。

	concurrency := 1
	if l.concurrency > concurrency {
		concurrency = l.concurrency
	}
	g.SetLimit(concurrency)

	for i := range l.visitors {
		g.Go(func() error {
			return l.visitors[i].Visit(fn)
		})
	}

	return g.Wait()
}

// EagerVisitorList 遍历所有子 Visitor，
// 最后统一返回全部错误。
//
// “急切”指不因单个错误提前退出，而是收集全部错误后一次性返回。
type EagerVisitorList []Visitor

// Visit 实现 Visitor，收集处理过程中的错误，
// 直到所有子 Visitor 遍历完毕。
func (l EagerVisitorList) Visit(fn VisitorFunc) error {
	var errs []error
	for i := range l {
		err := l[i].Visit(func(info *Info, err error) error {
			if err != nil {
				errs = append(errs, err)
				return nil
			}
			if err := fn(info, nil); err != nil {
				errs = append(errs, err)
			}
			return nil
		})
		if err != nil {
			errs = append(errs, err)
		}
	}
	return utilerrors.NewAggregate(errs)
}

// ValidateSchema 在解析对象前做 schema 校验；校验失败时附上如何关闭校验的提示，
// 因为 CLI 用户常被这个开关卡住。
func ValidateSchema(data []byte, schema ContentValidator) error {
	if schema == nil {
		return nil
	}
	if err := schema.ValidateBytes(data); err != nil {
		return fmt.Errorf("error validating data: %v; %s", err, stopValidateMessage)
	}
	return nil
}

// URLVisitor 下载 URL 内容，成功时返回
// 代表该对象的 Info。
//
// URLVisitor 把远程内容交给 StreamVisitor 解析，因此同样支持多文档 YAML。
type URLVisitor struct {
	URL *url.URL
	*StreamVisitor
	// HttpAttemptCount 控制重试次数，0 或负数直接报错。
	HttpAttemptCount int
}

// Visit 下载并解析 URL；响应体由本方法负责关闭。
func (v *URLVisitor) Visit(fn VisitorFunc) error {
	body, err := readHttpWithRetries(httpgetImpl, time.Second, v.URL.String(), v.HttpAttemptCount)
	if err != nil {
		return err
	}
	defer body.Close()
	v.StreamVisitor.Reader = body
	return v.StreamVisitor.Visit(fn)
}

// readHttpWithRetries 对 v.URL 最多发起 retries 次 http.Get。
//
// 重试策略：网络错误和 5xx 重试，4xx/3xx 立即失败；每次重试前 sleep duration。
func readHttpWithRetries(get httpget, duration time.Duration, u string, attempts int) (io.ReadCloser, error) {
	var err error
	if attempts <= 0 {
		return nil, fmt.Errorf("http attempts must be greater than 0, was %d", attempts)
	}
	for i := 0; i < attempts; i++ {
		var (
			statusCode int
			status     string
			body       io.ReadCloser
		)
		if i > 0 {
			time.Sleep(duration)
		}

		// 尝试获取 URL。
		statusCode, status, body, err = get(u)

		// 出错时重试。
		if err != nil {
			continue
		}

		if statusCode == http.StatusOK {
			return body, nil
		}
		body.Close()
		// 根据 StatusCode 设置错误。
		err = fmt.Errorf("unable to read URL %q, server reported %s, status code=%d", u, status, statusCode)

		if statusCode >= 500 && statusCode < 600 {
			// 500 错误需要重试。
			continue
		} else {
			// 其他状态码不重试。
			break
		}
	}
	return nil, err
}

// httpget 定义取回 URL 内容的函数，便于单元测试打桩。
type httpget func(url string) (int, string, io.ReadCloser, error)

// httpgetImpl 实现实际的 URL 内容获取。
// 把失败折叠成 error，调用方只需关心状态码与响应体。
func httpgetImpl(url string) (int, string, io.ReadCloser, error) {
	resp, err := http.Get(url)
	if err != nil {
		return 0, "", nil, err
	}
	return resp.StatusCode, resp.Status, resp.Body, nil
}

// DecoratedVisitor 依次执行装饰器，再调用 Visit 传入的
// Visitor 函数；任一装饰器报错即终止遍历。
//
// 装饰器模式让 namespace 填充、校验、延迟加载等横切逻辑可独立组合。
type DecoratedVisitor struct {
	visitor    Visitor
	decorators []VisitorFunc
}

// 创建 Visitor，在用户提供的 Visitor 函数执行前
// 先执行这些函数，使其有机会修改 Info
// 或提前报错终止。
func NewDecoratedVisitor(v Visitor, fn ...VisitorFunc) Visitor {
	// 没有装饰器就不要再包一层，减少无谓的间接调用。
	if len(fn) == 0 {
		return v
	}
	return DecoratedVisitor{v, fn}
}

// Visit 实现 Visitor 接口。
//
// 先跑完所有装饰器（可能修改 Info 或短路），再把 Info 交给最终回调。
func (v DecoratedVisitor) Visit(fn VisitorFunc) error {
	return v.visitor.Visit(func(info *Info, err error) error {
		if err != nil {
			return err
		}
		for i := range v.decorators {
			if err := v.decorators[i](info, nil); err != nil {
				return err
			}
		}
		return fn(info, nil)
	})
}

// ContinueOnErrorVisitor 遍历每一项，
// 即使个别项出错也继续，最后返回汇总错误。

// 与 EagerVisitorList 的区别：它在单项遍历层面容错，而后者在子 visitor 层面。
type ContinueOnErrorVisitor struct {
	Visitor
}

// 遍历无错返回 nil，单个错误返回普通错误，
// 多个错误返回汇总错误。
// 某一项失败不会阻止后续项被遍历；
// 但 Visitor 直接返回的错误
// 仍可能导致部分项
// 未被访问。
func (v ContinueOnErrorVisitor) Visit(fn VisitorFunc) error {
	var errs []error
	// 回调内部吞掉错误并收集，使底层 visitor 能继续往下走。
	err := v.Visitor.Visit(func(info *Info, err error) error {
		if err != nil {
			errs = append(errs, err)
			return nil
		}
		if err := fn(info, nil); err != nil {
			errs = append(errs, err)
		}
		return nil
	})
	if err != nil {
		errs = append(errs, err)
	}
	if len(errs) == 1 {
		return errs[0]
	}
	return utilerrors.NewAggregate(errs)
}

// FlattenListVisitor 把 runtime.ExtractList 识别为 List 的对象
// （含 "Items" 公开字段，元素为 runtime.Object 或实现该接口的对象）
// 展开为多个 Info；无错时返回 nil。
// 子项出错时（例如 List 中有对象没有注册的客户端或资源），
// 返回汇总错误。
//
// FlattenListVisitor 把 List 拆成单项 Info，让下游只面对同一种粒度。
type FlattenListVisitor struct {
	visitor Visitor
	typer   runtime.ObjectTyper
	mapper  *mapper
}

// 创建 Visitor，把 List 形式的 runtime.Object
// 展开为独立项后逐一遍历。
func NewFlattenListVisitor(v Visitor, typer runtime.ObjectTyper, mapper *mapper) Visitor {
	return FlattenListVisitor{v, typer, mapper}
}

// FlattenListVisitor.Visit 用显式队列展开嵌套列表：List 里可以再套 List，
// 直到所有元素都是非列表对象再逐个生成 Info。
func (v FlattenListVisitor) Visit(fn VisitorFunc) error {
	return v.visitor.Visit(func(info *Info, err error) error {
		if err != nil {
			return err
		}
		if info.Object == nil {
			return fn(info, nil)
		}
		if !meta.IsListType(info.Object) {
			return fn(info, nil)
		}

		items := []runtime.Object{}
		itemsToProcess := []runtime.Object{info.Object}

		for i := 0; i < len(itemsToProcess); i++ {
			currObj := itemsToProcess[i]
			if !meta.IsListType(currObj) {
				items = append(items, currObj)
				continue
			}

			currItems, err := meta.ExtractList(currObj)
			if err != nil {
				return err
			}
			if errs := runtime.DecodeList(currItems, v.mapper.decoder); len(errs) > 0 {
				return utilerrors.NewAggregate(errs)
			}
			itemsToProcess = append(itemsToProcess, currItems...)
		}

		// 查询 List 中对象的 Info 时，优先使用 List 自身的 GVK。
		var preferredGVKs []schema.GroupVersionKind
		if info.Mapping != nil && !info.Mapping.GroupVersionKind.Empty() {
			preferredGVKs = append(preferredGVKs, info.Mapping.GroupVersionKind)
		}
		var errs []error
		for i := range items {
			item, err := v.mapper.infoForObject(items[i], v.typer, preferredGVKs)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if len(info.ResourceVersion) != 0 {
				item.ResourceVersion = info.ResourceVersion
			}
			// 把 List 的来源传递给各子项。
			if len(info.Source) != 0 {
				item.Source = info.Source
			}
			if err := fn(item, nil); err != nil {
				errs = append(errs, err)
			}
		}
		return utilerrors.NewAggregate(errs)
	})
}

// ignoreFile 在递归遍历时按扩展名过滤；extensions 为空则不过滤。
func ignoreFile(path string, extensions []string) bool {
	if len(extensions) == 0 {
		return false
	}
	ext := filepath.Ext(path)
	for _, s := range extensions {
		if s == ext {
			return false
		}
	}
	return true
}

// 返回专门处理 STDIN 的 FileVisitor。
//
// 用常量路径 "STDIN" 区分标准输入，避免与真实文件名冲突。
func FileVisitorForSTDIN(mapper *mapper, schema ContentValidator) Visitor {
	return &FileVisitor{
		Path:          constSTDINstr,
		StreamVisitor: NewStreamVisitor(nil, mapper, constSTDINstr, schema),
	}
}

// 为给定路径返回一组处理文件的 FileVisitor。
// FileVisitor 打开文件后把 io.Reader 交给 StreamVisitor 读取（stdin
// 同样处理）；Paths 也接受单个文件，此时只返回一个 Visitor。
//
// 递归时按 extensions 过滤；显式给出的单个文件即使扩展名不匹配也会被处理。
func ExpandPathsToFileVisitors(mapper *mapper, paths string, recursive bool, extensions []string, schema ContentValidator) ([]Visitor, error) {
	var visitors []Visitor
	err := filepath.Walk(paths, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// 非递归模式：跳过根目录以下的子目录。
		if fi.IsDir() {
			if path != paths && !recursive {
				return filepath.SkipDir
			}
			return nil
		}
		// 路径被显式指定时不检查扩展名。
		if path != paths && ignoreFile(path, extensions) {
			return nil
		}

		visitor := &FileVisitor{
			Path:          path,
			StreamVisitor: NewStreamVisitor(nil, mapper, path, schema),
		}

		visitors = append(visitors, visitor)
		return nil
	})

	if err != nil {
		return nil, err
	}
	return visitors, nil
}

// FileVisitor 包装 StreamVisitor，负责文件的打开和关闭。
type FileVisitor struct {
	Path string
	*StreamVisitor
}

// 其 Visit 只负责打开和关闭文件。
//
// 标准输入不关闭（由进程生命周期管理）；普通文件在本次 Visit 结束后关闭。
func (v *FileVisitor) Visit(fn VisitorFunc) error {
	var f *os.File
	if v.Path == constSTDINstr {
		f = os.Stdin
	} else {
		var err error
		f, err = os.Open(v.Path)
		if err != nil {
			return err
		}
		defer f.Close()
	}

	// TODO: 考虑增加强制 UTF16 的标志，因为某些
	// Windows 工具不写 BOM。
	// 自动识别并剥离 UTF-8/UTF-16 BOM，兼容 Windows 生成的清单文件。
	utf16bom := unicode.BOMOverride(unicode.UTF8.NewDecoder())
	v.StreamVisitor.Reader = transform.NewReader(f, utf16bom)

	return v.StreamVisitor.Visit(fn)
}

// StreamVisitor 从 io.Reader 读取并遍历对象，且只能遍历
// 一次。
// TODO: 目前要求对象在解码前为 JSON 格式，应在 runtime.Codec 上
// 实现流式解码方法来正确处理。
//
// StreamVisitor 是不可重放的：Reader 被消费后无法再次遍历，需要多次使用时先缓存 Infos。
type StreamVisitor struct {
	io.Reader
	*mapper

	Source string
	Schema ContentValidator
}

// 需要修改结构体字段但保持调用方式不变时，可用这个辅助函数。
//
// 替代直接构造 StreamVisitor，避免调用方依赖字段布局。
func NewStreamVisitor(r io.Reader, mapper *mapper, source string, schema ContentValidator) *StreamVisitor {
	return &StreamVisitor{
		Reader: r,
		mapper: mapper,
		Source: source,
		Schema: schema,
	}
}

// Visit 在流上实现 Visitor；StreamVisitor 能区分一个流中的多个资源。
//
// 用 YAMLOrJSONDecoder 逐个文档解码，因此一个文件里用 "---" 分隔的多个对象会
// 分别生成 Info；空文档和 null 文档跳过。
func (v *StreamVisitor) Visit(fn VisitorFunc) error {
	d := yaml.NewYAMLOrJSONDecoder(v.Reader, 4096)
	for {
		ext := runtime.RawExtension{}
		if err := d.Decode(&ext); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("error parsing %s: %v", v.Source, err)
		}
		// TODO: 需要支持其他编码和 schema 的对象。
		ext.Raw = bytes.TrimSpace(ext.Raw)
		// 只有分隔符没有内容时 Raw 可能为空或 "null"，跳过而不是报错。
		if len(ext.Raw) == 0 || bytes.Equal(ext.Raw, []byte("null")) {
			continue
		}
		if err := ValidateSchema(ext.Raw, v.Schema); err != nil {
			return fmt.Errorf("error validating %q: %v", v.Source, err)
		}
		info, err := v.infoForData(ext.Raw, v.Source)
		if err != nil {
			// 解析失败也交给回调决定：ContinueOnError 模式下可继续处理后续文档。
			if fnErr := fn(info, err); fnErr != nil {
				return fnErr
			}
			continue
		}
		if err := fn(info, nil); err != nil {
			return err
		}
	}
}

// UpdateObjectNamespace 把 Info.Namespace 同步到对象自身的 metadata 上。
func UpdateObjectNamespace(info *Info, err error) error {
	if err != nil {
		return err
	}
	if info.Object != nil {
		return metadataAccessor.SetNamespace(info.Object, info.Namespace)
	}
	return nil
}

// 对象不属于命名空间作用域时，FilterNamespace 会去掉 namespace。
//
// 集群级资源不能带 namespace，否则服务端会拒绝请求。
func FilterNamespace(info *Info, err error) error {
	if err != nil {
		return err
	}
	if !info.Namespaced() {
		info.Namespace = ""
		UpdateObjectNamespace(info, nil)
	}
	return nil
}

// SetNamespace 确保遍历到的每个 Info 都设置了 namespace；
// 若 Info 带有 Object，也会一并修改。
//
// 只填充空 namespace，不覆盖对象里显式写的值。
func SetNamespace(namespace string) VisitorFunc {
	return func(info *Info, err error) error {
		if err != nil {
			return err
		}
		if !info.Namespaced() {
			return nil
		}
		if len(info.Namespace) == 0 {
			info.Namespace = namespace
			UpdateObjectNamespace(info, nil)
		}
		return nil
	}
}

// Info 未指定 namespace 时补上默认值；
// 已设置但与给定值不符时报错，
// 防止管理员误操作其他命名空间中的资源。

func RequireNamespace(namespace string) VisitorFunc {
	return func(info *Info, err error) error {
		if err != nil {
			return err
		}
		if !info.Namespaced() {
			return nil
		}
		// 对象未指定 namespace 时补默认值；已指定则必须与命令行一致，
		// 防止误操作到其他 namespace 的同名资源。
		if len(info.Namespace) == 0 {
			info.Namespace = namespace
			UpdateObjectNamespace(info, nil)
			return nil
		}
		if info.Namespace != namespace {
			return fmt.Errorf("the namespace from the provided object %q does not match the namespace %q. You must pass '--namespace=%s' to perform this operation.", info.Namespace, namespace, info.Namespace)
		}
		return nil
	}
}

// 通过标准客户端的 Get 更新每个 Info 的 Object，
// 取得最新副本。
//
// 已加载的 List 不能直接“取最新”，因为 Info 的粒度是单对象，这里明确报错。
func RetrieveLatest(info *Info, err error) error {
	if err != nil {
		return err
	}
	if meta.IsListType(info.Object) {
		return fmt.Errorf("watch is only supported on individual resources and resource collections, but a list of resources is found")
	}
	if len(info.Name) == 0 {
		return nil
	}
	if info.Namespaced() && len(info.Namespace) == 0 {
		return fmt.Errorf("no namespace set on resource %s %q", info.Mapping.Resource, info.Name)
	}
	return info.Get()
}

// 仅在对象尚未加载时才更新它。
//
// Builder 默认只构造 Info 而不立即请求，真正取对象推迟到遍历时。
func RetrieveLazy(info *Info, err error) error {
	if err != nil {
		return err
	}
	if info.Object == nil {
		return info.Get()
	}
	return nil
}

// FilterFunc 返回 false 表示丢弃该 Info；返回 error 则终止遍历。
type FilterFunc func(info *Info, err error) (bool, error)

// FilteredVisitor 依次应用过滤器，任一过滤器拒绝则不再继续。
type FilteredVisitor struct {
	visitor Visitor
	filters []FilterFunc
}

// NewFilteredVisitor 无过滤器时直接返回原 visitor，避免多余包装。
func NewFilteredVisitor(v Visitor, fn ...FilterFunc) Visitor {
	if len(fn) == 0 {
		return v
	}
	return FilteredVisitor{v, fn}
}

func (v FilteredVisitor) Visit(fn VisitorFunc) error {
	return v.visitor.Visit(func(info *Info, err error) error {
		if err != nil {
			return err
		}
		for _, filter := range v.filters {
			ok, err := filter(info, nil)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
		}
		return fn(info, nil)
	})
}

// FilterByLabelSelector 仅保留标签匹配 selector 的对象；本地文件也适用。
func FilterByLabelSelector(s labels.Selector) FilterFunc {
	return func(info *Info, err error) (bool, error) {
		if err != nil {
			return false, err
		}
		a, err := meta.Accessor(info.Object)
		if err != nil {
			return false, err
		}
		if !s.Matches(labels.Set(a.GetLabels())) {
			return false, nil
		}
		return true, nil
	}
}

// InfoListVisitor 把一组既有的 Info 适配成 Visitor，便于复用遍历逻辑。
type InfoListVisitor []*Info

// Visit 顺序回调每个 Info；注意错误会传入下一次回调，由调用方决定是否中止。
func (infos InfoListVisitor) Visit(fn VisitorFunc) error {
	var err error
	for _, i := range infos {
		err = fn(i, err)
	}
	return err
}
