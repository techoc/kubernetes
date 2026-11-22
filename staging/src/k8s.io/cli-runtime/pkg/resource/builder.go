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
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured/unstructuredscheme"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"sigs.k8s.io/kustomize/kyaml/filesys"
)

// FileExtensions 是默认被视为清单文件的扩展名；目录递归时据此过滤。
var FileExtensions = []string{".json", ".yaml", ".yml"}

// InputExtensions 在文件扩展名之外加上标准输入这个特殊“来源”。
var InputExtensions = append(FileExtensions, "stdin")

const defaultHttpGetAttempts = 3
const pathNotExistError = "the path %q does not exist"

// Builder provides convenience functions for taking arguments and parameters
// from the command line and converting them to a list of resources to iterate
// over using the Visitor interface.
//
// Builder 是链式配置器：每个方法只记录意图，真正解析资源并构造 Visitor 发生在 Do()。
// 配置阶段发现的错误会累积到 errs，而不是立即 panic，便于一次性反馈给用户。
type Builder struct {
	categoryExpanderFn CategoryExpanderFunc

	// mapper is set explicitly by resource builders
	mapper *mapper

	// clientConfigFn is a function to produce a client, *if* you need one
	clientConfigFn ClientConfigFunc

	restMapperFn RESTMapperFunc

	// objectTyper is statically determinant per-command invocation based on your internal or unstructured choice
	// it does not ever need to rely upon discovery.
	objectTyper runtime.ObjectTyper

	// codecFactory describes which codecs you want to use
	negotiatedSerializer runtime.NegotiatedSerializer

	// local indicates that we cannot make server calls
	local bool

	// errs 累积配置阶段错误；Do() 时统一返回。
	errs []error

	// paths 是本地文件/URL/stdin 来源；stream 标记输入是否为流式（不可重放）。
	paths      []Visitor
	stream     bool
	stdinInUse bool
	dir        bool

	// visitorConcurrency 控制并发遍历的数量，0/1 表示串行。
	visitorConcurrency int

	// selector 相关配置。selectAll 与显式 selector 互斥。
	labelSelector     *string
	fieldSelector     *string
	selectAll         bool
	limitChunks       int64
	requestTransforms []RequestTransform

	// resources 是按类型查询（无名字）的资源；subresource 指定子资源路径。
	resources   []string
	subresource string

	// namespace 相关：allNamespace 表示跨全部命名空间。
	namespace    string
	allNamespace bool
	names        []string

	resourceTuples []resourceTuple

	defaultNamespace bool
	requireNamespace bool

	flatten bool
	latest  bool

	requireObject bool

	singleResourceType bool
	continueOnError    bool

	// singleItemImplied 表示输入形态本身就暗示“单个对象”，用于结果语义判断。
	singleItemImplied bool

	schema ContentValidator

	// fakeClientFn is used for testing
	fakeClientFn FakeClientFunc
}

// missingResourceError 在既没有文件、也没有资源参数时作为用法错误返回；
// IsUsageError 依赖这个具体值来判断“是否需要打印帮助”。
var missingResourceError = fmt.Errorf(`You must provide one or more resources by argument or filename.
Example resource specifications include:
   '-f rsrc.yaml'
   '--filename=rsrc.json'
   '<resource> <name>'
   '<resource>'`)

// LocalResourceError 是 --local 模式下只允许 -f 输入的错误提示。
var LocalResourceError = errors.New(`error: you must specify resources by --filename when --local is set.
Example resource specifications include:
   '-f rsrc.yaml'
   '--filename=rsrc.json'`)

// StdinMultiUseError 表示同一进程内有两个来源都想读 stdin。
var StdinMultiUseError = errors.New("standard input cannot be used for multiple arguments")

// ErrMultipleResourceTypes is returned when Builder.SingleResourceType() was called,
// but multiple resource types were specified.
var ErrMultipleResourceTypes = errors.New("you may only specify a single resource type")

// TODO: expand this to include other errors.
// IsUsageError 目前只认出“缺少资源参数”这一种用法错误，历史行为如此；
// 新增用法错误时需要同步扩展，否则调用方不会打印帮助。
func IsUsageError(err error) bool {
	if err == nil {
		return false
	}
	return err == missingResourceError
}

// FilenameOptions 承载 -f/-k/-R 的原始输入，由 FilenameParam 负责校验和展开。
type FilenameOptions struct {
	Filenames []string
	Kustomize string
	Recursive bool
}

// validate 检查互斥的 flag 组合；-k 与 -f 不能同时出现，-k 也不支持 -R。
func (o *FilenameOptions) validate() []error {
	var errs []error
	if len(o.Filenames) > 0 && len(o.Kustomize) > 0 {
		errs = append(errs, fmt.Errorf("only one of -f or -k can be specified"))
	}
	if len(o.Kustomize) > 0 && o.Recursive {
		errs = append(errs, fmt.Errorf("the -k flag can't be used with -f or -R"))
	}
	return errs
}

// RequireFilenameOrKustomize 用于“必须从文件输入”的命令（如 create/apply）。
func (o *FilenameOptions) RequireFilenameOrKustomize() error {
	if len(o.Filenames) == 0 && len(o.Kustomize) == 0 {
		return fmt.Errorf("must specify one of -f and -k")
	}
	return nil
}

// resourceTuple 表示 "resource/name" 形式的单个目标。
type resourceTuple struct {
	Resource string
	Name     string
}

// FakeClientFunc 允许注入测试用客户端，参数为资源所属的 GroupVersion。
type FakeClientFunc func(version schema.GroupVersion) (RESTClient, error)

// NewFakeBuilder 构造一个使用注入客户端的 Builder，仅用于测试。
func NewFakeBuilder(fakeClientFn FakeClientFunc, restMapper RESTMapperFunc, categoryExpander CategoryExpanderFunc) *Builder {
	ret := newBuilder(nil, restMapper, categoryExpander)
	ret.fakeClientFn = fakeClientFn
	return ret
}

// NewBuilder creates a builder that operates on generic objects. At least one of
// internal or unstructured must be specified.
// TODO: Add versioned client (although versioned is still lossy)
// TODO remove internal and unstructured mapper and instead have them set the negotiated serializer for use in the client
//
// newBuilder 只搭骨架，mapper/typer 由 Unstructured 或 WithScheme 决定。
func newBuilder(clientConfigFn ClientConfigFunc, restMapper RESTMapperFunc, categoryExpander CategoryExpanderFunc) *Builder {
	return &Builder{
		clientConfigFn:     clientConfigFn,
		restMapperFn:       restMapper,
		categoryExpanderFn: categoryExpander,
		requireObject:      true,
	}
}

// noopClientGetter implements RESTClientGetter returning only errors.
// used as a dummy getter in a local-only builder.
type noopClientGetter struct{}

// 三个方法都显式失败，确保 --local 模式下永远不会意外访问集群。
func (noopClientGetter) ToRESTConfig() (*rest.Config, error) {
	return nil, fmt.Errorf("local operation only")
}
func (noopClientGetter) ToDiscoveryClient() (discovery.CachedDiscoveryInterface, error) {
	return nil, fmt.Errorf("local operation only")
}
func (noopClientGetter) ToRESTMapper() (meta.RESTMapper, error) {
	return nil, fmt.Errorf("local operation only")
}

// NewLocalBuilder returns a builder that is configured not to create REST clients and avoids asking the server for results.
func NewLocalBuilder() *Builder {
	return NewBuilder(noopClientGetter{}).Local()
}

// NewBuilder 用 RESTClientGetter 的配置、mapper 和 discovery 构造通用 Builder。
// categoryExpander 加了缓存，避免每个参数都触发一次 discovery。
func NewBuilder(restClientGetter RESTClientGetter) *Builder {
	categoryExpanderFn := func() (restmapper.CategoryExpander, error) {
		discoveryClient, err := restClientGetter.ToDiscoveryClient()
		if err != nil {
			return nil, err
		}
		return restmapper.NewDiscoveryCategoryExpander(discoveryClient), err
	}

	return newBuilder(
		restClientGetter.ToRESTConfig,
		restClientGetter.ToRESTMapper,
		(&cachingCategoryExpanderFunc{delegate: categoryExpanderFn}).ToCategoryExpander,
	)
}

// Schema 设置文件输入的 schema 校验器；nil 表示不校验。
func (b *Builder) Schema(schema ContentValidator) *Builder {
	b.schema = schema
	return b
}

// AddError 追加一个配置错误；nil 会被忽略，方便调用方无脑串联。
func (b *Builder) AddError(err error) *Builder {
	if err == nil {
		return b
	}
	b.errs = append(b.errs, err)
	return b
}

// VisitorConcurrency sets the number of concurrent visitors to use when
// visiting lists.
func (b *Builder) VisitorConcurrency(concurrency int) *Builder {
	b.visitorConcurrency = concurrency
	return b
}

// FilenameParam groups input in two categories: URLs and files (files, directories, STDIN)
// If enforceNamespace is false, namespaces in the specs will be allowed to
// override the default namespace. If it is true, namespaces that don't match
// will cause an error.
// If ContinueOnError() is set prior to this method, objects on the path that are not
// recognized will be ignored (but logged at V(2)).
func (b *Builder) FilenameParam(enforceNamespace bool, filenameOptions *FilenameOptions) *Builder {
	if errs := filenameOptions.validate(); len(errs) > 0 {
		b.errs = append(b.errs, errs...)
		return b
	}
	recursive := filenameOptions.Recursive
	paths := filenameOptions.Filenames
	for _, s := range paths {
		switch {
		// "-" 是 stdin 的约定写法。
		case s == "-":
			b.Stdin()
		// 只识别 http(s)；其他 scheme 会落到本地路径分支并报不存在。
		case strings.Index(s, "http://") == 0 || strings.Index(s, "https://") == 0:
			url, err := url.Parse(s)
			if err != nil {
				b.errs = append(b.errs, fmt.Errorf("the URL passed to filename %q is not valid: %v", s, err))
				continue
			}
			b.URL(defaultHttpGetAttempts, url)
		default:
			// 允许 shell 通配符展开；单文件且非递归时暗示“单个对象”。
			matches, err := expandIfFilePattern(s)
			if err != nil {
				b.errs = append(b.errs, err)
				continue
			}
			if !recursive && len(matches) == 1 {
				b.singleItemImplied = true
			}
			b.Path(recursive, matches...)
		}
	}
	// -k 直接构造 KustomizeVisitor，与普通文件路径并列。
	if filenameOptions.Kustomize != "" {
		b.paths = append(
			b.paths,
			&KustomizeVisitor{
				mapper:  b.mapper,
				dirPath: filenameOptions.Kustomize,
				schema:  b.schema,
				fSys:    filesys.MakeFsOnDisk(),
			})
	}

	if enforceNamespace {
		b.RequireNamespace()
	}

	return b
}

// Unstructured updates the builder so that it will request and send unstructured
// objects. Unstructured objects preserve all fields sent by the server in a map format
// based on the object's JSON structure which means no data is lost when the client
// reads and then writes an object. Use this mode in preference to Internal unless you
// are working with Go types directly.
//
// 与 WithScheme 互斥：mapper 已存在时视为调用方配置冲突。
func (b *Builder) Unstructured() *Builder {
	if b.mapper != nil {
		b.errs = append(b.errs, fmt.Errorf("another mapper was already selected, cannot use unstructured types"))
		return b
	}
	b.objectTyper = unstructuredscheme.NewUnstructuredObjectTyper()
	b.mapper = &mapper{
		localFn:      b.isLocal,
		restMapperFn: b.restMapperFn,
		clientFn:     b.getClient,
		decoder:      &metadataValidatingDecoder{unstructured.UnstructuredJSONScheme},
	}

	return b
}

// WithScheme uses the scheme to manage typing, conversion (optional), and decoding.  If decodingVersions
// is empty, then you can end up with internal types.  You have been warned.
func (b *Builder) WithScheme(scheme *runtime.Scheme, decodingVersions ...schema.GroupVersion) *Builder {
	if b.mapper != nil {
		b.errs = append(b.errs, fmt.Errorf("another mapper was already selected, cannot use internal types"))
		return b
	}
	b.objectTyper = scheme
	codecFactory := serializer.NewCodecFactory(scheme)
	negotiatedSerializer := runtime.NegotiatedSerializer(codecFactory)
	// if you specified versions, you're specifying a desire for external types, which you don't want to round-trip through
	// internal types
	// 指定版本即表示要外部类型，禁用转换可避免对象被转成 internal 版本。
	if len(decodingVersions) > 0 {
		negotiatedSerializer = codecFactory.WithoutConversion()
	}
	b.negotiatedSerializer = negotiatedSerializer

	b.mapper = &mapper{
		localFn:      b.isLocal,
		restMapperFn: b.restMapperFn,
		clientFn:     b.getClient,
		decoder:      codecFactory.UniversalDecoder(decodingVersions...),
	}

	return b
}

// LocalParam calls Local() if local is true.
func (b *Builder) LocalParam(local bool) *Builder {
	if local {
		b.Local()
	}
	return b
}

// Local will avoid asking the server for results.
//
// 本地模式仍然需要 mapper/typer 解析文件内容，只是不会创建 REST 客户端。
func (b *Builder) Local() *Builder {
	b.local = true
	return b
}

// isLocal 作为 mapper 的 localFn，让 mapper 在解析时也能感知本地模式。
func (b *Builder) isLocal() bool {
	return b.local
}

// Mapper returns a copy of the current mapper.
//
// 返回副本而不是指针本身，防止调用方修改 Builder 的内部状态。
func (b *Builder) Mapper() *mapper {
	mapper := *b.mapper
	return &mapper
}

// URL accepts a number of URLs directly.
//
// 每个 URL 一个 URLVisitor，共用同一个 mapper/schema。
func (b *Builder) URL(httpAttemptCount int, urls ...*url.URL) *Builder {
	for _, u := range urls {
		b.paths = append(b.paths, &URLVisitor{
			URL:              u,
			StreamVisitor:    NewStreamVisitor(nil, b.mapper, u.String(), b.schema),
			HttpAttemptCount: httpAttemptCount,
		})
	}
	return b
}

// Stdin will read objects from the standard input. If ContinueOnError() is set
// prior to this method being called, objects in the stream that are unrecognized
// will be ignored (but logged at V(2)). If StdinInUse() is set prior to this method
// being called, an error will be recorded as there are multiple entities trying to use
// the single standard input stream.
func (b *Builder) Stdin() *Builder {
	b.stream = true
	// stdin 只有一个，重复使用必须报错，否则两个来源会互相抢数据。
	if b.stdinInUse {
		b.errs = append(b.errs, StdinMultiUseError)
	}
	b.stdinInUse = true
	b.paths = append(b.paths, FileVisitorForSTDIN(b.mapper, b.schema))
	return b
}

// StdinInUse will mark standard input as in use by this Builder, and therefore standard
// input should not be used by another entity. If Stdin() is set prior to this method
// being called, an error will be recorded as there are multiple entities trying to use
// the single standard input stream.
func (b *Builder) StdinInUse() *Builder {
	// 记录占用但不再追加 visitor，供“只想声明 stdin 被别处使用”的场景。
	if b.stdinInUse {
		b.errs = append(b.errs, StdinMultiUseError)
	}
	b.stdinInUse = true
	return b
}

// Stream will read objects from the provided reader, and if an error occurs will
// include the name string in the error message. If ContinueOnError() is set
// prior to this method being called, objects in the stream that are unrecognized
// will be ignored (but logged at V(2)).
func (b *Builder) Stream(r io.Reader, name string) *Builder {
	// stream 模式下输入不可重放，Result 只应遍历一次。
	b.stream = true
	b.paths = append(b.paths, NewStreamVisitor(r, b.mapper, name, b.schema))
	return b
}

// Path accepts a set of paths that may be files, directories (all can containing
// one or more resources). Creates a FileVisitor for each file and then each
// FileVisitor is streaming the content to a StreamVisitor. If ContinueOnError() is set
// prior to this method being called, objects on the path that are unrecognized will be
// ignored (but logged at V(2)).
func (b *Builder) Path(recursive bool, paths ...string) *Builder {
	for _, p := range paths {
		// 先做存在性检查，把“路径不存在”和“无法读取”区分开，错误更具体。
		_, err := os.Stat(p)
		if os.IsNotExist(err) {
			b.errs = append(b.errs, fmt.Errorf(pathNotExistError, p))
			continue
		}
		if err != nil {
			b.errs = append(b.errs, fmt.Errorf("the path %q cannot be accessed: %v", p, err))
			continue
		}

		visitors, err := ExpandPathsToFileVisitors(b.mapper, p, recursive, FileExtensions, b.schema)
		if err != nil {
			b.errs = append(b.errs, fmt.Errorf("error reading %q: %v", p, err))
		}
		// 目录会展开成多个 visitor，结果不应再被当作“单个对象”。
		if len(visitors) > 1 {
			b.dir = true
		}

		b.paths = append(b.paths, visitors...)
	}
	if len(b.paths) == 0 && len(b.errs) == 0 {
		b.errs = append(b.errs, fmt.Errorf("error reading %v: recognized file extensions are %v", paths, FileExtensions))
	}
	return b
}

// ResourceTypes is a list of types of resources to operate on, when listing objects on
// the server or retrieving objects that match a selector.
func (b *Builder) ResourceTypes(types ...string) *Builder {
	b.resources = append(b.resources, types...)
	return b
}

// ResourceNames accepts a default type and one or more names, and creates tuples of
// resources
//
// name 已包含 "type/name" 形式时以其为准；否则用 resource 作为默认类型。
func (b *Builder) ResourceNames(resource string, names ...string) *Builder {
	for _, name := range names {
		// See if this input string is of type/name format
		tuple, ok, err := splitResourceTypeName(name)
		if err != nil {
			b.errs = append(b.errs, err)
			return b
		}

		if ok {
			b.resourceTuples = append(b.resourceTuples, tuple)
			continue
		}
		if len(resource) == 0 {
			b.errs = append(b.errs, fmt.Errorf("the argument %q must be RESOURCE/NAME", name))
			continue
		}

		// Use the given default type to create a resource tuple
		b.resourceTuples = append(b.resourceTuples, resourceTuple{Resource: resource, Name: name})
	}
	return b
}

// LabelSelectorParam defines a selector that should be applied to the object types to load.
// This will not affect files loaded from disk or URL. If the parameter is empty it is
// a no-op - to select all resources invoke `b.LabelSelector(labels.Everything.String)`.
//
// 空字符串按“未指定”处理，避免误把全部资源当成筛选结果。
func (b *Builder) LabelSelectorParam(s string) *Builder {
	selector := strings.TrimSpace(s)
	if len(selector) == 0 {
		return b
	}
	if b.selectAll {
		b.errs = append(b.errs, fmt.Errorf("found non-empty label selector %q with previously set 'all' parameter. ", s))
		return b
	}
	return b.LabelSelector(selector)
}

// LabelSelector accepts a selector directly and will filter the resulting list by that object.
// Use LabelSelectorParam instead for user input.
//
// 与服务端 selector 不同：本地路径的结果会用 FilterByLabelSelector 在客户端过滤。
func (b *Builder) LabelSelector(selector string) *Builder {
	if len(selector) == 0 {
		return b
	}

	b.labelSelector = &selector
	return b
}

// FieldSelectorParam defines a selector that should be applied to the object types to load.
// This will not affect files loaded from disk or URL. If the parameter is empty it is
// a no-op - to select all resources.
func (b *Builder) FieldSelectorParam(s string) *Builder {
	s = strings.TrimSpace(s)
	// 空白输入视为未指定。field selector 只能交给服务端，本地文件不支持。
	if len(s) == 0 {
		return b
	}
	if b.selectAll {
		b.errs = append(b.errs, fmt.Errorf("found non-empty field selector %q with previously set 'all' parameter. ", s))
		return b
	}
	b.fieldSelector = &s
	return b
}

// NamespaceParam accepts the namespace that these resources should be
// considered under from - used by DefaultNamespace() and RequireNamespace()
func (b *Builder) NamespaceParam(namespace string) *Builder {
	b.namespace = namespace
	return b
}

// DefaultNamespace instructs the builder to set the namespace value for any object found
// to NamespaceParam() if empty.
func (b *Builder) DefaultNamespace() *Builder {
	b.defaultNamespace = true
	return b
}

// AllNamespaces instructs the builder to metav1.NamespaceAll as a namespace to request resources
// across all of the namespace. This overrides the namespace set by NamespaceParam().
func (b *Builder) AllNamespaces(allNamespace bool) *Builder {
	// NamespaceAll 是空字符串，作为 List 的 namespace 参数即表示跨全部命名空间。
	if allNamespace {
		b.namespace = metav1.NamespaceAll
	}
	b.allNamespace = allNamespace
	return b
}

// RequireNamespace instructs the builder to set the namespace value for any object found
// to NamespaceParam() if empty, and if the value on the resource does not match
// NamespaceParam() an error will be returned.
func (b *Builder) RequireNamespace() *Builder {
	b.requireNamespace = true
	return b
}

// RequestChunksOf attempts to load responses from the server in batches of size limit
// to avoid long delays loading and transferring very large lists. If unset defaults to
// no chunking.
func (b *Builder) RequestChunksOf(chunkSize int64) *Builder {
	b.limitChunks = chunkSize
	return b
}

// TransformRequests alters API calls made by clients requested from this builder. Pass
// an empty list to clear modifiers.
func (b *Builder) TransformRequests(opts ...RequestTransform) *Builder {
	b.requestTransforms = opts
	return b
}

// Subresource instructs the builder to retrieve the object at the
// subresource path instead of the main resource path.
func (b *Builder) Subresource(subresource string) *Builder {
	b.subresource = subresource
	return b
}

// SelectAllParam 对应 kubectl get --all，选择指定类型下的全部对象。
// 原注释误写为 SelectEverythingParam，这里一并纠正以免 godoc 检索不到。
func (b *Builder) SelectAllParam(selectAll bool) *Builder {
	// --all 与显式 selector 语义冲突，同时出现时报错而不是静默取其一。
	if selectAll && (b.labelSelector != nil || b.fieldSelector != nil) {
		b.errs = append(b.errs, fmt.Errorf("setting 'all' parameter but found a non empty selector. "))
		return b
	}
	b.selectAll = selectAll
	return b
}

// ResourceTypeOrNameArgs indicates that the builder should accept arguments
// of the form `(<type1>[,<type2>,...]|<type> <name1>[,<name2>,...])`. When one argument is
// received, the types provided will be retrieved from the server (and be comma delimited).
// When two or more arguments are received, they must be a single type and resource name(s).
// The allowEmptySelector permits to select all the resources (via Everything func).
func (b *Builder) ResourceTypeOrNameArgs(allowEmptySelector bool, args ...string) *Builder {
	// 把 "a,b c" 这种多资源多名字的组合展开为 "a/c b/c" 形式。
	args = normalizeMultipleResourcesArgs(args)
	if ok, err := hasCombinedTypeArgs(args); ok {
		if err != nil {
			b.errs = append(b.errs, err)
			return b
		}
		for _, s := range args {
			tuple, ok, err := splitResourceTypeName(s)
			if err != nil {
				b.errs = append(b.errs, err)
				return b
			}
			if ok {
				b.resourceTuples = append(b.resourceTuples, tuple)
			}
		}
		return b
	}
	if len(args) > 0 {
		// Try replacing aliases only in types
		// 只替换类型位置的别名（如 all->pods,rc,svc），名字位置保持原样。
		args[0] = b.ReplaceAliases(args[0])
	}
	switch {
	// 1 个参数：类型列表；2 个：类型 + 名字；3 个及以上：类型 + 多个名字。
	case len(args) > 2:
		b.names = append(b.names, args[1:]...)
		b.ResourceTypes(SplitResourceArgument(args[0])...)
	case len(args) == 2:
		b.names = append(b.names, args[1])
		b.ResourceTypes(SplitResourceArgument(args[0])...)
	case len(args) == 1:
		b.ResourceTypes(SplitResourceArgument(args[0])...)
		if b.labelSelector == nil && allowEmptySelector {
			selector := labels.Everything().String()
			b.labelSelector = &selector
		}
	case len(args) == 0:
	default:
		b.errs = append(b.errs, fmt.Errorf("arguments must consist of a resource or a resource and name"))
	}
	return b
}

// ReplaceAliases accepts an argument and tries to expand any existing
// aliases found in it
//
// 别名来自 discovery 的 category（如 all、no、tier）；展开失败时保留原参数，
// 让后续 mappingFor 给出更准确的 “resource type not found” 错误。
func (b *Builder) ReplaceAliases(input string) string {
	replaced := []string{}
	for _, arg := range strings.Split(input, ",") {
		if b.categoryExpanderFn == nil {
			continue
		}
		categoryExpander, err := b.categoryExpanderFn()
		if err != nil {
			b.AddError(err)
			continue
		}

		if resources, ok := categoryExpander.Expand(arg); ok {
			asStrings := []string{}
			for _, resource := range resources {
				if len(resource.Group) == 0 {
					asStrings = append(asStrings, resource.Resource)
					continue
				}
				asStrings = append(asStrings, resource.Resource+"."+resource.Group)
			}
			arg = strings.Join(asStrings, ",")
		}
		replaced = append(replaced, arg)
	}
	return strings.Join(replaced, ",")
}

// hasCombinedTypeArgs 判断参数是否全部为 resource/name 形式；
// 混合形式（部分带斜杠）通常意味着用户多写了一个类型参数，给出明确纠正提示。
func hasCombinedTypeArgs(args []string) (bool, error) {
	hasSlash := 0
	for _, s := range args {
		if strings.Contains(s, "/") {
			hasSlash++
		}
	}
	switch {
	case hasSlash > 0 && hasSlash == len(args):
		return true, nil
	case hasSlash > 0 && hasSlash != len(args):
		// 错误信息里带上真实二进制名，让提示可以直接复制执行。
		baseCmd := "cmd"
		if len(os.Args) > 0 {
			baseCmdSlice := strings.Split(os.Args[0], "/")
			baseCmd = baseCmdSlice[len(baseCmdSlice)-1]
		}
		return true, fmt.Errorf("there is no need to specify a resource type as a separate argument when passing arguments in resource/name form (e.g. '%s get resource/<resource_name>' instead of '%s get resource resource/<resource_name>'", baseCmd, baseCmd)
	default:
		return false, nil
	}
}

// Normalize args convert multiple resources to resource tuples, a,b,c d
// as a transform to a/d b/d c/d
func normalizeMultipleResourcesArgs(args []string) []string {
	if len(args) >= 2 {
		resources := []string{}
		resources = append(resources, SplitResourceArgument(args[0])...)
		// 只有第一个参数确实是多资源（含逗号）时才展开，否则保持原样。
		if len(resources) > 1 {
			names := []string{}
			names = append(names, args[1:]...)
			newArgs := []string{}
			for _, resource := range resources {
				for _, name := range names {
					newArgs = append(newArgs, strings.Join([]string{resource, name}, "/"))
				}
			}
			return newArgs
		}
	}
	return args
}

// splitResourceTypeName handles type/name resource formats and returns a resource tuple
// (empty or not), whether it successfully found one, and an error
func splitResourceTypeName(s string) (resourceTuple, bool, error) {
	// 不含斜杠不是错误，只是不匹配该形式。
	if !strings.Contains(s, "/") {
		return resourceTuple{}, false, nil
	}
	// 多于一个斜杠无法区分 type/name，明确报错。
	seg := strings.Split(s, "/")
	if len(seg) != 2 {
		return resourceTuple{}, false, fmt.Errorf("arguments in resource/name form may not have more than one slash")
	}
	resource, name := seg[0], seg[1]
	if len(resource) == 0 || len(name) == 0 || len(SplitResourceArgument(resource)) != 1 {
		return resourceTuple{}, false, fmt.Errorf("arguments in resource/name form must have a single resource and name")
	}
	return resourceTuple{Resource: resource, Name: name}, true, nil
}

// Flatten will convert any objects with a field named "Items" that is an array of runtime.Object
// compatible types into individual entries and give them their own items. The original object
// is not passed to any visitors.
func (b *Builder) Flatten() *Builder {
	b.flatten = true
	return b
}

// Latest will fetch the latest copy of any objects loaded from URLs or files from the server.
func (b *Builder) Latest() *Builder {
	b.latest = true
	return b
}

// RequireObject ensures that resulting infos have an object set. If false, resulting info may not have an object set.
func (b *Builder) RequireObject(require bool) *Builder {
	b.requireObject = require
	return b
}

// ContinueOnError will attempt to load and visit as many objects as possible, even if some visits
// return errors or some objects cannot be loaded. The default behavior is to terminate after
// the first error is returned from a VisitorFunc.
func (b *Builder) ContinueOnError() *Builder {
	b.continueOnError = true
	return b
}

// SingleResourceType will cause the builder to error if the user specifies more than a single type
// of resource.
func (b *Builder) SingleResourceType() *Builder {
	b.singleResourceType = true
	return b
}

// mappingFor returns the RESTMapping for the Kind given, or the Kind referenced by the resource.
// Prefers a fully specified GroupVersionResource match. If one is not found, we match on a fully
// specified GroupVersionKind, or fallback to a match on GroupKind.
//
// 解析顺序刻意宽松：resource 简写、带 group/version 的全限定形式都接受，
// 最后才退化为 GroupKind，尽量兼容用户的各种写法。
func (b *Builder) mappingFor(resourceOrKindArg string) (*meta.RESTMapping, error) {
	fullySpecifiedGVR, groupResource := schema.ParseResourceArg(resourceOrKindArg)
	gvk := schema.GroupVersionKind{}
	restMapper, err := b.restMapperFn()
	if err != nil {
		return nil, err
	}

	if fullySpecifiedGVR != nil {
		gvk, _ = restMapper.KindFor(*fullySpecifiedGVR)
	}
	if gvk.Empty() {
		gvk, _ = restMapper.KindFor(groupResource.WithVersion(""))
	}
	if !gvk.Empty() {
		return restMapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	}

	fullySpecifiedGVK, groupKind := schema.ParseKindArg(resourceOrKindArg)
	if fullySpecifiedGVK == nil {
		gvk := groupKind.WithVersion("")
		fullySpecifiedGVK = &gvk
	}

	if !fullySpecifiedGVK.Empty() {
		if mapping, err := restMapper.RESTMapping(fullySpecifiedGVK.GroupKind(), fullySpecifiedGVK.Version); err == nil {
			return mapping, nil
		}
	}

	mapping, err := restMapper.RESTMapping(groupKind, gvk.Version)
	if err != nil {
		// if we error out here, it is because we could not match a resource or a kind
		// for the given argument. To maintain consistency with previous behavior,
		// announce that a resource type could not be found.
		// if the error is _not_ a *meta.NoKindMatchError, then we had trouble doing discovery,
		// so we should return the original error since it may help a user diagnose what is actually wrong
		if meta.IsNoMatchError(err) {
			// 按用户输入里存在的维度拼出具体提示，避免笼统的 "not found"。
			switch {
			case len(groupResource.Group) > 0 && len(gvk.Version) > 0:
				return nil, fmt.Errorf("the server doesn't have a resource type %q in group %q and version %q", groupResource.Resource, groupResource.Group, gvk.Version)
			case len(groupResource.Group) > 0:
				return nil, fmt.Errorf("the server doesn't have a resource type %q in group %q", groupResource.Resource, groupResource.Group)
			case len(gvk.Version) > 0:
				return nil, fmt.Errorf("the server doesn't have a resource type %q in version %q", groupResource.Resource, gvk.Version)
			default:
				return nil, fmt.Errorf("the server doesn't have a resource type %q", groupResource.Resource)
			}
		}
		return nil, err
	}

	return mapping, nil
}

// resourceMappings 解析所有资源类型并去重（同一 GVK 的短名/复数只保留一个）。
func (b *Builder) resourceMappings() ([]*meta.RESTMapping, error) {
	if len(b.resources) > 1 && b.singleResourceType {
		return nil, ErrMultipleResourceTypes
	}
	mappings := []*meta.RESTMapping{}
	seen := map[schema.GroupVersionKind]bool{}
	for _, r := range b.resources {
		mapping, err := b.mappingFor(r)
		if err != nil {
			return nil, err
		}
		// This ensures the mappings for resources(shortcuts, plural) unique
		if seen[mapping.GroupVersionKind] {
			continue
		}
		seen[mapping.GroupVersionKind] = true

		mappings = append(mappings, mapping)
	}
	return mappings, nil
}

// resourceTupleMappings 把每个 resourceTuples 里的资源名解析成 mapping；
// canonical 用来判断是否真的指定了多种资源（供 SingleResourceType 校验）。
func (b *Builder) resourceTupleMappings() (map[string]*meta.RESTMapping, error) {
	mappings := make(map[string]*meta.RESTMapping)
	canonical := make(map[schema.GroupVersionResource]struct{})
	for _, r := range b.resourceTuples {
		if _, ok := mappings[r.Resource]; ok {
			continue
		}
		mapping, err := b.mappingFor(r.Resource)
		if err != nil {
			return nil, err
		}

		mappings[r.Resource] = mapping
		canonical[mapping.Resource] = struct{}{}
	}
	if len(canonical) > 1 && b.singleResourceType {
		return nil, ErrMultipleResourceTypes
	}
	return mappings, nil
}

// visitorResult 按输入来源优先级选择遍历策略：
// 文件/URL > selector > resource/name > 名字列表 > 仅资源类型。
func (b *Builder) visitorResult() *Result {
	// 配置阶段有错就提前返回，不再猜测用户意图。
	if len(b.errs) > 0 {
		return &Result{err: utilerrors.NewAggregate(b.errs)}
	}

	if b.selectAll {
		selector := labels.Everything().String()
		b.labelSelector = &selector
	}

	// visit items specified by paths
	if len(b.paths) != 0 {
		return b.visitByPaths()
	}

	// visit selectors
	if b.labelSelector != nil || b.fieldSelector != nil {
		return b.visitBySelector()
	}

	// visit items specified by resource and name
	if len(b.resourceTuples) != 0 {
		return b.visitByResource()
	}

	// visit items specified by name
	if len(b.names) != 0 {
		return b.visitByName()
	}

	if len(b.resources) != 0 {
		for _, r := range b.resources {
			_, err := b.mappingFor(r)
			if err != nil {
				return &Result{err: err}
			}
		}
		return &Result{err: fmt.Errorf("resource(s) were provided, but no name was specified")}
	}
	return &Result{err: missingResourceError}
}

// visitBySelector 为每种资源类型构造一个 Selector，命名空间型资源才带 namespace。
func (b *Builder) visitBySelector() *Result {
	result := &Result{
		targetsSingleItems: false,
	}

	// 以下组合均无意义，提前给出明确错误而不是让服务端报错。
	if len(b.names) != 0 {
		return result.withError(fmt.Errorf("name cannot be provided when a selector is specified"))
	}
	if len(b.resourceTuples) != 0 {
		return result.withError(fmt.Errorf("selectors and the all flag cannot be used when passing resource/name arguments"))
	}
	if len(b.resources) == 0 {
		return result.withError(fmt.Errorf("at least one resource must be specified to use a selector"))
	}
	if len(b.subresource) != 0 {
		return result.withError(fmt.Errorf("subresource cannot be used when bulk resources are specified"))
	}

	mappings, err := b.resourceMappings()
	if err != nil {
		result.err = err
		return result
	}

	var labelSelector, fieldSelector string
	if b.labelSelector != nil {
		labelSelector = *b.labelSelector
	}
	if b.fieldSelector != nil {
		fieldSelector = *b.fieldSelector
	}

	visitors := []Visitor{}
	for _, mapping := range mappings {
		client, err := b.getClient(mapping.GroupVersionKind.GroupVersion())
		if err != nil {
			result.err = err
			return result
		}
		// 集群级资源不接受 namespace 参数，留空避免服务端 404。
		selectorNamespace := b.namespace
		if mapping.Scope.Name() != meta.RESTScopeNameNamespace {
			selectorNamespace = ""
		}
		visitors = append(visitors, NewSelector(client, mapping, selectorNamespace, labelSelector, fieldSelector, b.limitChunks))
	}
	// ContinueOnError 用 EagerVisitorList 收集所有错误；否则顺序执行、遇错即停。
	if b.continueOnError {
		result.visitor = EagerVisitorList(visitors)
	} else {
		result.visitor = VisitorList(visitors)
	}
	result.sources = visitors
	return result
}

// getClient 按 builder 的类型模式选择客户端：fake > 有 negotiatedSerializer 的强类型
// 客户端 > unstructured 客户端。requestTransforms 在最后统一套上。
func (b *Builder) getClient(gv schema.GroupVersion) (RESTClient, error) {
	var (
		client RESTClient
		err    error
	)

	switch {
	case b.fakeClientFn != nil:
		client, err = b.fakeClientFn(gv)
	case b.negotiatedSerializer != nil:
		client, err = b.clientConfigFn.withStdinUnavailable(b.stdinInUse).clientForGroupVersion(gv, b.negotiatedSerializer)
	default:
		client, err = b.clientConfigFn.withStdinUnavailable(b.stdinInUse).unstructuredClientForGroupVersion(gv)
	}

	if err != nil {
		return nil, err
	}

	return NewClientWithOptions(client, b.requestTransforms...), nil
}

// visitByResource 处理 "resource/name" 形式；每种资源类型复用一个客户端。
func (b *Builder) visitByResource() *Result {
	// if b.singleItemImplied is false, this could be by default, so double-check length
	// of resourceTuples to determine if in fact it is singleItemImplied or not
	// 只有一个 tuple 时也视为单对象语义，供调用方决定输出形态。
	isSingleItemImplied := b.singleItemImplied
	if !isSingleItemImplied {
		isSingleItemImplied = len(b.resourceTuples) == 1
	}

	result := &Result{
		singleItemImplied:  isSingleItemImplied,
		targetsSingleItems: true,
	}

	if len(b.resources) != 0 {
		return result.withError(fmt.Errorf("you may not specify individual resources and bulk resources in the same call"))
	}

	// retrieve one client for each resource
	mappings, err := b.resourceTupleMappings()
	if err != nil {
		result.err = err
		return result
	}
	clients := make(map[string]RESTClient)
	for _, mapping := range mappings {
		// key 用 GV + 资源名，避免不同 group/version 下的同名资源共用一个客户端。
		s := fmt.Sprintf("%s/%s", mapping.GroupVersionKind.GroupVersion().String(), mapping.Resource.Resource)
		if _, ok := clients[s]; ok {
			continue
		}
		client, err := b.getClient(mapping.GroupVersionKind.GroupVersion())
		if err != nil {
			result.err = err
			return result
		}
		clients[s] = client
	}

	items := []Visitor{}
	for _, tuple := range b.resourceTuples {
		mapping, ok := mappings[tuple.Resource]
		if !ok {
			return result.withError(fmt.Errorf("resource %q is not recognized: %v", tuple.Resource, mappings))
		}
		s := fmt.Sprintf("%s/%s", mapping.GroupVersionKind.GroupVersion().String(), mapping.Resource.Resource)
		client, ok := clients[s]
		if !ok {
			return result.withError(fmt.Errorf("could not find a client for resource %q", tuple.Resource))
		}

		// 按名字取单个资源必须确定 namespace；跨全部命名空间无法唯一定位。
		selectorNamespace := b.namespace
		if mapping.Scope.Name() != meta.RESTScopeNameNamespace {
			selectorNamespace = ""
		} else {
			if len(b.namespace) == 0 {
				errMsg := "namespace may not be empty when retrieving a resource by name"
				if b.allNamespace {
					errMsg = "a resource cannot be retrieved by name across all namespaces"
				}
				return result.withError(errors.New(errMsg))
			}
		}

		// Info 先只记录定位信息，Object 留空等待 RetrieveLazy/显式 Get。
		info := &Info{
			Client:      client,
			Mapping:     mapping,
			Namespace:   selectorNamespace,
			Name:        tuple.Name,
			Subresource: b.subresource,
		}
		items = append(items, info)
	}

	var visitors Visitor
	if b.continueOnError {
		visitors = EagerVisitorList(items)
	} else {
		visitors = VisitorList(items)
	}
	result.visitor = visitors
	result.sources = items
	return result
}

// visitByName 处理 "类型 + 单个/多个名字" 形式；类型必须唯一。
func (b *Builder) visitByName() *Result {
	result := &Result{
		// 多个名字时不暗示单对象，输出会按列表处理。
		singleItemImplied:  len(b.names) == 1,
		targetsSingleItems: true,
	}

	// 路径输入和参数输入不能混用。
	if len(b.paths) != 0 {
		return result.withError(fmt.Errorf("when paths, URLs, or stdin is provided as input, you may not specify a resource by arguments as well"))
	}
	if len(b.resources) == 0 {
		return result.withError(fmt.Errorf("you must provide a resource and a resource name together"))
	}
	if len(b.resources) > 1 {
		return result.withError(fmt.Errorf("you must specify only one resource"))
	}

	mappings, err := b.resourceMappings()
	if err != nil {
		result.err = err
		return result
	}
	mapping := mappings[0]

	client, err := b.getClient(mapping.GroupVersionKind.GroupVersion())
	if err != nil {
		result.err = err
		return result
	}

	selectorNamespace := b.namespace
	if mapping.Scope.Name() != meta.RESTScopeNameNamespace {
		selectorNamespace = ""
	} else {
		if len(b.namespace) == 0 {
			errMsg := "namespace may not be empty when retrieving a resource by name"
			if b.allNamespace {
				errMsg = "a resource cannot be retrieved by name across all namespaces"
			}
			return result.withError(errors.New(errMsg))
		}
	}

	// 多个名字共用同一个客户端和 mapping，只替换 Name。
	visitors := []Visitor{}
	for _, name := range b.names {
		info := &Info{
			Client:      client,
			Mapping:     mapping,
			Namespace:   selectorNamespace,
			Name:        name,
			Subresource: b.subresource,
		}
		visitors = append(visitors, info)
	}
	result.visitor = VisitorList(visitors)
	result.sources = visitors
	return result
}

// visitByPaths 处理文件/URL/stdin，并在本地结果上叠加 flatten/latest/selector 过滤。
func (b *Builder) visitByPaths() *Result {
	result := &Result{
		// 单个文件且非目录/流时暗示单对象；多文件或流式输入按列表处理。
		singleItemImplied:  !b.dir && !b.stream && len(b.paths) == 1,
		targetsSingleItems: true,
	}

	// 路径输入不能再同时给资源/名字参数。
	if len(b.resources) != 0 {
		return result.withError(fmt.Errorf("when paths, URLs, or stdin is provided as input, you may not specify resource arguments as well"))
	}
	if len(b.names) != 0 {
		return result.withError(fmt.Errorf("name cannot be provided when a path is specified"))
	}
	if len(b.resourceTuples) != 0 {
		return result.withError(fmt.Errorf("resource/name arguments cannot be provided when a path is specified"))
	}

	var visitors Visitor
	if b.continueOnError {
		visitors = EagerVisitorList(b.paths)
	} else {
		// 非 continueOnError 时允许多文件并发读取，顺序由装饰器保证语义不变。
		visitors = ConcurrentVisitorList{
			visitors:    b.paths,
			concurrency: b.visitorConcurrency,
		}
	}

	// flatten 放在 latest 之前：先展开成单对象，后续才能逐个从服务端取最新值。
	if b.flatten {
		visitors = NewFlattenListVisitor(visitors, b.objectTyper, b.mapper)
	}

	// only items from disk can be refetched
	if b.latest {
		// must set namespace prior to fetching
		if b.defaultNamespace {
			visitors = NewDecoratedVisitor(visitors, SetNamespace(b.namespace))
		}
		visitors = NewDecoratedVisitor(visitors, RetrieveLatest)
	}
	if b.labelSelector != nil {
		selector, err := labels.Parse(*b.labelSelector)
		if err != nil {
			return result.withError(fmt.Errorf("the provided selector %q is not valid: %v", *b.labelSelector, err))
		}
		visitors = NewFilteredVisitor(visitors, FilterByLabelSelector(selector))
	}
	result.visitor = visitors
	result.sources = b.paths
	return result
}

// Do returns a Result object with a Visitor for the resources identified by the Builder.
// The visitor will respect the error behavior specified by ContinueOnError. Note that stream
// inputs are consumed by the first execution - use Infos() or Object() on the Result to capture a list
// for further iteration.
func (b *Builder) Do() *Result {
	r := b.visitorResult()
	// 结果上挂 mapper，供后续按对象反查资源类型。
	r.mapper = b.Mapper()
	if r.err != nil {
		return r
	}
	// visitorResult 里已对路径 flatten 过一次；这里兜底处理其他来源。
	if b.flatten {
		r.visitor = NewFlattenListVisitor(r.visitor, b.objectTyper, b.mapper)
	}
	// 装饰器顺序很重要：先补 namespace，再校验，再清理，最后才真正取对象。
	helpers := []VisitorFunc{}
	if b.defaultNamespace {
		helpers = append(helpers, SetNamespace(b.namespace))
	}
	if b.requireNamespace {
		helpers = append(helpers, RequireNamespace(b.namespace))
	}
	helpers = append(helpers, FilterNamespace)
	// requireObject 决定 Info 是否在遍历时就发 GET；设为 false 可让调用方自己控制请求时机。
	if b.requireObject {
		helpers = append(helpers, RetrieveLazy)
	}
	if b.continueOnError {
		// 最外层再包一层，确保底层 visitor 的错误不会中断整批遍历。
		r.visitor = ContinueOnErrorVisitor{Visitor: r.visitor}
	}
	r.visitor = NewDecoratedVisitor(r.visitor, helpers...)
	return r
}

// SplitResourceArgument splits the argument with commas and returns unique
// strings in the original order.
//
// 去重但保留顺序，保证 "a,b,a" 与 "a,b" 行为一致且输出稳定。
func SplitResourceArgument(arg string) []string {
	out := []string{}
	set := sets.New[string]()
	for _, s := range strings.Split(arg, ",") {
		if set.Has(s) {
			continue
		}
		set.Insert(s)
		out = append(out, s)
	}
	return out
}

// HasNames returns true if the provided args contain resource names
func HasNames(args []string) (bool, error) {
	// 先做与 ResourceTypeOrNameArgs 相同的归一化，保证判定结果一致。
	args = normalizeMultipleResourcesArgs(args)
	hasCombinedTypes, err := hasCombinedTypeArgs(args)
	if err != nil {
		return false, err
	}
	return hasCombinedTypes || len(args) > 1, nil
}

// expandIfFilePattern returns all the filenames that match the input pattern
// or the filename if it is a specific filename and not a pattern.
// If the input is a pattern and it yields no result it will result in an error.
//
// 先 stat：已存在的路径不做 glob，避免把文件名里的元字符当成 pattern。
func expandIfFilePattern(pattern string) ([]string, error) {
	if _, err := os.Stat(pattern); os.IsNotExist(err) {
		matches, err := filepath.Glob(pattern)
		if err == nil && len(matches) == 0 {
			return nil, fmt.Errorf(pathNotExistError, pattern)
		}
		if err == filepath.ErrBadPattern {
			return nil, fmt.Errorf("pattern %q is not valid: %v", pattern, err)
		}
		return matches, err
	}
	return []string{pattern}, nil
}

// cachingCategoryExpanderFunc 缓存 discovery 构造的 CategoryExpander，
// 避免 ReplaceAliases 对每个参数都重建一次。
type cachingCategoryExpanderFunc struct {
	delegate CategoryExpanderFunc

	lock   sync.Mutex
	cached restmapper.CategoryExpander
}

// ToCategoryExpander 用互斥锁保护懒加载；错误不缓存，允许下次重试 discovery。
func (c *cachingCategoryExpanderFunc) ToCategoryExpander() (restmapper.CategoryExpander, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.cached != nil {
		return c.cached, nil
	}

	ret, err := c.delegate()
	if err != nil {
		return nil, err
	}
	c.cached = ret
	return c.cached, nil
}
