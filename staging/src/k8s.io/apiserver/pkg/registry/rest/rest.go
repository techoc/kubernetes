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

package rest

import (
	"context"
	"io"
	"net/http"
	"net/url"

	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

//TODO:
// 存储接口需要拆分为两组：作用于集合（collection）的接口和作用于
// 单个具名对象（individually named items）的接口。
// 集合接口：
// （方法：当前 -> 提议）
//    GET: Lister -> CollectionGetter
//    WATCH: Watcher -> CollectionWatcher
//    CREATE: Creater -> CollectionCreater
//    DELETE: (n/a) -> CollectionDeleter
//    UPDATE: (n/a) -> CollectionUpdater
//
// 单个对象接口：
// （方法：当前 -> 提议）
//    GET: Getter -> NamedGetter
//    WATCH: (n/a) -> NamedWatcher
//    CREATE: (n/a) -> NamedCreater
//    DELETE: Deleter -> NamedDeleter
//    UPDATE: Update -> NamedUpdater

// Storage 是 RESTful 存储服务通用的接口。
// 凡是对外暴露在 apiserver RESTful API 中的资源都必须实现该接口。对象
// 可以按需实现下面列出的任意接口。
//
// 可以的话，请考虑使用 StorageWithReadiness。
//
// 能力通过组合小接口表达：调用方对具体资源只依赖它实际需要的能力，
// 例如只读端点只需 Lister，避免为未实现的动词提供假实现。
type Storage interface {
	// New 返回一个空对象，在把请求数据填入之后可用于 Create 和 Update。
	// 该对象必须是指针类型，以便配合 Codec.DecodeInto([]byte, runtime.Object) 使用。
	New() runtime.Object

	// Destroy 在关闭时清理自身占用的资源。
	// Destroy 必须以线程安全的方式实现，并且要能接受
	// 被多次调用的情况。
	Destroy()
}

// StorageWithReadiness 在 Storage 接口之上扩展了就绪检查。
// 用于需要等待底层存储（如 etcd 连接、缓存同步）就绪的资源。
type StorageWithReadiness interface {
	Storage

	// ReadinessCheck 用于检查存储是否就绪。
	ReadinessCheck() error
}

// Scoper 指明资源所处的作用域（scope），必须给出。
// 通常它会依据你的 strategy 自动提供。
type Scoper interface {
	// NamespaceScoped 在存储属于命名空间级作用域时返回 true
	NamespaceScoped() bool
}

// KindProvider 为其 API 指定一个与内部存储不同的 kind。这对于未编译进
// apiserver 的外部对象是必要的：这类对象没有内存中的表示形式，
// 因此必须以通用对象（例如 runtime.Unknown）表示；但当我们把该对象作为
// API 发现的一部分展示时，希望呈现的是具体的 kind，而不是通用的内部表示。
type KindProvider interface {
	Kind() string
}

// ShortNamesProvider 是 RESTful 存储服务的接口之一，用于提供某个资源的一组简称。
// kubectl 会利用这份列表为资源提供简称形式。
type ShortNamesProvider interface {
	ShortNames() []string
}

// CategoriesProvider 允许资源声明自己属于哪些资源分组（categories）。API 客户端
// 可以用 categories 以一个名字指代一批资源（例如 "all" 可以展开为 "pod,rc,svc,..."）。
type CategoriesProvider interface {
	Categories() []string
}

// SingularNameProvider 返回资源的单数名称。kubectl 的发现（discovery）机制用它
// 获得资源的单数形式。若与 CRD 的简称（shortcut）发生冲突，单数名称应当始终指向本资源。
type SingularNameProvider interface {
	GetSingularName() string
}

// GroupVersionKindProvider 用于向发现机制指定一个具体的 GroupVersionKind。这对多态（polymorphic）
// 端点很有用，它们通常指向其他版本，例如 Scale 指向的是 Scale.v1beta1.extensions。
// 由于它能提供所需的全部信息，因此优先级高于 KindProvider。
// TODO 仅供 federation 使用的 KindProvider 应当移除并由本接口替代，但在 1.8 后期这样做风险过大。
type GroupVersionKindProvider interface {
	GroupVersionKind(containingGV schema.GroupVersion) schema.GroupVersionKind
}

// GroupVersionAcceptor 用于判断某个 GroupVersion 是否可以发送给某个端点。
// 这适用于接受多个版本的端点（这种情况极为罕见）。
// 已知唯一的例子是 pods/evictions：它接受 policy/v1，但为向后兼容也接受 policy/v1beta1。
type GroupVersionAcceptor interface {
	AcceptsGroupVersion(gv schema.GroupVersion) bool
}

// Lister 是可以检索符合所给字段（field）与标签（label）条件的资源的对象。
type Lister interface {
	// NewList 返回一个空对象，可用于 List 调用。
	// 该对象必须是指针类型，以便配合 Codec.DecodeInto([]byte, runtime.Object) 使用。
	NewList() runtime.Object
	// List 选出存储中匹配 selector 的资源。'options' 可以为 nil。
	List(ctx context.Context, options *metainternalversion.ListOptions) (runtime.Object, error)
	// TableConvertor 要求所有 List 的实现者同时实现表格转换。
	TableConvertor
}

// Getter 是可以检索具名 RESTful 资源的对象。
type Getter interface {
	// Get 在存储中按名称查找资源并返回它。
	// 虽然它可以返回任意错误值，但当指定资源不存在时，
	// 返回的错误 err 一定满足 IsNotFound(err)。
	Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error)
}

// GetterWithOptions 是可以检索具名 RESTful 资源的对象，并在 get 请求上接受
// 额外的 options。它还允许调用方同时获得 GET 请求的 subpath。
type GetterWithOptions interface {
	// Get 在存储中按名称查找资源并返回它。
	// 虽然它可以返回任意错误值，但当指定资源不存在时，
	// 返回的错误 err 一定满足 IsNotFound(err)。
	// 传入的 options 对象与 NewGetOptions 方法
	// 返回的类型相同。
	// TODO: 改为传入 metav1.GetOptions。
	Get(ctx context.Context, name string, options runtime.Object) (runtime.Object, error)

	// NewGetOptions 返回一个空的 options 对象，用于向 Get 方法
	// 传递选项。它还可以返回一个 bool 和一个 string：若该 bool 为 true，
	// 对象之下那段请求路径的值会以该 string 作为名字，被纳入运行时对象的
	// 序列化结果中。例如返回 "path" 会让传给 converter 的
	// map[string][]string 中，请求尾部的 scheme 值变成以 "path" 为键。
	NewGetOptions() (runtime.Object, bool, string)
}

// TableConvertor 把对象转换为 metav1.Table；列表端点必须实现，
// 以支持 kubectl get 的服务端表格渲染。
type TableConvertor interface {
	ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error)
}

// GracefulDeleter 知道如何传递删除选项，以允许对一个
// RESTful 对象执行延迟删除。
type GracefulDeleter interface {
	// Delete 在存储中查找资源并将其删除。
	// 删除请求会先由 deleteValidation 校验。
	// 如果调用方提供了 options，资源会尽量按照它们执行，否则
	// 返回非法请求错误。
	// 虽然它可以返回任意错误值，但当指定资源不存在时，
	// 返回的错误 err 一定满足 IsNotFound(err)。
	// Delete *可以*返回被删除的对象，或者返回一个描述删除
	// 附加信息的 status 对象。
	// 它还会返回一个布尔值：资源被立即删除时为 true，
	// 将被异步删除时为 false。
	Delete(ctx context.Context, name string, deleteValidation ValidateObjectFunc, options *metav1.DeleteOptions) (runtime.Object, bool, error)
}

// MayReturnFullObjectDeleter 在删除时可以返回被删除的完整对象（而不是一个简单的 status 对象）。
type MayReturnFullObjectDeleter interface {
	DeleteReturnsDeletedObject() bool
}

// CollectionDeleter 是可以删除一批
// RESTful 资源的对象。
type CollectionDeleter interface {
	// DeleteCollection 选出存储中匹配给定 'listOptions' 的所有资源
	// 并将它们删除。删除请求会先由 deleteValidation 校验。
	// 如果调用方提供了 'options'，资源会尽量按照它们执行，否则返回
	// 非法请求错误。
	// DeleteCollection 不保证是原子操作 —— 也就是说它可能已经删除了部分对象
	// 仍然返回错误。成功时，返回被删除对象组成的列表。
	DeleteCollection(ctx context.Context, deleteValidation ValidateObjectFunc, options *metav1.DeleteOptions, listOptions *metainternalversion.ListOptions) (runtime.Object, error)
}

// Creater 是可以创建 RESTful 对象实例的对象。
type Creater interface {
	// New 返回一个空对象，在把请求数据填入之后可用于 Create。
	// 该对象必须是指针类型，以便配合 Codec.DecodeInto([]byte, runtime.Object) 使用。
	New() runtime.Object

	// Create 创建资源的一个新版本。
	Create(ctx context.Context, obj runtime.Object, createValidation ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error)
}

// NamedCreater 是可以借助 name 参数创建 RESTful 对象实例的对象。
type NamedCreater interface {
	// New 返回一个空对象，在把请求数据填入之后可用于 Create。
	// 该对象必须是指针类型，以便配合 Codec.DecodeInto([]byte, runtime.Object) 使用。
	New() runtime.Object

	// Create 创建资源的一个新版本，并要求从路径中提供 name 参数。
	// 子资源（subresource）的创建操作需要在路径中包含父资源的名称，
	// 因此需要该参数。
	Create(ctx context.Context, name string, obj runtime.Object, createValidation ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error)
}

// SubresourceObjectMetaPreserver 为子资源的 Creater 增加配置选项。
type SubresourceObjectMetaPreserver interface {
	// PreserveRequestObjectMetaSystemFieldsOnSubresourceCreate 表示
	// handler 应当保留 ObjectMeta 中由系统管理的字段。
	PreserveRequestObjectMetaSystemFieldsOnSubresourceCreate() bool
}

// UpdatedObjectInfo 向 Updater 提供关于被更新对象的信息。
// 它需要能访问旧对象，才能返回更新后的新对象。
type UpdatedObjectInfo interface {
	// 返回根据更新后的对象构造出的 preconditions（如果适用）。
	// 如果无法从更新后的对象中确定 preconditions，
	// 可以返回 nil，或返回字段均为 nil 的 preconditions 对象。
	Preconditions() *metav1.Preconditions

	// UpdatedObject 在给定 context 和旧对象的情况下返回更新后的对象。
	// 唯一应当传入空 oldObj 的时机，是发生“更新即创建”（create on update）时（此时不存在 oldObj）。
	UpdatedObject(ctx context.Context, oldObj runtime.Object) (newObj runtime.Object, err error)
}

// ValidateObjectFunc 是对给定对象执行操作（校验）的函数。若该钩子无法完成，
// 可以返回错误。ValidateObjectFunc 不得改动（transform）传入的
// 对象。
type ValidateObjectFunc func(ctx context.Context, obj runtime.Object) error

// ValidateAllObjectFunc 是 ValidateObjectFunc 的一个“全部放行”实现。
func ValidateAllObjectFunc(ctx context.Context, obj runtime.Object) error {
	return nil
}

// ValidateObjectUpdateFunc 是对给定对象及其前序版本执行操作（校验）的函数。
// 若该钩子无法完成，可以返回错误。UpdateObjectFunc
// 不得改动（transform）传入的对象。
type ValidateObjectUpdateFunc func(ctx context.Context, obj, old runtime.Object) error

// ValidateAllObjectUpdateFunc 是 ValidateObjectUpdateFunc 的一个“全部放行”实现。
func ValidateAllObjectUpdateFunc(ctx context.Context, obj, old runtime.Object) error {
	return nil
}

// Updater 是可以更新 RESTful 对象实例的对象。
type Updater interface {
	// New 返回一个空对象，在把请求数据填入之后可用于 Update。
	// 该对象必须是指针类型，以便配合 Codec.DecodeInto([]byte, runtime.Object) 使用。
	New() runtime.Object

	// Update 在存储中查找资源并更新它。某些实现
	// 可能允许更新操作顺带创建对象 —— 这类实现应当把 created
	// 布尔值置为 true。
	Update(ctx context.Context, name string, objInfo UpdatedObjectInfo, createValidation ValidateObjectFunc, updateValidation ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error)
}

// CreaterUpdater 是必须同时支持创建和更新的存储对象。
// Go 不允许嵌入实现了同名方法的多个接口。
type CreaterUpdater interface {
	Creater
	Update(ctx context.Context, name string, objInfo UpdatedObjectInfo, createValidation ValidateObjectFunc, updateValidation ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error)
}

// CreaterUpdater 必须满足 Updater 接口。
var _ Updater = CreaterUpdater(nil)

// Patcher 是同时支持 get 和 update 的存储对象。
type Patcher interface {
	Getter
	Updater
}

// Watcher 应当由所有希望
// 通过 watch api 提供变更监听能力的 Storage 对象实现。
type Watcher interface {
	// 'label' 按标签筛选；'field' 按对象字段筛选。并非所有字段
	// 都受支持；如果 'field' 试图筛选一个不受支持的字段，应当返回错误。
	// 'resourceVersion' 允许从某个特定版本开始或继续监听。
	Watch(ctx context.Context, options *metainternalversion.ListOptions) (watch.Interface, error)
}

// StandardStorage 是覆盖常见动词的接口，用于测试某个资源
// 是否具备常规存储方法。传递不透明的存储对象时请使用 Storage。
type StandardStorage interface {
	Getter
	Lister
	CreaterUpdater
	GracefulDeleter
	CollectionDeleter
	Watcher

	// Destroy 在关闭时清理自身占用的资源。
	// Destroy 必须以线程安全的方式实现，并且要能接受
	// 被多次调用的情况。
	Destroy()
}

// Redirector 知道如何返回远端资源的位置。
// 用于代理/重定向类资源（如 nodes/proxy），由存储层决定实际远端地址。
type Redirector interface {
	// ResourceLocation 应当返回给定资源的远端位置、请求它时可用的可选 transport，或者一个错误。
	ResourceLocation(ctx context.Context, id string) (remoteLocation *url.URL, transport http.RoundTripper, err error)
}

// Responder 抽象了 REST 方法的常规响应行为，并传给这样的调用方：
// 它们可能希望在某些情况下直接处理响应，而在其他情况下
// 委托给常规的错误或对象响应行为。
type Responder interface {
	// Object 把给定对象写入响应。多次调用该方法的行为是未定义的。
	Object(statusCode int, obj runtime.Object)
	// Error 把给定错误写入响应。该方法只能被调用一次。
	Error(err error)
}

// Connecter 是可以响应连接（connection）请求的存储对象。
type Connecter interface {
	// Connect 返回一个 http.Handler，它会处理某次 API 调用的请求/响应。
	// 传入的 responder 可用于常见的 API 响应。responder 会同时写入状态码
	// 和响应体，因此在调用完 responder 之后 ServeHTTP 方法就应当返回。该 Handler
	// 只用于单次 API 请求，随后即被丢弃。Responder 保证写入
	// 传给 ServeHTTP 的同一个 http.ResponseWriter。
	Connect(ctx context.Context, id string, options runtime.Object, r Responder) (http.Handler, error)

	// NewConnectOptions 返回一个空的 options 对象，用于向 Connect
	// 方法传递选项。若返回 nil，则传给 Connect 的 options
	// 对象为 nil。它还可以返回一个 bool 和一个 string：若该 bool 为 true，
	// 对象之下那段请求路径的值会以该 string 作为名字，被纳入
	// 运行时对象的序列化结果中。
	NewConnectOptions() (runtime.Object, bool, string)

	// ConnectMethods 返回 Connect 所处理的 HTTP 方法列表
	ConnectMethods() []string
}

// ResourceStreamer 是“希望以流式方式从服务端传输、而不是直接解码”的对象所实现的接口。
type ResourceStreamer interface {
	// InputStream 在给定对象支持流式传输时应当返回一个 io.ReadCloser。调用时会传入
	// 期望的 api 版本和一个 accept 头（可以为空）。若没有发生错误，
	// 调用方可以返回一个标志，指明结果是否应当在写入发生时立即刷新（flush），
	// 以及一个表明流类型的 content type 字符串。
	// 如果返回的流为 null，则会生成一个 StatusNoContent 响应。
	InputStream(ctx context.Context, apiVersion, acceptHeader string) (stream io.ReadCloser, flush bool, mimeType string, err error)
}

// StorageMetadata 是一个可选接口，调用方可以实现它，以便提供关于其
// Storage 对象的额外信息。
type StorageMetadata interface {
	// ProducesMIMETypes 返回指定 HTTP 动词（GET、POST、DELETE、
	// PATCH）能够响应的 MIME 类型列表。
	ProducesMIMETypes(verb string) []string

	// ProducesObject 返回指定 HTTP 动词所响应的对象。若它非 nil，则会覆盖存储对象。
	// 只有返回对象的类型有意义，其值会被忽略。
	ProducesObject(verb string) interface{}
}

// StorageVersionProvider 是存储对象若希望披露自身存储版本
// 便可以实现的一个可选接口。
type StorageVersionProvider interface {
	// StorageVersion 返回一个 group versioner：在给定对象可能属于的一组 kinds 时，
	// 它会输出该对象在持久化到 etcd 之前会被转换成的 GVK。
	StorageVersion() runtime.GroupVersioner
}

// ResetFieldsStrategy 是一个可选接口，存储对象若希望提供其各策略所重置的字段，
// 便可以实现它。
// 字段重置用于在更新时清除由系统管理的字段，避免客户端持久化这些值。
type ResetFieldsStrategy interface {
	GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set
}

// ResetFieldsFilterStrategy 是存储对象若希望由其策略提供可重置的
// fields filter，便可以实现的一个可选接口。
type ResetFieldsFilterStrategy interface {
	GetResetFieldsFilter() map[fieldpath.APIVersion]fieldpath.Filter
}

// CreateUpdateResetFieldsStrategy 是 RESTCreateUpdateStrategy
// 与 ResetFieldsStrategy 的并集。
type CreateUpdateResetFieldsStrategy interface {
	RESTCreateUpdateStrategy
	ResetFieldsStrategy
}

// UpdateResetFieldsStrategy 是 RESTUpdateStrategy
// 与 ResetFieldsStrategy 的并集。
type UpdateResetFieldsStrategy interface {
	RESTUpdateStrategy
	ResetFieldsStrategy
}

// CorruptObjectDeleterProvider 是存储对象为支持不安全地删除损坏对象
// 而实现的接口。它返回一个
// GracefulDeleter，用于执行损坏对象的不安全删除。
type CorruptObjectDeleterProvider interface {
	GetCorruptObjDeleter() GracefulDeleter
}
