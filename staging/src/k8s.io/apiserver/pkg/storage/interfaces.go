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

package storage

import (
	"context"
	"fmt"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
)

// Feature 是存储层各个特性的名称，由 feature_support_checker 逐个检查其支持情况。
// 这里用类型别名而非新类型，方便直接把字符串常量作为 map 的 key 与开关使用。
type Feature = string

// RequestWatchProgress 是 etcd 的一项特性，用于进度通知请求。默认值：false。
var RequestWatchProgress Feature = "RequestWatchProgress"

// RangeStream 是 etcd 的一项特性（etcd 3.7+），对应流式 list RPC。
// 默认值：true。收到 Unimplemented 响应时 MarkUnsupported 会把它翻转为 false，
// 并在一个重检（recheck）间隔后恢复为 true。
//
// 这种“乐观启用 + 失败降级 + 定期重探”的模式避免了启动时探测带来的
// 额外延迟，同时保证与旧版 etcd 的兼容性。
var RangeStream Feature = "RangeStream"

// Versioner 抽象了「把数据库响应中的元数据字段设置/读取到对象或列表上」这一行为。
// 它是维护存储不变量所必需的——除 ResourceVersion 和 SelfLink 外用相同数据
// 连续更新同一对象两次，必须是空操作（no-op）。uint64 类型的 resourceVersion
// 是「原始（raw）」resourceVersion，直接与后端之间收发；string 类型的
// resourceVersion 是「安全（safe）」resourceVersion，供用户使用。
//
// 该不变量是 etcd 乐观并发控制的基础：resourceVersion 不参与对象内容比较，
// 否则无意义的重复写入会不断递增版本号并唤醒所有 watcher。
type Versioner interface {
	// UpdateObject 把存储元数据写入 API 对象。若对象无法被正确更新则返回错误。
	// 若请求的对象不需要来自数据库的元数据，可以返回 nil。
	UpdateObject(obj runtime.Object, resourceVersion uint64) error
	// UpdateList 把 resource version 写入 API 列表对象。若对象无法被正确更新则返回错误。
	// 若请求的对象不需要来自数据库的元数据，可以返回 nil。continueValue 是可选的，
	// 它表示：只要客户端在后续调用中把该值传给服务端，就有更多结果可取。
	// remainingItemCount 表示列表为部分结果时剩余对象的数量；
	// 若将其设为 nil，序列化时会省略 remainingItemCount 字段。
	UpdateList(obj runtime.Object, resourceVersion uint64, continueValue string, remainingItemCount *int64) error
	// PrepareObjectForStorage 应当把 SelfLink 和 ResourceVersion 置为空值。
	// 若指定对象无法被更新，应当返回错误。
	PrepareObjectForStorage(obj runtime.Object) error
	// ObjectResourceVersion 返回指定对象的 resource version（用于持久化）。
	// 若指定对象没有可持久化的版本，应当返回错误。
	ObjectResourceVersion(obj runtime.Object) (uint64, error)

	// ParseResourceVersion 接收一个 resource version 参数并将其转换为存储后端的形式。
	// 对于 watch，应当把结果传给 helper.Watch()。由于 resourceVersion 是一个不透明值，
	// 非零 resourceVersion 的默认 watch 行为是从下一个值开始观察
	// （传 "1"，你会看到从 "2" 开始的更新）。
	ParseResourceVersion(resourceVersion string) (uint64, error)
}

// ResponseMeta 包含与对象关联的数据库元数据信息。它抽象了实际底层对象，
// 以避免与具体数据库耦合，并提升可测试性。
type ResponseMeta struct {
	// TTL 是包含所返回对象的那个节点的存活时间（time to live）。某些情况下它可能
	// 为零或负值（由于服务端滞后，对象可能在所请求的过期时间之后才被清理）。
	TTL int64
	// 包含所返回对象的那个节点的 resource version。
	ResourceVersion uint64
}

// IndexerFunc 针对给定对象计算某个特定 `<index>` 的 `<value of an index>`。
// 返回单个字符串意味着一个索引键只对应一个值；多值索引需编码为单一字符串。
type IndexerFunc func(obj runtime.Object) string

// IndexerFuncs 是从 `<index name>`（索引名）到函数的映射，
// 该函数针对给定对象计算 `<value for that index>`（该索引对应的值）。
type IndexerFuncs map[string]IndexerFunc

// Everything 接受所有对象。注意：SelectionPredicate 内含接口字段，不要直接修改此包级变量。
var Everything = SelectionPredicate{
	Label: labels.Everything(),
	Field: fields.Everything(),
}

// MatchValue 定义一个二元组（`<index name>`，`<value for that index>`）。
type MatchValue struct {
	IndexName string
	Value     string
}

// 向 Interface.GuaranteedUpdate 传入一个 UpdateFunc，即可执行一次
// 保证会成功的更新。更多细节见 GuaranteedUpdate 的注释。
//
// 返回的 ttl 为 nil 表示保持原 TTL；非 nil 则覆盖。返回 error 会中止重试。
type UpdateFunc func(input runtime.Object, res ResponseMeta) (output runtime.Object, ttl *uint64, err error)

// ValidateObjectFunc 是针对给定对象执行操作（校验）的函数。若该钩子无法完成，
// 可以返回错误。此函数不得（NOT）转换所传入的对象。
// 注意：若因转换（transformation）或解码错误而无法从存储中读出对象，
// obj 可能为 nil。
type ValidateObjectFunc func(ctx context.Context, obj runtime.Object) error

// ValidateAllObjectFunc 是 ValidateObjectFunc 的「一律放行」实例。
func ValidateAllObjectFunc(ctx context.Context, obj runtime.Object) error {
	return nil
}

// Preconditions 是在执行操作（更新、删除等）之前必须满足的前置条件，
// 仅包含不可变身份字段（UID）与乐观锁字段（ResourceVersion）。
type Preconditions struct {
	// 指定目标 UID。
	// +optional
	UID *types.UID `json:"uid,omitempty"`
	// 指定目标 ResourceVersion
	// +optional
	ResourceVersion *string `json:"resourceVersion,omitempty"`
}

// NewUIDPreconditions 返回一个已设置 UID 的 Preconditions。
func NewUIDPreconditions(uid string) *Preconditions {
	u := types.UID(uid)
	return &Preconditions{UID: &u}
}

// Check 针对 obj 校验前置条件。接收者为 nil 表示「无前置条件」，
// 永远校验成功，因此调用方无需对 nil 做分支判断。
func (p *Preconditions) Check(key string, obj runtime.Object) error {
	if p == nil {
		return nil
	}
	objMeta, err := meta.Accessor(obj)
	if err != nil {
		return NewInternalError(
			fmt.Errorf("can't enforce preconditions %v on un-introspectable object %v, got error: %w",
				*p,
				obj,
				err))
	}
	if p.UID != nil && *p.UID != objMeta.GetUID() {
		err := fmt.Sprintf(
			"Precondition failed: UID in precondition: %v, UID in object meta: %v",
			*p.UID,
			objMeta.GetUID())
		return NewInvalidObjError(key, err)
	}
	if p.ResourceVersion != nil && *p.ResourceVersion != objMeta.GetResourceVersion() {
		err := fmt.Sprintf(
			"Precondition failed: ResourceVersion in precondition: %v, ResourceVersion in object meta: %v",
			*p.ResourceVersion,
			objMeta.GetResourceVersion())
		return NewInvalidObjError(key, err)
	}
	return nil
}

// Interface 为对象的序列化/反序列化操作提供统一接口，
// 并把所有存储相关操作隐藏在它背后。
//
// 所有实现都必须保证方法并发安全；ctx 的取消应尽快中止底层请求。
type Interface interface {
	// 返回与此接口关联的 Versioner。
	Versioner() Versioner

	// Create 在某个 key 上新建对象，除非该 key 已存在。'ttl' 是以秒计的
	// 存活时间（0 表示永不过期）。若未返回错误且 out 非 nil，
	// out 将被设置为从数据库读回的值。
	Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error

	// Delete 删除指定的 key，并返回该位置原本存在的值。
	// 若 key 不存在，将返回 NotFound 存储错误。
	// 若 'cachedExistingObject' 非 nil，可把它当作关于对象当前版本的建议，
	// 以免再从存储读取一次取回它。
	// 但实现必须在该建议过期时进行重试。
	Delete(
		ctx context.Context, key string, out runtime.Object, preconditions *Preconditions,
		validateDeletion ValidateObjectFunc, cachedExistingObject runtime.Object, opts DeleteOptions) error

	// Watch 开始监听指定的 key。事件会被解码为 API 对象，其中被 'p' 选中的条目
	// 发送到返回的 watch.Interface，关闭该接口由调用方负责。
	// resourceVersion 可用于指定从哪个版本开始监听，应当使用当前的 resourceVersion，
	// 而不再是 rv+1（例如重连时不丢失任何更新）。
	// 如果 resourceVersion 为 "0"，该接口会先获取给定 key 当前的对象，
	// 以一个 "ADDED" 事件发出，然后才开始 watch。
	Watch(ctx context.Context, key string, opts ListOptions) (watch.Interface, error)

	// Get 把 key 处找到的对象反序列化到 objPtr。遇到 not found 错误时，
	// 依据 'opts.ignoreNotFound' 返回所请求类型的零值对象、或者返回错误。
	// 空响应与 nil 响应节点与 not found 错误按完全相同的方式处理。
	// 返回的内容可能滞后，但保证会依据 'opts.ResourceVersionMatch'
	// 匹配 'opts.ResourceVersion'。
	Get(ctx context.Context, key string, opts GetOptions, objPtr runtime.Object) error

	// GetList 把 key 处找到的多个对象反序列化进一个 *List API 对象
	// （即满足 runtime.IsList 定义的对象）。
	// 若 'opts.Recursive' 为 false，'key' 用作精确匹配；若 'opts.Recursive'
	// 为 true，'key' 用作前缀。
	// 返回的内容可能滞后，但保证会依据 'opts.ResourceVersionMatch'
	// 匹配 'opts.ResourceVersion'。
	GetList(ctx context.Context, key string, opts ListOptions, listObj runtime.Object) error

	// GuaranteedUpdate 会反复调用 'tryUpdate()' 更新 key 'key'（其类型为 'destination'），
	// 若发生索引冲突则一直重试直到成功。这是“读-改-写”的原子替代：tryUpdate 可能被
	// 多次调用，因此必须幂等。
	// 注意：如果有其他写入者并发修改同一对象，传给 tryUpdate 的对象在多次调用之间可能
	// 发生变化，所以 tryUpdate() 在决定更新后的对象应该是什么样子时，必须以对象当时的
	// 内容为准。
	// 如果 key 不存在：ignoreNotFound=false 时返回 NotFound 存储错误，否则把 `destination`
	// 设置为其类型的零值。
	// 如果最终成功的那次 `tryUpdate` 调用返回的序列化内容与输入完全相同，则不执行任何
	// 更新，而是把 `destination` 设置为具有该内容的那个对象。
	// 如果 'cachedExistingObject' 非 nil，可把它当作关于对象当前版本的建议，以免再从
	// 存储读取一次；但实现仍需要在该建议过期时重试。
	//
	// Example:
	//
	// s := /* implementation of Interface */
	// err := s.GuaranteedUpdate(
	//     "myKey", &MyType{}, true, preconditions,
	//     func(input runtime.Object, res ResponseMeta) (runtime.Object, *uint64, error) {
	//       // Before each invocation of the user defined function, "input" is reset to
	//       // current contents for "myKey" in database.
	//       curr := input.(*MyType)  // Guaranteed to succeed.
	//
	//       // Make the modification
	//       curr.Counter++
	//
	//       // Return the modified object - return an error to stop iterating. Return
	//       // a uint64 to alter the TTL on the object, or nil to keep it the same value.
	//       return cur, nil, nil
	//    }, cachedExistingObject
	// )
	GuaranteedUpdate(
		ctx context.Context, key string, destination runtime.Object, ignoreNotFound bool,
		preconditions *Preconditions, tryUpdate UpdateFunc, cachedExistingObject runtime.Object) error

	// Stats 返回存储的统计信息。
	Stats(ctx context.Context) (Stats, error)

	// ReadinessCheck 检查存储是否已就绪、可以接受请求。
	ReadinessCheck() error

	// RequestWatchProgress 请求尽快在 watch 响应流中发送一条 watch 流的
	// progress 状态。用于在监听的对象没有任何变化时也能监控 watch 进度。
	//
	// 如果 watch 存在滞后，progress 状态可能：
	// * 指向过期的 resource version。可使用 etcd KV 请求获取线性一致（linearizable）的 resource version。
	// * 完全不被投递。建议周期性地轮询请求 progress。
	//
	// 注意：只有 context 中 grpc metadata 相匹配的 watch 会收到通知。
	// https://github.com/kubernetes/kubernetes/blob/9325a57125e8502941d1b0c7379c4bb80a678d5c/vendor/go.etcd.io/etcd/client/v3/watch.go#L1037-L1042
	//
	// TODO: 当 storage.Interface 与 etcd3.store 分离后移除。
	// Deprecated: 临时添加，用于简化向 watch cache 暴露 RequestProgress。
	RequestWatchProgress(ctx context.Context) error

	// GetCurrentResourceVersion 从 etcd 获取当前的 resource version。
	// 该方法会发起一次空的 list 请求，且只从对象元数据中读取 ResourceVersion
	GetCurrentResourceVersion(ctx context.Context) (uint64, error)

	// EnableResourceSizeEstimation 通过提供一个从存储获取 key 的函数来启用资源大小估算。
	EnableResourceSizeEstimation(KeysFunc) error

	// CompactRevision 返回最近一次观测到的已压缩（compacted）revision。
	// 未启用 ListFromCacheSnapshot 时，只会观测到本地执行的压缩。
	// 若尚未观测到任何压缩，返回 0。
	CompactRevision() int64
}

// KeysFunc 是从存储获取 key 的函数原型。
type KeysFunc func(context.Context) ([]string, error)

// GetOptions 提供存储 get 操作可以传入的选项。
type GetOptions struct {
	// IgnoreNotFound 决定所请求的对象未找到时返回什么：为 true 时返回零值对象，
	// 为 false 时返回错误。
	IgnoreNotFound bool
	// ResourceVersion 为 get 操作施加一个「不早于（not older than）」的
	// resource version 约束：结果包含的数据至少与所给的 ResourceVersion 一样新。
	// 优先使用最新可用数据，但任何不早于该 ResourceVersion 的数据都可以被返回。
	ResourceVersion string
}

// ListOptions 提供存储 list 操作可以传入的选项。
type ListOptions struct {
	// ResourceVersion 为 list 操作施加一个「不早于（not older than）」的
	// resource version 约束：结果包含的数据至少与所给的 ResourceVersion 一样新。
	// 优先使用最新可用数据，但任何不早于该 ResourceVersion 的数据都可以被返回。
	ResourceVersion string
	// ResourceVersionMatch 规定 resource version 约束的适用规则。若设为默认值 ""，
	// 则沿用旧版（legacy）的 resource version 语义。
	ResourceVersionMatch metav1.ResourceVersionMatch
	// Predicate 提供 list 操作的筛选规则。
	Predicate SelectionPredicate
	// Recursive 决定 list 或 watch 的作用对象：是给定 key 处的单个对象，
	// 还是以给定 key 为前缀的全部对象集合。
	Recursive bool
	// ProgressNotify 决定来自存储层的 bookmark（progress notify）事件是否应投递给用户。
	// 非 watch 请求会忽略该选项。
	//
	// 首先，注意此字段与 Predicate.AllowWatchBookmarks 字段不同。
	// 其次，此字段仅供内部客户端（例如 watch cache）使用。
	//
	// 这意味着外部客户端无法直接设置该字段——例如无法通过设置
	// allowWatchBookmarks 查询参数来开启。
	//
	// 这样做的动机在于：像 etcd 这样的存储产生 bookmark 事件的频率
	// 可能非常高。随着 watch 请求数量增长，服务器负载也会随之上升。
	//
	// 此外，服务器完全没有义务提供 bookmark 事件，
	// 详见 https://github.com/kubernetes/enhancements/tree/master/keps/sig-api-machinery/956-watch-bookmark#proposal
	ProgressNotify bool
	// SendInitialEvents 与 Watch 选项一起设置时，会以合成的初始事件开始 watch 流，
	// 构建出所有资源的完整状态，随后跟一个携带 ResourceVersion 的合成 "Bookmark"
	// 事件，之后服务器继续流式发送事件。
	SendInitialEvents *bool
	// RecordTimestamps 要求存储层把每个发出的 watch 对象包装进 WatchEventWithRecordTime，
	// 携带其解码时间戳，用于分发延迟遥测（telemetry）。该选项仅供内部客户端
	// （watch cache）使用，外部客户端无法设置。watch cache 在存储或序列化对象之前
	// 会剥离这层包装，因此它永远不会到达其他 watcher。
	RecordTimestamps bool
}

// WatchEventWithRecordTime 包装一个 runtime.Object，并携带存储层解码对应 watch 事件
// 时的时间戳。该类型只在进程内部流转，绝不能泄漏给客户端：仅当 ListOptions.RecordTimestamps
// 被置位时才由存储层产生，并在底层对象被存储或序列化之前由 watch cache 消费并剥离，
// 绝不会发送给 watch 客户端。
type WatchEventWithRecordTime interface {
	runtime.Object
	// RecordTime 返回被包装事件的解码时间戳。
	RecordTime() time.Time
	// Unwrap 返回底层的原始对象。
	Unwrap() runtime.Object
}

// DeleteOptions 提供存储 delete 操作可以传入的选项。
type DeleteOptions struct {
	// ExpectTransformOrDecodeError 若启用，则当对象能够被正常转换和解码时返回错误。
	// 注意：正常删除流程下它应当始终为 false；只有为了不安全地删除损坏对象
	// （这类对象用正常流程无法删除）时，调用方才可以启用它。
	ExpectTransformOrDecodeError bool
}

// ValidateListOptions 校验 ListOptions，并计算出读取时使用的 etcd revision，
// 以及分页请求的 continue key。
//
// withRev 为 0 表示“读取最新版本”；continue 令牌与 resourceVersion 互斥，
// 因为令牌本身已经固定了遍历起点。
func ValidateListOptions(keyPrefix string, versioner Versioner, opts ListOptions) (withRev int64, continueKey string, err error) {
	if opts.Recursive && len(opts.Predicate.Continue) > 0 {
		continueKey, continueRV, err := DecodeContinue(opts.Predicate.Continue, keyPrefix)
		if err != nil {
			return 0, "", apierrors.NewBadRequest(fmt.Sprintf("invalid continue token: %v", err))
		}
		if len(opts.ResourceVersion) > 0 && opts.ResourceVersion != "0" {
			return 0, "", apierrors.NewBadRequest("specifying resource version is not allowed when using continue")
		}
		// 若 continueRV > 0，该 LIST 请求需要一个特定的 resource version。
		// continueRV==0 是非法的。
		// 若 continueRV < 0，则该请求针对最新的 resource version。
		if continueRV > 0 {
			withRev = continueRV
		}
		return withRev, continueKey, nil
	}
	if len(opts.ResourceVersion) == 0 {
		return withRev, "", nil
	}
	parsedRV, err := versioner.ParseResourceVersion(opts.ResourceVersion)
	if err != nil {
		return withRev, "", apierrors.NewBadRequest(fmt.Sprintf("invalid resource version: %v", err))
	}
	switch opts.ResourceVersionMatch {
	case metav1.ResourceVersionMatchNotOlderThan:
		// 「不早于」约束在拿到 etcd 响应之后才校验，
		// 届时 withRev 会被设置为 etcd 响应返回的 revision。
	case metav1.ResourceVersionMatchExact:
		withRev = int64(parsedRV)
	case "": // 旧版（legacy）情形
		if opts.Recursive && opts.Predicate.Limit > 0 && parsedRV > 0 {
			withRev = int64(parsedRV)
		}
	default:
		return withRev, "", fmt.Errorf("unknown ResourceVersionMatch value: %v", opts.ResourceVersionMatch)
	}
	return withRev, "", nil
}

// Stats 提供关于存储的统计信息。
type Stats struct {
	// ObjectCount 表示存储中保存的对象数量。
	ObjectCount int64
	// EstimatedAverageObjectSizeBytes 基于序列化后的大小，表示存储中对象的大小。
	// 该值是估算值，不要求准确，也不要求一致。
	EstimatedAverageObjectSizeBytes int64
}

// PrepareKey 在使用前校验并规范化存储 key。
//
// 拒绝 "." 与 ".." 路径段是安全要求：key 来自用户可控的资源名/命名空间，
// 若不拦截可越过前缀边界访问其他资源的数据。
func PrepareKey(resourcePrefix, key string, recursive bool) (string, error) {
	if key == ".." ||
		strings.HasPrefix(key, "../") ||
		strings.HasSuffix(key, "/..") ||
		strings.Contains(key, "/../") {
		return "", fmt.Errorf("invalid key: %q", key)
	}
	if key == "." ||
		strings.HasPrefix(key, "./") ||
		strings.HasSuffix(key, "/.") ||
		strings.Contains(key, "/./") {
		return "", fmt.Errorf("invalid key: %q", key)
	}
	if key == "" || key == "/" {
		return "", fmt.Errorf("empty key: %q", key)
	}
	// 对于递归请求，必须确保 key 以 "/" 结尾，这样只会取回子级「目录」。
	// 例如存在 key "/a"、"/a/b"、"/ab" 时，用前缀 "/a" 取 key 会返回全部三个，
	// 而用前缀 "/a/" 只返回 "/a/b"，后者才是正确答案。
	if recursive && !strings.HasSuffix(key, "/") {
		key += "/"
	}
	// 前缀检查必须放在规范化之后，否则 "/a/../b" 之类的输入可能绕过。
	if !strings.HasPrefix(key, resourcePrefix) {
		return "", fmt.Errorf("invalid key: %q lacks resource prefix: %q", key, resourcePrefix)
	}
	return key, nil
}
