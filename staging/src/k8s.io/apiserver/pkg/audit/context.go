/*
Copyright 2020 The Kubernetes Authors.

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

package audit

import (
	"context"
	"errors"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	authnv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/apiserver/pkg/authentication/user"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/klog/v2"
)

// The key type is unexported to prevent collisions
type key int

// auditKey is the context key for storing the audit context that is being
// captured and the evaluated policy that applies to the given request.
const auditKey key = iota

// AuditContext 持有构造当前请求审计事件所需的信息。
// 并发模型是本类型的核心——initialized/sink/requestAuditConfig 只写一次，
// 由 atomic.Bool 保证无锁读取安全；event 则被多个过滤器并发读写，统一由 lock 保护。
// 任何新增字段都要明确归入其中一类。
type AuditContext struct {
	// initialized indicates whether requestAuditConfig and sink have been populated and are safe to read unguarded.
	// This should only be set via Init().
	initialized atomic.Bool
	// requestAuditConfig is the audit configuration that applies to the request.
	// This should only be written via Init(RequestAuditConfig, Sink), and only read when initialized.Load() is true.
	requestAuditConfig RequestAuditConfig
	// sink is the sink to use when processing event stages.
	// This should only be written via Init(RequestAuditConfig, Sink), and only read when initialized.Load() is true.
	sink Sink

	// lock guards event
	lock sync.Mutex

	// event is the audit Event object that is being captured to be written in
	// the API audit log.
	event auditinternal.Event

	// unguarded copy of auditID from the event
	auditID atomic.Value
}

// Enabled 检查该审计上下文的审计是否启用。
// 未初始化时返回 true，是为了让认证等早期阶段仍能收集注解；
// 真正的 Level 判断要等策略求值后（Init）才能确定。
func (ac *AuditContext) Enabled() bool {
	if ac == nil {
		// protect against nil pointers
		return false
	}
	if !ac.initialized.Load() {
		// Note: An unset Level should be considered Enabled, so that request data (e.g. annotations)
		// can still be captured before the audit policy is evaluated.
		return true
	}
	return ac.requestAuditConfig.Level != auditinternal.LevelNone
}

// Init 绑定本次请求的策略结果与 sink。
// 只允许初始化一次——第二次说明装配逻辑有误，返回错误而不是覆盖，
// 否则已产生的注解可能被悄悄丢弃。
func (ac *AuditContext) Init(requestAuditConfig RequestAuditConfig, sink Sink) error {
	ac.lock.Lock()
	defer ac.lock.Unlock()
	if ac.initialized.Load() {
		return errors.New("audit context was already initialized")
	}
	ac.requestAuditConfig = requestAuditConfig
	ac.sink = sink
	ac.event.Level = requestAuditConfig.Level
	ac.initialized.Store(true)
	return nil
}

func (ac *AuditContext) AuditID() types.UID {
	// return the unguarded copy of the auditID
	id, _ := ac.auditID.Load().(types.UID)
	return id
}

func (ac *AuditContext) visitEvent(f func(event *auditinternal.Event)) {
	ac.lock.Lock()
	defer ac.lock.Unlock()
	f(&ac.event)
}

// ProcessEventStage 处理指定阶段的审计事件，成功返回 true，处理出错返回 false。
// 未初始化/无 sink/命中 OmitStages 都视为成功跳过；StageRequestReceived
// 的时间戳复用请求到达时间，保证与 RequestReceivedTimestamp 一致。
func (ac *AuditContext) ProcessEventStage(ctx context.Context, stage auditinternal.Stage) bool {
	if ac == nil || !ac.initialized.Load() {
		return true
	}
	if ac.sink == nil {
		return true
	}
	for _, omitStage := range ac.requestAuditConfig.OmitStages {
		if stage == omitStage {
			return true
		}
	}

	processed := false
	ac.visitEvent(func(event *auditinternal.Event) {
		event.Stage = stage
		if stage == auditinternal.StageRequestReceived {
			event.StageTimestamp = event.RequestReceivedTimestamp
		} else {
			event.StageTimestamp = metav1.NewMicroTime(time.Now())
		}

		ObserveEvent(ctx)
		processed = ac.sink.ProcessEvents(event)
	})
	return processed
}

func (ac *AuditContext) LogImpersonatedUser(user user.Info, constraint string) {
	ac.visitEvent(func(ev *auditinternal.Event) {
		if ev == nil || ev.Level.Less(auditinternal.LevelMetadata) {
			return
		}
		ev.ImpersonatedUser = &authnv1.UserInfo{
			Username: user.GetName(),
		}
		ev.ImpersonatedUser.Groups = user.GetGroups()
		ev.ImpersonatedUser.UID = user.GetUID()
		ev.ImpersonatedUser.Extra = map[string]authnv1.ExtraValue{}
		for k, v := range user.GetExtra() {
			ev.ImpersonatedUser.Extra[k] = authnv1.ExtraValue(v)
		}
		if len(constraint) > 0 {
			if ev.AuthenticationMetadata == nil {
				ev.AuthenticationMetadata = &auditinternal.AuthenticationMetadata{}
			}
			ev.AuthenticationMetadata.ImpersonationConstraint = constraint
		}
	})
}

func (ac *AuditContext) LogResponseObject(status *metav1.Status, obj *runtime.Unknown) {
	ac.visitEvent(func(ae *auditinternal.Event) {
		if status != nil {
			// selectively copy the bounded fields.
			ae.ResponseStatus = &metav1.Status{
				Status:  status.Status,
				Message: status.Message,
				Reason:  status.Reason,
				Details: status.Details,
				Code:    status.Code,
			}
		}
		if ae.Level.Less(auditinternal.LevelRequestResponse) {
			return
		}
		ae.ResponseObject = obj
	})
}

// LogRequestPatch fills in the given patch as the request object into an audit event.
func (ac *AuditContext) LogRequestPatch(patch []byte) {
	ac.visitEvent(func(ae *auditinternal.Event) {
		ae.RequestObject = &runtime.Unknown{
			Raw:         patch,
			ContentType: runtime.ContentTypeJSON,
		}
	})
}

// GetEventUser returns a copy of the User associated with the audit Event.
func (ac *AuditContext) GetEventUser() authnv1.UserInfo {
	var val *authnv1.UserInfo
	ac.visitEvent(func(ev *auditinternal.Event) {
		val = ev.User.DeepCopy()
	})
	return *val
}

// GetEventImpersonatedUser returns a copy of the ImpersonatedUser associated with the audit Event,
// or returns nil when there is no ImpersonatedUser.
func (ac *AuditContext) GetEventImpersonatedUser() *authnv1.UserInfo {
	var val *authnv1.UserInfo
	ac.visitEvent(func(ev *auditinternal.Event) {
		val = ev.ImpersonatedUser.DeepCopy()
	})
	return val
}

func (ac *AuditContext) GetEventAnnotation(key string) (string, bool) {
	var val string
	var ok bool
	ac.visitEvent(func(event *auditinternal.Event) {
		val, ok = event.Annotations[key]
	})
	return val, ok
}

func (ac *AuditContext) GetEventLevel() auditinternal.Level {
	var level auditinternal.Level
	ac.visitEvent(func(event *auditinternal.Event) {
		level = event.Level
	})
	return level
}

func (ac *AuditContext) SetEventStage(stage auditinternal.Stage) {
	ac.visitEvent(func(event *auditinternal.Event) {
		event.Stage = stage
	})
}

func (ac *AuditContext) GetEventStage() auditinternal.Stage {
	var stage auditinternal.Stage
	ac.visitEvent(func(event *auditinternal.Event) {
		stage = event.Stage
	})
	return stage
}

func (ac *AuditContext) SetEventStageTimestamp(timestamp metav1.MicroTime) {
	ac.visitEvent(func(event *auditinternal.Event) {
		event.StageTimestamp = timestamp
	})
}

func (ac *AuditContext) GetEventResponseStatus() *metav1.Status {
	var status *metav1.Status
	ac.visitEvent(func(event *auditinternal.Event) {
		status = event.ResponseStatus
	})
	return status
}

func (ac *AuditContext) GetEventRequestReceivedTimestamp() metav1.MicroTime {
	var timestamp metav1.MicroTime
	ac.visitEvent(func(event *auditinternal.Event) {
		timestamp = event.RequestReceivedTimestamp
	})
	return timestamp
}

func (ac *AuditContext) GetEventStageTimestamp() metav1.MicroTime {
	var timestamp metav1.MicroTime
	ac.visitEvent(func(event *auditinternal.Event) {
		timestamp = event.StageTimestamp
	})
	return timestamp
}

func (ac *AuditContext) SetEventResponseStatus(status *metav1.Status) {
	ac.visitEvent(func(event *auditinternal.Event) {
		event.ResponseStatus = status
	})
}

func (ac *AuditContext) SetEventResponseStatusCode(statusCode int32) {
	ac.visitEvent(func(event *auditinternal.Event) {
		if event.ResponseStatus == nil {
			event.ResponseStatus = &metav1.Status{}
		}
		event.ResponseStatus.Code = statusCode
	})
}

func (ac *AuditContext) GetEventAnnotations() map[string]string {
	var annotations map[string]string
	ac.visitEvent(func(event *auditinternal.Event) {
		annotations = maps.Clone(event.Annotations)
	})
	return annotations
}

// AddAuditAnnotation 为给定的 key、value 设置审计注解。
// 在 WithAuditAnnotations 之后的请求流程中大部分位置调用都是安全的。
// 显著的例外是：运行在 WithAudit 之前的处理器中，不得通过 defer 语句调用
// （即 ServeHTTP 之后），因为那时审计事件已经发送给了审计 sink。
// 不了解自身在整体请求流程中位置的处理器，应优先使用 AddAuditAnnotation
// 而不是 LogAnnotation，以避免注解被丢弃。
// 同名 key 只允许写一次且值必须相同；冲突时保留先到的值并告警，
// 以免后来的过滤器覆盖安全相关的审计信息。
func AddAuditAnnotation(ctx context.Context, key, value string) {
	ac := AuditContextFrom(ctx)
	if !ac.Enabled() {
		return
	}

	ac.lock.Lock()
	defer ac.lock.Unlock()

	addAuditAnnotationLocked(ac, key, value)
}

// AddAuditAnnotations 是 AddAuditAnnotation 的批量版本，调用限制参见 AddAuditAnnotation。
// keysAndValues 是要添加的键值对，数量必须为偶数。
// 奇数个参数时丢弃最后一个并记录错误，而不是整体失败，保证已配对的
// 注解仍能写入。
func AddAuditAnnotations(ctx context.Context, keysAndValues ...string) {
	ac := AuditContextFrom(ctx)
	if !ac.Enabled() {
		return
	}

	ac.lock.Lock()
	defer ac.lock.Unlock()

	if len(keysAndValues)%2 != 0 {
		klog.Errorf("Dropping mismatched audit annotation %q", keysAndValues[len(keysAndValues)-1])
	}
	for i := 0; i < len(keysAndValues); i += 2 {
		addAuditAnnotationLocked(ac, keysAndValues[i], keysAndValues[i+1])
	}
}

// AddAuditAnnotationsMap is a bulk version of AddAuditAnnotation. Refer to AddAuditAnnotation for
// restrictions on when this can be called.
func AddAuditAnnotationsMap(ctx context.Context, annotations map[string]string) {
	ac := AuditContextFrom(ctx)
	if !ac.Enabled() {
		return
	}

	ac.lock.Lock()
	defer ac.lock.Unlock()

	for k, v := range annotations {
		addAuditAnnotationLocked(ac, k, v)
	}
}

// addAuditAnnotationLocked 把审计注解记录到事件上。
// 必须持有 ac.lock；冲突告警包含 AuditID，便于在日志中定位具体请求。
func addAuditAnnotationLocked(ac *AuditContext, key, value string) {
	ae := &ac.event
	if ae.Annotations == nil {
		ae.Annotations = make(map[string]string)
	}
	if v, ok := ae.Annotations[key]; ok && v != value {
		klog.Warningf("Failed to set annotations[%q] to %q for audit:%q, it has already been set to %q", key, value, ae.AuditID, ae.Annotations[key])
		return
	}
	ae.Annotations[key] = value
}

// WithAuditContext 返回存储了 AuditContext 的新 context。
// 已存在则原样返回，避免多个过滤器各自创建 AuditContext 导致事件分叉。
func WithAuditContext(parent context.Context) context.Context {
	if AuditContextFrom(parent) != nil {
		return parent // Avoid double registering.
	}

	return genericapirequest.WithValue(parent, auditKey, &AuditContext{
		event: auditinternal.Event{
			Stage: auditinternal.StageResponseStarted,
		},
	})
}

// AuditContextFrom returns the pair of the audit configuration object
// that applies to the given request and the audit event that is going to
// be written to the API audit log.
func AuditContextFrom(ctx context.Context) *AuditContext {
	ev, _ := ctx.Value(auditKey).(*AuditContext)
	return ev
}

// WithAuditID sets the AuditID on the AuditContext. The AuditContext must already be present in the
// request context. If the specified auditID is empty, no value is set.
func WithAuditID(ctx context.Context, auditID types.UID) {
	if auditID == "" {
		return
	}
	if ac := AuditContextFrom(ctx); ac != nil {
		ac.visitEvent(func(event *auditinternal.Event) {
			ac.auditID.Store(auditID)
			event.AuditID = auditID
		})
	}
}

// AuditIDFrom returns the value of the audit ID from the request context, along with whether
// auditing is enabled.
func AuditIDFrom(ctx context.Context) (types.UID, bool) {
	if ac := AuditContextFrom(ctx); ac != nil {
		id, _ := ac.auditID.Load().(types.UID)
		return id, true
	}
	return "", false
}

// GetAuditIDTruncated 从请求 context 中获取（截断后的）审计 ID。
// 如果 Audit-ID 值的长度超过上限，则截断保留前 N（maxAuditIDLength）个字符。
// 该函数仅供日志记录使用。
// 截断是为了防止客户端通过超长 Audit-ID 头放大日志体积；
// 不要用它做审计事件关联，关联请用完整 AuditID。
func GetAuditIDTruncated(ctx context.Context) string {
	auditID, ok := AuditIDFrom(ctx)
	if !ok {
		return ""
	}

	// if the user has specified a very long audit ID then we will use the first N characters
	// Note: assuming Audit-ID header is in ASCII
	const maxAuditIDLength = 64
	if len(auditID) > maxAuditIDLength {
		auditID = auditID[:maxAuditIDLength]
	}

	return string(auditID)
}
