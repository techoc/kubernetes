/*
Copyright 2017 The Kubernetes Authors.

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
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	authnv1 "k8s.io/api/authentication/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/klog/v2"
)

// 默认参数：User-Agent 过长会污染审计事件，截断到 1024 字节并加标记。
const (
	maxUserAgentLength      = 1024
	userAgentTruncateSuffix = "...TRUNCATED"
)

// LogRequestMetadata 把请求级元数据（动词、URI、来源 IP、用户、对象引用）写入审计事件。
// 该函数只处理 metadata 及以上级别；请求体是否记录由后续 LogRequestObject 决定。
func LogRequestMetadata(ctx context.Context, req *http.Request, requestReceivedTimestamp time.Time, attribs authorizer.Attributes) {
	ac := AuditContextFrom(ctx)
	if !ac.Enabled() {
		return
	}

	ac.visitEvent(func(ev *auditinternal.Event) {
		ev.RequestReceivedTimestamp = metav1.NewMicroTime(requestReceivedTimestamp)
		ev.Verb = attribs.GetVerb()
		ev.RequestURI = req.URL.RequestURI()
		ev.UserAgent = maybeTruncateUserAgent(req)

		// 保留所有源 IP（经过多层代理时可能有多个），便于追溯真实客户端。
		ips := utilnet.SourceIPs(req)
		ev.SourceIPs = make([]string, len(ips))
		for i := range ips {
			ev.SourceIPs[i] = ips[i].String()
		}

		if user := attribs.GetUser(); user != nil {
			ev.User.Username = user.GetName()
			// 显式初始化 map，保证审计输出中 extra 为 {} 而非 null。
			ev.User.Extra = map[string]authnv1.ExtraValue{}
			for k, v := range user.GetExtra() {
				ev.User.Extra[k] = authnv1.ExtraValue(v)
			}
			ev.User.Groups = user.GetGroups()
			ev.User.UID = user.GetUID()
		}

		// 只有资源请求才有对象引用；非资源请求（如 /healthz）不填充 ObjectRef。
		if attribs.IsResourceRequest() {
			ev.ObjectRef = &auditinternal.ObjectReference{
				Namespace:   attribs.GetNamespace(),
				Name:        attribs.GetName(),
				Resource:    attribs.GetResource(),
				Subresource: attribs.GetSubresource(),
				APIGroup:    attribs.GetAPIGroup(),
				APIVersion:  attribs.GetAPIVersion(),
			}
		}
	})
}

// LogImpersonatedUser 把被假冒（impersonated）用户的属性写入审计事件，
// 同时记录所命中的 impersonation 限制，便于审计"谁以谁的身份做了什么"。
func LogImpersonatedUser(ctx context.Context, user user.Info, constraint string) {
	ac := AuditContextFrom(ctx)
	if !ac.Enabled() {
		return
	}
	ac.LogImpersonatedUser(user, constraint)
}

// LogRequestObject 把请求对象写入审计事件，传入的 runtime.Object 会被转换为给定的 gv。
// 仅 metadata 级别及以上才记录；请求体本身只在 Request/RequestResponse 级别输出。
func LogRequestObject(ctx context.Context, obj runtime.Object, objGV schema.GroupVersion, gvr schema.GroupVersionResource, subresource string, s runtime.NegotiatedSerializer) {
	ac := AuditContextFrom(ctx)
	if !ac.Enabled() {
		return
	}
	if ac.GetEventLevel().Less(auditinternal.LevelMetadata) {
		return
	}

	// meta.Accessor 比 ObjectMetaAccessor 更通用；取不到 ObjectMeta 时仅跳过元数据填充，不影响请求体的记录。
	objMeta, _ := meta.Accessor(obj)
	if shouldOmitManagedFields(ac) {
		// 必须复制后再清理，绝不能修改请求对象本身：审计不能改变业务语义。
		copy, ok, err := copyWithoutManagedFields(obj)
		if err != nil {
			klog.ErrorS(err, "Error while dropping managed fields from the request", "auditID", ac.AuditID())
		}
		if ok {
			obj = copy
		}
	}

	// TODO(audit): hook into the serializer to avoid double conversion
	requestObject, err := encodeObject(obj, objGV, s)
	if err != nil {
		// TODO(audit): add error slice to audit event struct
		// 编码失败不阻断请求，只记录日志；审计缺失比拒绝请求的代价更小。
		klog.ErrorS(err, "Encoding failed of request object", "auditID", ac.AuditID(), "gvr", gvr.String(), "obj", obj)
		return
	}

	ac.visitEvent(func(ae *auditinternal.Event) {
		if ae.ObjectRef == nil {
			ae.ObjectRef = &auditinternal.ObjectReference{}
		}

		// 只填充尚未设置的字段，保留此前（例如从 URL 解析出的）更权威的值。
		if objMeta != nil {
			if len(ae.ObjectRef.Namespace) == 0 {
				ae.ObjectRef.Namespace = objMeta.GetNamespace()
			}
			if len(ae.ObjectRef.Name) == 0 {
				ae.ObjectRef.Name = objMeta.GetName()
			}
			if len(ae.ObjectRef.UID) == 0 {
				ae.ObjectRef.UID = objMeta.GetUID()
			}
			if len(ae.ObjectRef.ResourceVersion) == 0 {
				ae.ObjectRef.ResourceVersion = objMeta.GetResourceVersion()
			}
		}
		if len(ae.ObjectRef.APIVersion) == 0 {
			ae.ObjectRef.APIGroup = gvr.Group
			ae.ObjectRef.APIVersion = gvr.Version
		}
		if len(ae.ObjectRef.Resource) == 0 {
			ae.ObjectRef.Resource = gvr.Resource
		}
		if len(ae.ObjectRef.Subresource) == 0 {
			ae.ObjectRef.Subresource = subresource
		}

		// 低于 Request 级别时只保留元数据，丢弃已编码的请求体。
		if ae.Level.Less(auditinternal.LevelRequest) {
			return
		}
		ae.RequestObject = requestObject
	})
}

// LogRequestPatch fills in the given patch as the request object into an audit event.
func LogRequestPatch(ctx context.Context, patch []byte) {
	ac := AuditContextFrom(ctx)
	// patch 体量大且可能含敏感内容，只在 Request 及以上级别记录。
	if ac.GetEventLevel().Less(auditinternal.LevelRequest) {
		return
	}
	ac.LogRequestPatch(patch)
}

// LogResponseObject fills in the response object into an audit event. The passed runtime.Object
// will be converted to the given gv.
func LogResponseObject(ctx context.Context, obj runtime.Object, gv schema.GroupVersion, s runtime.NegotiatedSerializer) {
	// 响应阶段可能尚未创建审计上下文，WithAuditContext 保证后续调用有可用的上下文。
	ac := AuditContextFrom(WithAuditContext(ctx))
	status, _ := obj.(*metav1.Status)
	if ac.GetEventLevel().Less(auditinternal.LevelMetadata) {
		return
	} else if ac.GetEventLevel().Less(auditinternal.LevelRequestResponse) {
		// Metadata 级别只记录 Status（错误信息），不记录完整响应体。
		ac.LogResponseObject(status, nil)
		return
	}

	if shouldOmitManagedFields(ac) {
		copy, ok, err := copyWithoutManagedFields(obj)
		if err != nil {
			klog.ErrorS(err, "Error while dropping managed fields from the response", "auditID", ac.AuditID())
		}
		if ok {
			obj = copy
		}
	}

	// TODO(audit): hook into the serializer to avoid double conversion
	var err error
	responseObject, err := encodeObject(obj, gv, s)
	if err != nil {
		// 编码失败时仍调用 LogResponseObject，保证 Status 与事件收尾逻辑不丢失。
		klog.ErrorS(err, "Encoding failed of response object", "auditID", ac.AuditID(), "obj", obj)
	}
	ac.LogResponseObject(status, responseObject)
}

// encodeObject 用 JSON 序列化对象并包装为 runtime.Unknown。
// 固定使用 JSON：审计后端（如 webhook）按约定解析 Raw 字段，媒体类型必须稳定。
func encodeObject(obj runtime.Object, gv schema.GroupVersion, serializer runtime.NegotiatedSerializer) (*runtime.Unknown, error) {
	const mediaType = runtime.ContentTypeJSON
	info, ok := runtime.SerializerInfoForMediaType(serializer.SupportedMediaTypes(), mediaType)
	if !ok {
		return nil, fmt.Errorf("unable to locate encoder -- %q is not a supported media type", mediaType)
	}

	enc := serializer.EncoderForVersion(info.Serializer, gv)
	var buf bytes.Buffer
	if err := enc.Encode(obj, &buf); err != nil {
		return nil, fmt.Errorf("encoding failed: %v", err)
	}

	return &runtime.Unknown{
		Raw:         buf.Bytes(),
		ContentType: mediaType,
	}, nil
}

// User-Agent 过长时截断，否则直接返回；限制长度可防止恶意客户端用超长 UA 撑大审计日志。
func maybeTruncateUserAgent(req *http.Request) string {
	ua := req.UserAgent()
	if len(ua) > maxUserAgentLength {
		ua = ua[:maxUserAgentLength] + userAgentTruncateSuffix
	}

	return ua
}

// copyWithoutManagedFields 对指定对象做深拷贝，并在拷贝中丢弃 managed fields。
// 指定对象应是 meta.Object 或 "list"。
// 参数 obj 按只读处理，不会被修改。
// 移除 managed fields 出错时返回 error；拷贝成功布尔值为 true，否则为 false；
// 不支持的对象类型返回 (nil, false, nil)，调用方据此决定是否使用原对象。
func copyWithoutManagedFields(obj runtime.Object) (runtime.Object, bool, error) {
	isAccessor := true
	if _, err := meta.Accessor(obj); err != nil {
		isAccessor = false
	}
	isList := meta.IsListType(obj)
	_, isTable := obj.(*metav1.Table)
	if !isAccessor && !isList && !isTable {
		return nil, false, nil
	}

	// TODO a deep copy isn't really needed here, figure out how we can reliably
	//  use shallow copy here to omit the manageFields.
	// 先深拷贝再清理，避免污染缓存中共享的对象。
	copy := obj.DeepCopyObject()

	if isAccessor {
		if err := removeManagedFields(copy); err != nil {
			return nil, false, err
		}
	}

	// 列表需要逐个元素清理；EachListItem 在元素不可访问时返回错误。
	if isList {
		if err := meta.EachListItem(copy, removeManagedFields); err != nil {
			return nil, false, err
		}
	}

	// Table 的每一行都内嵌对象，需逐行清理。
	if isTable {
		table := copy.(*metav1.Table)
		for i := range table.Rows {
			rowObj := table.Rows[i].Object
			if err := removeManagedFields(rowObj.Object); err != nil {
				return nil, false, err
			}
		}
	}

	return copy, true, nil
}

// removeManagedFields 清除对象的 managedFields；nil 对象视为成功。
func removeManagedFields(obj runtime.Object) error {
	if obj == nil {
		return nil
	}
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return err
	}
	accessor.SetManagedFields(nil)
	return nil
}

// shouldOmitManagedFields 判断当前请求的审计配置是否要求剔除 managedFields。
func shouldOmitManagedFields(ac *AuditContext) bool {
	if ac != nil && ac.initialized.Load() && ac.requestAuditConfig.OmitManagedFields {
		return true
	}

	// 无法判定（配置尚未初始化）时返回 false，保守地保留 managedFields，维持审计中保留 manage fields 的既有行为。
	return false
}
