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
	"fmt"
	"strconv"
	"strings"
	"time"

	auditinternal "k8s.io/apiserver/pkg/apis/audit"
)

// EventString 生成审计事件的单行文本表示，只使用事件结构体中的部分信息。
// 该格式用于 kube-apiserver 的 stderr 审计输出，面向人类阅读与 grep，
// 不是稳定的机器可解析格式；需要结构化数据时请使用 JSON 后端。
func EventString(ev *auditinternal.Event) string {
	// 占位符区分“字段缺失”与“字段为空”：未认证为 <none>，
	// 未模拟为 <self>，模拟但未带 groups 为 <lookup>（需另行查询）。
	username := "<none>"
	groups := "<none>"
	if len(ev.User.Username) > 0 {
		username = ev.User.Username
		if len(ev.User.Groups) > 0 {
			groups = auditStringSlice(ev.User.Groups)
		}
	}
	asuser := "<self>"
	asgroups := "<lookup>"
	if ev.ImpersonatedUser != nil {
		asuser = ev.ImpersonatedUser.Username
		if ev.ImpersonatedUser.Groups != nil {
			asgroups = auditStringSlice(ev.ImpersonatedUser.Groups)
		}
	}

	// 非资源请求或 List 请求没有 ObjectRef，命名空间记为 <none>。
	namespace := "<none>"
	if ev.ObjectRef != nil && len(ev.ObjectRef.Namespace) != 0 {
		namespace = ev.ObjectRef.Namespace
	}

	// RequestReceived 阶段响应尚未产生，记为 <deferred>。
	response := "<deferred>"
	if ev.ResponseStatus != nil {
		response = strconv.Itoa(int(ev.ResponseStatus.Code))
	}

	// SourceIPs 的最后一项才是可信的直连地址；这里取第一项（最靠近客户端），
	// 与该格式的历史输出保持一致。
	ip := "<unknown>"
	if len(ev.SourceIPs) > 0 {
		ip = ev.SourceIPs[0]
	}

	return fmt.Sprintf("%s AUDIT: id=%q stage=%q ip=%q method=%q user=%q groups=%q as=%q asgroups=%q user-agent=%q namespace=%q uri=%q response=\"%s\"",
		ev.RequestReceivedTimestamp.Format(time.RFC3339Nano), ev.AuditID, ev.Stage, ip, ev.Verb, username, groups, asuser, asgroups, ev.UserAgent, namespace, ev.RequestURI, response)
}

// auditStringSlice 把字符串切片格式化为带引号、逗号分隔的单个字符串，
// 避免含空格的 group 名破坏单行格式的可读性。
func auditStringSlice(inList []string) string {
	quotedElements := make([]string, len(inList))
	for i, in := range inList {
		quotedElements[i] = fmt.Sprintf("%q", in)
	}
	return strings.Join(quotedElements, ",")
}
