/*
Copyright 2018 The Kubernetes Authors.

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

package authenticator

import "context"

// Audiences is a container for the Audiences of a token.
//
// audience 是令牌的预期接收方。校验时要求令牌的 audience 与请求声明的 audience
// 有交集，否则视为令牌被用于错误的服务，从而限制令牌在集群内的横向复用。
type Audiences []string

// The key type is unexported to prevent collisions
//
// 使用未导出的 key 类型，防止其他包构造出同名的 context key 而覆盖本包的取值。
type key int

const (
	// audiencesKey is the context key for request audiences.
	audiencesKey key = iota
)

// WithAudiences returns a context that stores a request's expected audiences.
func WithAudiences(ctx context.Context, auds Audiences) context.Context {
	return context.WithValue(ctx, audiencesKey, auds)
}

// AudiencesFrom returns a request's expected audiences stored in the request context.
// 第二个返回值区分"未设置"与"设置为空"，调用方可据此决定是否回退到默认 audience。
func AudiencesFrom(ctx context.Context) (Audiences, bool) {
	auds, ok := ctx.Value(audiencesKey).(Audiences)
	return auds, ok
}

// Has checks if Audiences contains a specific audiences.
// 线性扫描足够：audience 列表通常只有个位数，避免为每次校验构造 map 的开销。
func (a Audiences) Has(taud string) bool {
	for _, aud := range a {
		if aud == taud {
			return true
		}
	}
	return false
}

// Intersect intersects Audiences with a target Audiences and returns all
// elements in both.
// 结果按 tauds 的顺序排列，保证同一输入总是产生相同顺序，便于比较与日志稳定。
func (a Audiences) Intersect(tauds Audiences) Audiences {
	selected := Audiences{}
	for _, taud := range tauds {
		if a.Has(taud) {
			selected = append(selected, taud)
		}
	}
	return selected
}
