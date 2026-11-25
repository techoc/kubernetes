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

package request

import (
	"context"
	"time"
)

// requestReceivedTimestampKeyType 是私有类型，防止其他包用裸 int 伪造 context key。
type requestReceivedTimestampKeyType int

// requestReceivedTimestampKey 是 ReceivedTimestamp（请求到达 apiserver 的时刻）在 context 中的 key。
const requestReceivedTimestampKey requestReceivedTimestampKeyType = iota

// WithReceivedTimestamp 返回 parent 的副本，其中已设置 ReceivedTimestamp（请求到达 apiserver 的时刻）。
// 若传入的 ReceivedTimestamp 为零值，则不写入任何值，原样返回 parent。
// 零值不写入，是为了让调用方可以无条件调用本函数，而不必先判断时间是否有效。
func WithReceivedTimestamp(parent context.Context, receivedTimestamp time.Time) context.Context {
	if receivedTimestamp.IsZero() {
		return parent
	}
	return WithValue(parent, requestReceivedTimestampKey, receivedTimestamp)
}

// ReceivedTimestampFrom 从指定 context 中读取 ReceivedTimestamp key 的值。
// 第二个返回值区分“未设置”与“设置为零值”；调用方据此决定是否回退到当前时间。
func ReceivedTimestampFrom(ctx context.Context) (time.Time, bool) {
	info, ok := ctx.Value(requestReceivedTimestampKey).(time.Time)
	return info, ok
}
