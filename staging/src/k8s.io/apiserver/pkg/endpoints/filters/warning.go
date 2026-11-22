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

package filters

import (
	"fmt"
	"net/http"
	"sync"
	"unicode/utf8"

	"k8s.io/apimachinery/pkg/util/net"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/warning"
)

// WithWarningRecorder attaches a deduplicating k8s.io/apiserver/pkg/warning#WarningRecorder to the request context.
//
// 中文说明：Warning 头是逐请求的，且同一消息重复出现只会让客户端日志变噪音，
// 因此这里每个请求新建一个 recorder，而不是全局共享。
func WithWarningRecorder(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		recorder := &recorder{writer: w}
		req = req.WithContext(warning.WithWarningRecorder(req.Context(), recorder))
		handler.ServeHTTP(w, req)
	})
}

// 总量上限设为 4KB 左右，单条 256 rune：HTTP 头过大可能被代理拒绝，
// 但截断粒度必须落在 rune 边界，避免产生非法 UTF-8。
var (
	truncateAtTotalRunes = 4 * 1024
	truncateItemRunes    = 256
)

type recordedWarning struct {
	agent string
	text  string
}

// recorder 收集单个请求产生的 warning，并在超过上限后统一截断重放。
// 它会被多个 goroutine 并发调用（不同 webhook/插件），因此所有状态都由 lock 保护。
type recorder struct {
	// lock guards calls to AddWarning from multiple threads
	lock sync.Mutex

	// recorded tracks whether AddWarning was already called with a given text
	recorded map[string]bool

	// ordered tracks warnings added so they can be replayed and truncated if needed
	ordered []recordedWarning

	// written tracks how many runes of text have been added as warning headers
	written int

	// truncating tracks if we have already exceeded truncateAtTotalRunes and are now truncating warning messages as we add them
	truncating bool

	// writer is the response writer to add warning headers to
	writer http.ResponseWriter
}

// AddWarning 追加一条 Warning 头；重复文本、空文本以及已进入截断模式后的溢出
// 输入都会被丢弃。
func (r *recorder) AddWarning(agent, text string) {
	if len(text) == 0 {
		return
	}

	r.lock.Lock()
	defer r.lock.Unlock()

	// if we've already exceeded our limit and are already truncating, return early
	if r.written >= truncateAtTotalRunes && r.truncating {
		return
	}

	// init if needed
	if r.recorded == nil {
		r.recorded = map[string]bool{}
	}

	// dedupe if already warned
	if r.recorded[text] {
		return
	}
	r.recorded[text] = true
	r.ordered = append(r.ordered, recordedWarning{agent: agent, text: text})

	// truncate on a rune boundary, if needed
	textRuneLength := utf8.RuneCountInString(text)
	if r.truncating && textRuneLength > truncateItemRunes {
		text = string([]rune(text)[:truncateItemRunes])
		textRuneLength = truncateItemRunes
	}

	// compute the header
	header, err := net.NewWarningHeader(299, agent, text)
	if err != nil {
		return
	}

	// if this fits within our limit, or we're already truncating, write and return
	if r.written+textRuneLength <= truncateAtTotalRunes || r.truncating {
		r.written += textRuneLength
		r.writer.Header().Add("Warning", header)
		return
	}

	// otherwise, enable truncation, reset, and replay the existing items as truncated warnings
	//
	// 中文说明：一旦超限就不可逆地进入截断模式，并清空已写头重新生成，
	// 保证最终响应里所有 warning 长度一致、不会出现“前几条完整后面残缺”。
	r.truncating = true
	r.written = 0
	r.writer.Header().Del("Warning")
	utilruntime.HandleError(fmt.Errorf("exceeded max warning header size, truncating"))
	for _, w := range r.ordered {
		agent := w.agent
		text := w.text

		textRuneLength := utf8.RuneCountInString(text)
		if textRuneLength > truncateItemRunes {
			text = string([]rune(text)[:truncateItemRunes])
			textRuneLength = truncateItemRunes
		}
		if header, err := net.NewWarningHeader(299, agent, text); err == nil {
			r.written += textRuneLength
			r.writer.Header().Add("Warning", header)
		}
	}
}
