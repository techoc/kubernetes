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
	"math/rand"
	"net/http"
	"sync"
)

// GoawayDecider 决定某个请求是否应触发 GOAWAY。
type GoawayDecider interface {
	Goaway(r *http.Request) bool
}

var (
	// 用池复用 rand.Rand，避免共享实例加锁带来的竞争。
	//
	// rand.Rand 不是并发安全的，而每个请求都加锁会形成热点；用 sync.Pool
	// 复用实例可以避免锁竞争。种子取自全局 rand，无需额外同步。
	randPool = &sync.Pool{
		New: func() interface{} {
			return rand.New(rand.NewSource(rand.Int63()))
		},
	}
)

// WithProbabilisticGoaway 返回一个 handler，按给定概率对 HTTP/2 请求发送 GOAWAY。
// 客户端收到后，已在途的长请求不受影响，新请求会用新 TCP 连接，
// 从而有机会被负载均衡重新分配到其他服务器。
func WithProbabilisticGoaway(inner http.Handler, chance float64) http.Handler {
	return &goaway{
		handler: inner,
		decider: &probabilisticGoawayDecider{
			chance: chance,
			next: func() float64 {
				rnd := randPool.Get().(*rand.Rand)
				ret := rnd.Float64()
				randPool.Put(rnd)
				return ret
			},
		},
	}
}

// goaway 按决策器的判断对 HTTP/2 请求发送 GOAWAY。
type goaway struct {
	handler http.Handler
	decider GoawayDecider
}

// ServeHTTP 实现 HTTP handler。
//
// 只在 HTTP/2 上生效：HTTP/1.1 的 "Connection: close" 语义不同，且该过滤器
// 的目的是让长连接（HTTP/2 复用）有机会重新负载均衡。
func (h *goaway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Proto == "HTTP/2.0" && h.decider.Goaway(r) {
		// 发送 GOAWAY，并在连接空闲时关闭 TCP 连接。
		w.Header().Set("Connection", "close")
	}

	h.handler.ServeHTTP(w, r)
}

// probabilisticGoawayDecider 按给定概率决定是否发送 GOAWAY。
type probabilisticGoawayDecider struct {
	chance float64
	next   func() float64
}

// Goaway 实现 GoawayDecider。
func (p *probabilisticGoawayDecider) Goaway(r *http.Request) bool {
	return p.next() < p.chance
}
