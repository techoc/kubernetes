/*
Copyright 2016 The Kubernetes Authors.

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

package workqueue

import (
	"testing"
	"time"
)

// TestItemExponentialFailureRateLimiter 验证按 item 独立计数的指数退避：
// 每个 item 从 baseDelay 开始翻倍，Forget 后计数与延迟都清零。
func TestItemExponentialFailureRateLimiter(t *testing.T) {
	limiter := NewItemExponentialFailureRateLimiter(1*time.Millisecond, 1*time.Second)

	if e, a := 1*time.Millisecond, limiter.When("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := 2*time.Millisecond, limiter.When("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := 4*time.Millisecond, limiter.When("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := 8*time.Millisecond, limiter.When("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := 16*time.Millisecond, limiter.When("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := 5, limiter.NumRequeues("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

	if e, a := 1*time.Millisecond, limiter.When("two"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := 2*time.Millisecond, limiter.When("two"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := 2, limiter.NumRequeues("two"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

	limiter.Forget("one")
	if e, a := 0, limiter.NumRequeues("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := 1*time.Millisecond, limiter.When("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

}

// TestItemExponentialFailureRateLimiterOverFlow 验证多次失败后延迟封顶为
// maxDelay，以及大数值下不会因位移溢出得到负数或异常值。
func TestItemExponentialFailureRateLimiterOverFlow(t *testing.T) {
	limiter := NewItemExponentialFailureRateLimiter(1*time.Millisecond, 1000*time.Second)
	for i := 0; i < 5; i++ {
		limiter.When("one")
	}
	if e, a := 32*time.Millisecond, limiter.When("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

	for i := 0; i < 1000; i++ {
		limiter.When("overflow1")
	}
	if e, a := 1000*time.Second, limiter.When("overflow1"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

	limiter = NewItemExponentialFailureRateLimiter(1*time.Minute, 1000*time.Hour)
	for i := 0; i < 2; i++ {
		limiter.When("two")
	}
	if e, a := 4*time.Minute, limiter.When("two"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

	for i := 0; i < 1000; i++ {
		limiter.When("overflow2")
	}
	if e, a := 1000*time.Hour, limiter.When("overflow2"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

}

// TestItemFastSlowRateLimiter 验证“快-慢”限速：前 maxFastAttempts 次使用
// fastDelay，之后切换到 slowDelay；计数同样按 item 独立。
func TestItemFastSlowRateLimiter(t *testing.T) {
	limiter := NewItemFastSlowRateLimiter(5*time.Millisecond, 10*time.Second, 3)

	if e, a := 5*time.Millisecond, limiter.When("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := 5*time.Millisecond, limiter.When("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := 5*time.Millisecond, limiter.When("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := 10*time.Second, limiter.When("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := 10*time.Second, limiter.When("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := 5, limiter.NumRequeues("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

	if e, a := 5*time.Millisecond, limiter.When("two"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := 5*time.Millisecond, limiter.When("two"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := 2, limiter.NumRequeues("two"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

	limiter.Forget("one")
	if e, a := 0, limiter.NumRequeues("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := 5*time.Millisecond, limiter.When("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

}

// TestMaxOfRateLimiter 验证组合限速器取各子限速器延迟的最大值：
// 前三次 fast 与 exponential 都是 5ms（取大），之后 fast 切到 3s 成为主导。
func TestMaxOfRateLimiter(t *testing.T) {
	limiter := NewMaxOfRateLimiter(
		NewItemFastSlowRateLimiter(5*time.Millisecond, 3*time.Second, 3),
		NewItemExponentialFailureRateLimiter(1*time.Millisecond, 1*time.Second),
	)

	if e, a := 5*time.Millisecond, limiter.When("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := 5*time.Millisecond, limiter.When("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := 5*time.Millisecond, limiter.When("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := 3*time.Second, limiter.When("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := 3*time.Second, limiter.When("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := 5, limiter.NumRequeues("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

	if e, a := 5*time.Millisecond, limiter.When("two"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := 5*time.Millisecond, limiter.When("two"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := 2, limiter.NumRequeues("two"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

	limiter.Forget("one")
	if e, a := 0, limiter.NumRequeues("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := 5*time.Millisecond, limiter.When("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

}

// TestWithMaxWaitRateLimiter 验证 WithMaxWaitRateLimiter 会把底层限速器的
// 延迟截断到 maxWait，即使底层之后还会给出更大的值。
func TestWithMaxWaitRateLimiter(t *testing.T) {
	limiter := NewWithMaxWaitRateLimiter(NewStepRateLimiter(5*time.Millisecond, 1000*time.Second, 100), 500*time.Second)
	for i := 0; i < 100; i++ {
		if e, a := 5*time.Millisecond, limiter.When(i); e != a {
			t.Errorf("expected %v, got %v ", e, a)
		}
	}

	for i := 100; i < 200; i++ {
		if e, a := 500*time.Second, limiter.When(i); e != a {
			t.Errorf("expected %v, got %v", e, a)
		}
	}
}

var _ RateLimiter = &StepRateLimiter{}

// NewStepRateLimiter 构造一个测试用的阶梯限速器：前 threshold 次返回
// baseDelay，之后永远返回 maxDelay。
func NewStepRateLimiter(baseDelay time.Duration, maxDelay time.Duration, threshold int) RateLimiter {
	return &StepRateLimiter{
		baseDelay: baseDelay,
		maxDelay:  maxDelay,
		threshold: threshold,
	}
}

// StepRateLimiter 是只在测试中使用的阶梯式 RateLimiter 实现。
type StepRateLimiter struct {
	count     int
	threshold int
	baseDelay time.Duration
	maxDelay  time.Duration
}

// When 返回本次应等待的时长，并递增内部计数。
func (r *StepRateLimiter) When(item interface{}) time.Duration {
	r.count += 1
	if r.count <= r.threshold {
		return r.baseDelay
	}
	return r.maxDelay
}

// NumRequeues 固定返回 0；该测试实现不跟踪重入次数。
func (r *StepRateLimiter) NumRequeues(item interface{}) int {
	return 0
}

// Forget 是空实现；阶梯状态按整个限速器而非 item 维护。
func (r *StepRateLimiter) Forget(item interface{}) {
}
