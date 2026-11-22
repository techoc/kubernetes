/*
Copyright 2019 The Kubernetes Authors.

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

// Package fairqueuing 实现 API 优先级与公平性（APF）的排队与分发核心。
package fairqueuing

import (
	"math"
	"sync"
	"time"

	fcmetrics "k8s.io/apiserver/pkg/util/flowcontrol/metrics"

	"k8s.io/utils/clock"
)

// Integrator computes the moments of some variable X over time as
// read from a particular clock.  The integrals start when the
// Integrator is created, and ends at the latest operation on the
// Integrator.
// 时间加权统计：仅在取值变化或读取结果时才推进积分，避免为每个请求打点。
type Integrator interface {
	fcmetrics.Gauge

	GetResults() IntegratorResults

	// Return the results of integrating to now, and reset integration to start now
	Reset() IntegratorResults
}

// IntegratorResults holds statistical abstracts of the integration
type IntegratorResults struct {
	Duration  float64 //seconds
	Average   float64 //time-weighted
	Deviation float64 //standard deviation: sqrt(avg((value-avg)^2))
	Min, Max  float64
}

// Equal tests for semantic equality.
// This considers all NaN values to be equal to each other.
// 未产生有效积分区间时平均值为 NaN，比较时需把 NaN 视为相等。
func (x *IntegratorResults) Equal(y *IntegratorResults) bool {
	return x == y || x != nil && y != nil && x.Duration == y.Duration && x.Min == y.Min && x.Max == y.Max && (x.Average == y.Average || math.IsNaN(x.Average) && math.IsNaN(y.Average)) && (x.Deviation == y.Deviation || math.IsNaN(x.Deviation) && math.IsNaN(y.Deviation))
}

// integrator 是 Integrator 的实现，所有字段受内嵌 Mutex 保护。
type integrator struct {
	name  string
	clock clock.PassiveClock
	sync.Mutex
	lastTime time.Time
	x        float64
	moments  Moments
	min, max float64
}

// NewNamedIntegrator makes one that uses the given clock and name
func NewNamedIntegrator(clock clock.PassiveClock, name string) Integrator {
	return &integrator{
		name:     name,
		clock:    clock,
		lastTime: clock.Now(),
	}
}

// Set 设置当前值并推进积分。
func (igr *integrator) Set(x float64) {
	igr.Lock()
	igr.setLocked(x)
	igr.Unlock()
}

// Add 在当前值基础上增减，并推进积分。
func (igr *integrator) Add(deltaX float64) {
	igr.Lock()
	igr.setLocked(igr.x + deltaX)
	igr.Unlock()
}

// Inc 是 Add(1) 的简写。
func (igr *integrator) Inc() {
	igr.Add(1)
}

// Dec 是 Add(-1) 的简写。
func (igr *integrator) Dec() {
	igr.Add(-1)
}

// SetToCurrentTime 把当前时间（纳秒）作为值写入，用于需要以时间戳为变量的场景。
func (igr *integrator) SetToCurrentTime() {
	igr.Set(float64(time.Now().UnixNano()))
}

// setLocked 推进积分后更新当前值与极值；调用方必须持锁。
func (igr *integrator) setLocked(x float64) {
	igr.updateLocked()
	igr.x = x
	if x < igr.min {
		igr.min = x
	}
	if x > igr.max {
		igr.max = x
	}
}

// updateLocked 把 [lastTime, now] 区间按常量值 igr.x 积分后累加。
func (igr *integrator) updateLocked() {
	now := igr.clock.Now()
	dt := now.Sub(igr.lastTime).Seconds()
	igr.lastTime = now
	igr.moments = igr.moments.Add(ConstantMoments(dt, igr.x))
}

// GetResults 返回截至当前时刻的统计结果，不重置积分。
func (igr *integrator) GetResults() IntegratorResults {
	igr.Lock()
	defer igr.Unlock()
	return igr.getResultsLocked()
}

// Reset 返回统计结果并把积分与极值重置到当前值。
func (igr *integrator) Reset() IntegratorResults {
	igr.Lock()
	defer igr.Unlock()
	results := igr.getResultsLocked()
	igr.moments = Moments{}
	igr.min = igr.x
	igr.max = igr.x
	return results
}

// getResultsLocked 计算最终结果；调用方必须持锁。
func (igr *integrator) getResultsLocked() (results IntegratorResults) {
	igr.updateLocked()
	results.Min, results.Max = igr.min, igr.max
	results.Duration = igr.moments.ElapsedSeconds
	results.Average, results.Deviation = igr.moments.AvgAndStdDev()
	return
}

// Moments are the integrals of the 0, 1, and 2 powers of some
// variable X over some range of time.
type Moments struct {
	ElapsedSeconds float64 // integral of dt
	IntegralX      float64 // integral of x dt
	IntegralXX     float64 // integral of x*x dt
}

// ConstantMoments is for a constant X
func ConstantMoments(dt, x float64) Moments {
	return Moments{
		ElapsedSeconds: dt,
		IntegralX:      x * dt,
		IntegralXX:     x * x * dt,
	}
}

// Add combines over two ranges of time
func (igr Moments) Add(ogr Moments) Moments {
	return Moments{
		ElapsedSeconds: igr.ElapsedSeconds + ogr.ElapsedSeconds,
		IntegralX:      igr.IntegralX + ogr.IntegralX,
		IntegralXX:     igr.IntegralXX + ogr.IntegralXX,
	}
}

// Sub finds the difference between a range of time and a subrange
func (igr Moments) Sub(ogr Moments) Moments {
	return Moments{
		ElapsedSeconds: igr.ElapsedSeconds - ogr.ElapsedSeconds,
		IntegralX:      igr.IntegralX - ogr.IntegralX,
		IntegralXX:     igr.IntegralXX - ogr.IntegralXX,
	}
}

// AvgAndStdDev returns the average and standard devation
func (igr Moments) AvgAndStdDev() (float64, float64) {
	// 没有时间跨度时无法定义平均值，返回 NaN 以区别于真实的 0。
	if igr.ElapsedSeconds <= 0 {
		return math.NaN(), math.NaN()
	}
	avg := igr.IntegralX / igr.ElapsedSeconds
	// standard deviation is sqrt( average( (x - xbar)^2 ) )
	// = sqrt( Integral( x^2 + xbar^2 -2*x*xbar dt ) / Duration )
	// = sqrt( ( Integral( x^2 dt ) + Duration * xbar^2 - 2*xbar*Integral(x dt) ) / Duration)
	// = sqrt( Integral(x^2 dt)/Duration - xbar^2 )
	variance := igr.IntegralXX/igr.ElapsedSeconds - avg*avg
	// 浮点误差可能让方差略小于 0，此时标准差无定义，返回 NaN。
	if variance >= 0 {
		return avg, math.Sqrt(variance)
	}
	return avg, math.NaN()
}
