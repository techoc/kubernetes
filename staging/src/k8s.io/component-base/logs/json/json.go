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

// Package json 提供基于 zap 的 JSON 日志格式实现，并通过 logr 桥接
// 到 klog 的上下文日志接口。
package json

import (
	"io"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"k8s.io/component-base/featuregate"
	logsapi "k8s.io/component-base/logs/api/v1"
)

var (
	// timeNow stubbed out for testing
	//
	// 时间编码被单独抽出，测试可固定时间戳以获得确定输出。
	timeNow = time.Now
)

// runtime 持有可动态调整的 verbosity，作为 zap 的 LevelEnabler。
type runtime struct {
	// v 用原子方式读写：日志级别可在运行时由 GlogSetter 修改。
	v uint32
}

// ZapV 把 klog 风格的正向 verbosity 转为 zap 的负值级别。
func (r *runtime) ZapV() zapcore.Level {
	// zap levels are inverted: everything with a verbosity >= threshold gets logged.
	return -zapcore.Level(atomic.LoadUint32(&r.v))
}

// Enabled implements the zapcore.LevelEnabler interface.
//
// 日志级别判断在每次写日志时调用，必须无锁、廉价。
func (r *runtime) Enabled(level zapcore.Level) bool {
	return level >= r.ZapV()
}

// SetVerbosityLevel 原子更新 verbosity，与 Enabled 并发安全。
func (r *runtime) SetVerbosityLevel(v uint32) error {
	atomic.StoreUint32(&r.v, v)
	return nil
}

var _ zapcore.LevelEnabler = &runtime{}

// NewJSONLogger creates a new json logr.Logger and its associated
// control interface. The separate error stream is optional and may be nil.
// The encoder config is also optional.
//
// errorStream 非 nil 时按级别拆分输出：错误写 errorStream，其余写 infoStream，
// 便于运维只采集错误流或对两者做不同处理。
func NewJSONLogger(v logsapi.VerbosityLevel, infoStream, errorStream zapcore.WriteSyncer, encoderConfig *zapcore.EncoderConfig) (logr.Logger, logsapi.RuntimeControl) {
	r := &runtime{v: uint32(v)}

	if encoderConfig == nil {
		// 默认字段名与 klog JSON 输出保持兼容，避免下游解析器需要区分格式来源。
		encoderConfig = &zapcore.EncoderConfig{
			MessageKey:     "msg",
			CallerKey:      "caller",
			NameKey:        "logger",
			TimeKey:        "ts",
			EncodeTime:     epochMillisTimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		}
	}

	encoder := zapcore.NewJSONEncoder(*encoderConfig)
	var core zapcore.Core
	if errorStream == nil {
		core = zapcore.NewCore(encoder, infoStream, r)
	} else {
		// 两个 core 共用同一 encoder 与级别门控，保证拆分后行格式一致。
		highPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
			return lvl >= zapcore.ErrorLevel && r.Enabled(lvl)
		})
		lowPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
			return lvl < zapcore.ErrorLevel && r.Enabled(lvl)
		})
		core = zapcore.NewTee(
			zapcore.NewCore(encoder, errorStream, highPriority),
			zapcore.NewCore(encoder, infoStream, lowPriority),
		)
	}
	l := zap.New(core, zap.WithCaller(true))
	// zapr 选项让 "v" 作为信息级别键、"err" 作为错误键，与 klog 约定一致。
	return zapr.NewLoggerWithOptions(l, zapr.LogInfoLevel("v"), zapr.ErrorKey("err")),
		logsapi.RuntimeControl{
			SetVerbosityLevel: r.SetVerbosityLevel,
			Flush: func() {
				// 忽略 Sync 错误：多数输出流不支持 sync，失败不应影响退出。
				_ = l.Sync()
			},
		}
}

// epochMillisTimeEncoder 输出 Unix 毫秒浮点数，精度足以区分高频日志且字段紧凑。
func epochMillisTimeEncoder(_ time.Time, enc zapcore.PrimitiveArrayEncoder) {
	nanos := timeNow().UnixNano()
	millis := float64(nanos) / float64(time.Millisecond)
	enc.AppendFloat64(millis)
}

// Factory produces JSON logger instances.
type Factory struct{}

var _ logsapi.LogFormatFactory = Factory{}

// Feature 返回该格式所属的特性门控：JSON 输出仍处于 Beta。
func (f Factory) Feature() featuregate.Feature {
	return logsapi.LoggingBetaOptions
}

// Create 按配置构造日志器；SplitStream 决定是否把错误单独写到 stderr。
func (f Factory) Create(c logsapi.LoggingConfiguration, o logsapi.LoggingOptions) (logr.Logger, logsapi.RuntimeControl) {
	// We intentionally avoid all os.File.Sync calls. Output is unbuffered,
	// therefore we don't need to flush, and calling the underlying fsync
	// would just slow down writing.
	//
	// The assumption is that logging only needs to ensure that data gets
	// written to the output stream before the process terminates, but
	// doesn't need to worry about data not being written because of a
	// system crash or powerloss.
	stderr := zapcore.Lock(AddNopSync(o.ErrorStream))
	if c.Options.JSON.SplitStream {
		stdout := zapcore.Lock(AddNopSync(o.InfoStream))
		size := c.Options.JSON.InfoBufferSize.Value()
		if size > 0 {
			// Prevent integer overflow.
			// 上限 2GiB，避免 int 转换溢出或分配不合理的大缓冲。
			if size > 2*1024*1024*1024 {
				size = 2 * 1024 * 1024 * 1024
			}
			stdout = &zapcore.BufferedWriteSyncer{
				WS:   stdout,
				Size: int(size),
			}
		}
		// stdout for info messages, stderr for errors.
		return NewJSONLogger(c.Verbosity, stdout, stderr, nil)
	}
	// Write info messages and errors to stderr to prevent mixing with normal program output.
	// 单流模式下全部写 stderr，避免污染组件的正式 stdout 输出（如 kubectl 数据）。
	return NewJSONLogger(c.Verbosity, stderr, nil, nil)
}

// AddNoSync adds a NOP Sync implementation.
//
// 包一层 no-op Sync 是为了满足 zapcore.WriteSyncer 接口，同时明确不做 fsync。
func AddNopSync(writer io.Writer) zapcore.WriteSyncer {
	return nopSync{Writer: writer}
}

// nopSync 是 io.Writer 到 WriteSyncer 的适配器，Sync 无操作。
type nopSync struct {
	io.Writer
}

// Sync 始终成功：日志只需在进程退出前写入流，无需持久化保证。
func (f nopSync) Sync() error {
	return nil
}
