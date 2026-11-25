/*
Copyright 2021 The Kubernetes Authors.

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

package benchmark

import (
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"go.uber.org/zap/zapcore"
	"k8s.io/component-base/featuregate"
	logsapi "k8s.io/component-base/logs/api/v1"
	_ "k8s.io/component-base/logs/json/register"
	"k8s.io/klog/v2"
)

func BenchmarkEncoding(b *testing.B) {
	seen := map[string]bool{}

	// 每个 "data/(v[0-9]/)?*.log" 文件都应包含 JSON 日志消息。
	// 为每个文件生成一个子 benchmark，使用目录对应的日志级别进行测试。
	if err := filepath.Walk("data", func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !strings.HasSuffix(path, ".log") {
			return nil
		}
		messages, stats, err := loadLog(path)
		if err != nil {
			return err
		}
		// 只打印不重复的统计信息，在首次出现该统计的文件中展示。
		// 这里假设不存在内容不同却统计完全一致的文件。
		statsStr := stats.String()
		if !seen[statsStr] {
			b.Log(path + "\n" + statsStr)
			seen[statsStr] = true
		}
		b.Run(strings.TrimSuffix(strings.TrimPrefix(path, "data/"), ".log"), func(b *testing.B) {
			// 若目录名中含有详细级别，则取其作为阈值。
			vMatch := regexp.MustCompile(`/v(\d+)/`).FindStringSubmatch(path)
			v := 0
			if vMatch != nil {
				v, _ = strconv.Atoi(vMatch[1])
			}

			fileSizes := map[string]int{}
			test := func(b *testing.B, format string, print func(logger klog.Logger, item logMessage)) {
				state := klog.CaptureState()
				defer state.Restore()

				// 为让测试更贴近真实情况，每次写入至少经过一次系统调用。
				output := newBytesWritten(b, "/dev/null")
				c := logsapi.NewLoggingConfiguration()
				c.Format = format
				o := logsapi.LoggingOptions{
					ErrorStream: output,
					InfoStream:  output,
				}
				klog.SetOutput(output)
				defer func() {
					if err := logsapi.ResetForTest(nil); err != nil {
						b.Errorf("error resetting logsapi: %v", err)
					}
				}()
				if err := logsapi.ValidateAndApplyWithOptions(c, &o, nil); err != nil {
					b.Fatalf("Unexpected error configuring logging: %v", err)
				}
				logger := klog.Background()

				// 修改此处并启用该 if 分支，可用 slog 代替 zapr 输出 JSON。
				if format == "json" && false {
					var level slog.LevelVar
					level.Set(slog.Level(-3)) // 权宜之计
					logger = logr.FromSlogHandler(slog.NewJSONHandler(output, &slog.HandlerOptions{
						AddSource: true,
						Level:     &level,
						ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
							switch a.Key {
							case slog.TimeKey:
								// 可能是用户提供的 "time" 字段。
								if a.Value.Kind() == slog.KindTime {
									return slog.Float64("ts", float64(a.Value.Time().UnixMicro())/1000)
								}
							case slog.LevelKey:
								level := a.Value.Any().(slog.Level)
								if level >= slog.LevelError {
									// 错误日志不携带详细级别。
									return slog.Attr{}
								}
								if level >= 0 {
									return slog.Int("v", 0)
								}
								return slog.Int("v", int(-level))
							case slog.SourceKey:
								caller := zapcore.EntryCaller{
									Defined: true,
									File:    a.Value.String(),
								}
								return slog.String("caller", caller.TrimmedPath())
							}
							return a
						},
					}))
				}

				b.ResetTimer()
				start := time.Now()
				total := int64(0)
				for i := 0; i < b.N; i++ {
					for _, item := range messages {
						if item.verbosity <= v {
							print(logger, item)
							total++
						}
					}
				}
				end := time.Now()
				duration := end.Sub(start)

				// 报告 messages/s 而非 ns/op，因为每次 "op" 的处理量并不固定。
				b.ReportMetric(0, "ns/op")
				b.ReportMetric(float64(total)/duration.Seconds(), "msgs/s")
				fileSizes[filepath.Base(b.Name())] = int(output.bytesWritten)
			}

			b.Run("printf", func(b *testing.B) {
				test(b, "text", func(_ klog.Logger, item logMessage) {
					printf(item)
				})
			})
			b.Run("structured", func(b *testing.B) {
				test(b, "text", prints)
			})
			b.Run("JSON", func(b *testing.B) {
				test(b, "json", prints)
			})

			b.Logf("%s: file sizes: %v\n", path, fileSizes)
		})
		return nil
	}); err != nil {
		b.Fatalf("reading 'data' directory: %v", err)
	}
}

type loadGeneratorConfig struct {
	// 每条日志中写入的消息长度。
	messageLength int

	// 错误日志所占百分比。
	errorPercentage float64

	// 并发生成日志的 goroutine 数量。
	workers int
}

// BenchmarkWriting 模拟按一定比例混合 info 与 error 日志的写入流。与 BenchmarkEncoding 不同，
// 它侧重压测输出处理路径，并包含 caller、时间戳等常规附加信息。
//
// 背景见 https://github.com/kubernetes/kubernetes/issues/107029
func BenchmarkWriting(b *testing.B) {
	// 这里可以做成可配置的，也可以自动压测多种配置。
	config := loadGeneratorConfig{
		messageLength:   300,
		errorPercentage: 1.0,
		workers:         100,
	}

	benchmarkWriting(b, config)
}

func benchmarkWriting(b *testing.B, config loadGeneratorConfig) {
	b.Run("discard", func(b *testing.B) {
		benchmarkOutputFormats(b, config, true)
	})
	b.Run("tmp-files", func(b *testing.B) {
		benchmarkOutputFormats(b, config, false)
	})
}

func benchmarkOutputFormats(b *testing.B, config loadGeneratorConfig, discard bool) {
	b.Run("structured", func(b *testing.B) {
		benchmarkOutputFormat(b, config, discard, "text")
	})
	b.Run("JSON", func(b *testing.B) {
		benchmarkOutputFormat(b, config, discard, "json")
	})
}

func benchmarkOutputFormat(b *testing.B, config loadGeneratorConfig, discard bool, format string) {
	b.Run("single-stream", func(b *testing.B) {
		benchmarkOutputFormatStream(b, config, discard, format, false)
	})
	b.Run("split-stream", func(b *testing.B) {
		benchmarkOutputFormatStream(b, config, discard, format, true)
	})
}

func benchmarkOutputFormatStream(b *testing.B, config loadGeneratorConfig, discard bool, format string, splitStreams bool) {
	tmpDir := b.TempDir()
	state := klog.CaptureState()
	defer state.Restore()

	featureGate := featuregate.NewFeatureGate()
	logsapi.AddFeatureGates(featureGate)
	if err := featureGate.SetFromMap(map[string]bool{
		string(logsapi.LoggingAlphaOptions): true,
		string(logsapi.LoggingBetaOptions):  true,
	}); err != nil {
		b.Fatalf("Set feature gates: %v", err)
	}

	// 用与普通组件完全相同的代码创建日志配置，并提供 LoggingOptions 以重定向输出。
	var o logsapi.LoggingOptions
	c := logsapi.NewLoggingConfiguration()
	c.Format = format
	if splitStreams {
		c.Options.JSON.SplitStream = true
		if err := c.Options.JSON.InfoBufferSize.Set("64Ki"); err != nil {
			b.Fatalf("Error setting buffer size: %v", err)
		}
		c.Options.Text.SplitStream = true
		if err := c.Options.Text.InfoBufferSize.Set("64Ki"); err != nil {
			b.Fatalf("Error setting buffer size: %v", err)
		}
	}
	var files []*os.File
	if discard {
		o.ErrorStream = io.Discard
		o.InfoStream = io.Discard
	} else {
		out1, err := os.Create(filepath.Join(tmpDir, "stream-1.log"))
		if err != nil {
			b.Fatal(err)
		}
		defer out1.Close()
		out2, err := os.Create(filepath.Join(tmpDir, "stream-2.log"))
		if err != nil {
			b.Fatal(err)
		}
		defer out2.Close()

		if splitStreams {
			files = append(files, out1, out2)
			o.ErrorStream = out1
			o.InfoStream = out2
		} else {
			files = append(files, out1)
			o.ErrorStream = out1
			o.InfoStream = out1
		}
	}

	klog.SetOutput(o.ErrorStream)
	defer func() {
		if err := logsapi.ResetForTest(nil); err != nil {
			b.Errorf("error resetting logsapi: %v", err)
		}
	}()
	if err := logsapi.ValidateAndApplyWithOptions(c, &o, featureGate); err != nil {
		b.Fatalf("Unexpected error configuring logging: %v", err)
	}

	generateOutput(b, config, files...)
}

func generateOutput(b *testing.B, config loadGeneratorConfig, files ...*os.File) {
	msg := strings.Repeat("X", config.messageLength)
	err := errors.New("fail")
	start := time.Now()

	// 放大 1000 倍，因为 "go test -bench" 从 b.N == 1 开始，太小了。
	n := b.N * 1000
	total := config.workers * n

	b.ResetTimer()
	var wg sync.WaitGroup
	for i := 0; i < config.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			acc := 0.0
			for range n {
				if acc > 100 {
					klog.ErrorS(err, msg, "key", "value")
					acc -= 100
				} else {
					klog.InfoS(msg, "key", "value")
				}
				acc += config.errorPercentage
			}
		}()
	}
	wg.Wait()
	klog.Flush()
	b.StopTimer()
	end := time.Now()
	duration := end.Sub(start)

	// 报告 messages/s 而非 ns/op，因为每次 "op" 的处理量并不固定。
	b.ReportMetric(0, "ns/op")
	b.ReportMetric(float64(total)/duration.Seconds(), "msgs/s")

	// 打印结果相关信息。
	b.Logf("Wrote %d log entries in %s -> %.1f/s", total, duration, float64(total)/duration.Seconds())
	for i, file := range files {
		if file != nil {
			pos, err := file.Seek(0, io.SeekEnd)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := file.Seek(0, io.SeekStart); err != nil {
				b.Fatal(err)
			}
			max := 50
			buffer := make([]byte, max)
			actual, err := file.Read(buffer)
			if err != nil {
				if err != io.EOF {
					b.Fatal(err)
				}
				buffer = nil
			}
			if actual == max {
				buffer[max-3] = '.'
				buffer[max-2] = '.'
				buffer[max-1] = '.'
			}
			b.Logf("      %d bytes to file #%d -> %.1fMiB/s (starts with: %s)", pos, i, float64(pos)/duration.Seconds()/1024/1024, string(buffer))
		}
	}
}
