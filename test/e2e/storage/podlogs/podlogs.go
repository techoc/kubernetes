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

// Package podlogs 实时捕获 namespace 中部分或全部 Pod 产生的事件和日志消息。
// 既便于调试正在运行的测试（了解当前发生了什么），也便于分析 CI 运行输出
// （事件按时间顺序排列，还能看到通常无法获取的输出，如命令的 stdout 消息）。
package podlogs

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
)

// LogOutput 决定 CopyAllLogs 的输出去向。
//
// 与接收日志输出相关的错误消息单独存放，可选地写入 StatusWriter。
//
// 日志输出可以发送到一个或多个目标。
type LogOutput struct {
	// 非 nil 时，遇到的错误会记录到这里。
	StatusWriter io.Writer

	// 非 nil 时，所有容器输出都以 "<pod>/<container>:" 为前缀写入此 writer。
	LogWriter io.Writer

	// 每个容器一个日志文件的基础目录。
	// 非空时，每个日志文件的完整路径为 <log path prefix><pod>-<container>.log。
	LogPathPrefix string

	// LogOpen 非 nil 时，每当开始监听某个容器的日志输出就会调用它。
	// 返回 nil 表示输出可以丢弃，除非它被写到别处。
	//
	// 容器的 stdout 和 stderr 输出会写入返回的 writer。
	// 若该 writer 实现了 io.Closer，则所有输出处理完毕后会将其关闭。
	// 每次写入都是单行，包含换行符。
	// 写入错误会被忽略。
	LogOpen func(podName, containerName string) io.Writer
}

// 匹配来自 pkg/kubelet/kubelet_pods.go 的无害错误。
var expectedErrors = regexp.MustCompile(`container .* in pod .* is (terminated|waiting to start|not available)|the server could not find the requested resource|context canceled`)

// CopyAllLogs 相当于对给定 namespace ns 中所有当前及未来的 Pod 调用 CopyPodLogs。
func CopyAllLogs(ctx context.Context, cs clientset.Interface, ns string, to LogOutput) error {
	return CopyPodLogs(ctx, cs, ns, "", to)
}

// CopyPodLogs 跟踪给定 podName 的 Pod 中所有容器的日志（包括将来创建的容器），
// 并按输出选项的配置写入每一行日志，直到 context 结束或发生错误。
//
// podName 为空时，会跟踪给定 namespace ns 中的所有 Pod。
//
// 注意，目前无法在删除 Pod 之前强制完成日志收集，因此 "停止 Pod" 与
// "收集日志条目" 之间存在已知的竞态。另一种方案是采用阻塞式函数，
// 收集所有当前运行中 Pod 的日志，但那样就无法覆盖已被删除的 Pod。
//
// 还有一种竞态出现在 Pod 关闭时：日志记录停止，但如果 Pod 没有足够快地从
// apiserver 删除，日志记录就会恢复并重复输出旧日志。过去允许这种行为，
// 基于的假设是：重复记录总比漏掉那些启动后立即终止的 Pod 的日志、或
// 日志临时中断期间的日志要好。
//
// 但事实证明这很容易引起混淆，所以现在采用一种启发式策略：如果某个容器的
// 日志已经被捕获过，那么当 Pod 被标记为删除时，就不再恢复捕获。
func CopyPodLogs(ctx context.Context, cs clientset.Interface, ns, podName string, to LogOutput) error {
	options := meta.ListOptions{}
	if podName != "" {
		options = meta.ListOptions{
			FieldSelector: fmt.Sprintf("metadata.name=%s", podName),
		}
	}
	watcher, err := cs.CoreV1().Pods(ns).Watch(ctx, options)

	if err != nil {
		return fmt.Errorf("cannot create Pod event watcher: %w", err)
	}

	go func() {
		var m sync.Mutex
		// key 为 pod/container 名称，value 表示当前是否正在记录该容器的日志。
		active := map[string]bool{}
		// key 为 pod/container/container-id，value 表示是否曾经开始捕获其输出。
		started := map[string]bool{}
		// key 为 pod/container/container-id，value 为已看到最后一行日志的时间戳。
		latest := map[string]*meta.Time{}

		check := func() {
			m.Lock()
			defer m.Unlock()

			pods, err := cs.CoreV1().Pods(ns).List(ctx, options)
			if err != nil {
				if ctx.Err() == nil && to.StatusWriter != nil {
					_, _ = fmt.Fprintf(to.StatusWriter, "ERROR: get pod list in %s: %s\n", ns, err)
				}
				return
			}

			for _, pod := range pods.Items {
				for i, c := range pod.Spec.Containers {
					// 防御性检查：数组应为每个容器都包含一项。
					if len(pod.Status.ContainerStatuses) <= i {
						continue
					}
					name := pod.ObjectMeta.Name + "/" + c.Name
					id := name + "/" + pod.Status.ContainerStatuses[i].ContainerID
					if active[name] ||
						// 如果之前已处理过某个容器而现在它已终止，则不会再有新输出，可以忽略。
						(pod.Status.ContainerStatuses[i].State.Terminated != nil &&
							started[id]) ||
						// 容器可能已停止运行但 State.Terminated 尚未更新，因此还要检查 Pod 是否被删除。
						(pod.DeletionTimestamp != nil && started[id]) ||
						// 容器只有处于 Running 或 Terminated 状态时才获取日志；
						// 其他状态获取日志只会得到不得不抑制的错误。
						(pod.Status.ContainerStatuses[i].State.Running == nil &&
							pod.Status.ContainerStatuses[i].State.Terminated == nil) {
						continue
					}

					// 确定输出目标。若失败，故意不清除 active[name] 标记就返回，
					// 避免反复尝试创建输出文件。
					var logWithPrefix, logWithoutPrefix, output io.Writer
					var prefix string
					podHandled := false
					if to.LogWriter != nil {
						podHandled = true
						logWithPrefix = to.LogWriter
						nodeName := pod.Spec.NodeName
						if len(nodeName) > 10 {
							nodeName = nodeName[0:4] + ".." + nodeName[len(nodeName)-4:]
						}
						prefix = name + "@" + nodeName + ": "
					}
					if to.LogPathPrefix != "" {
						podHandled = true
						filename := to.LogPathPrefix + pod.ObjectMeta.Name + "-" + c.Name + ".log"
						if err := os.MkdirAll(path.Dir(filename), 0755); err != nil {
							if to.StatusWriter != nil {
								_, _ = fmt.Fprintf(to.StatusWriter, "ERROR: pod log: create directory for %s: %s\n", filename, err)
							}
							return
						}
						// 测试套件可能多次运行同一测试，因此这里必须追加写入。
						file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
						if err != nil {
							if to.StatusWriter != nil {
								_, _ = fmt.Fprintf(to.StatusWriter, "ERROR: pod log: create file %s: %s\n", filename, err)
							}
							return
						}
						logWithoutPrefix = file
					}
					if to.LogOpen != nil {
						if writer := to.LogOpen(pod.Name, c.Name); writer != nil {
							podHandled = true
							output = writer
						}
					}

					if !podHandled {
						// 没有输出目标对这个 Pod 感兴趣，跳过。
						continue
					}

					closeOutput := func() {
						// 全部执行，即使其中一个发生 panic。
						defer maybeClose(logWithPrefix)
						defer maybeClose(logWithoutPrefix)
						defer maybeClose(output)
					}

					sinceTime := latest[id]
					readCloser, err := logsForPod(ctx, cs, ns, pod.ObjectMeta.Name,
						&v1.PodLogOptions{
							Container:  c.Name,
							Follow:     true,
							Timestamps: true,
							SinceTime:  sinceTime,
						})
					if err != nil {
						closeOutput()

						// 这里确实会出现一些"正常"错误，比如读取过早，可以忽略。
						if to.StatusWriter != nil &&
							expectedErrors.FindStringIndex(err.Error()) == nil {
							_, _ = fmt.Fprintf(to.StatusWriter, "WARNING: pod log: %s: %s\n", name, err)
						}
						continue
					}

					active[name] = true
					started[id] = true

					go func() {
						defer closeOutput()
						first := true
						defer func() {
							m.Lock()
							// 如果从未输出过任何内容，也跳过结束消息。
							if !first {
								if logWithPrefix != nil {
									_, _ = fmt.Fprintf(logWithPrefix, "%s==== end of pod log at %s ====\n", prefix, latest[id])
								}
								if logWithoutPrefix != nil {
									_, _ = fmt.Fprintf(logWithoutPrefix, "==== end of pod log for container %s at %s ====\n", name, latest[id])
								}
							}
							active[name] = false
							m.Unlock()
							readCloser.Close()
						}()
						scanner := bufio.NewScanner(readCloser)
						var latestTS time.Time
						for scanner.Scan() {
							line := scanner.Text()
							// 过滤预期的 "end of stream" 错误消息，它只会让不知情的开发者困惑。
							// 同样过滤从尚未就绪的容器读取日志的尝试。
							if !strings.HasPrefix(line, "rpc error: code = Unknown desc = Error: No such container:") &&
								!strings.HasPrefix(line, "unable to retrieve container logs for ") &&
								!strings.HasPrefix(line, "Unable to retrieve container logs for ") {
								if first {
									// 同一日志可能被不同测试实例多次写入，额外输出一行以分隔它们，
									// 同时提供一些有用的附加信息。
									since := "(initial part)"
									if sinceTime != nil {
										since = fmt.Sprintf("since %s", sinceTime)
									}
									if logWithPrefix != nil {
										_, _ = fmt.Fprintf(logWithPrefix, "%s==== start of pod log %s ====\n", prefix, since)
									}
									if logWithoutPrefix != nil {
										_, _ = fmt.Fprintf(logWithoutPrefix, "==== start of pod log for container %s %s ====\n", name, since)
									}
									first = false
								}
								index := strings.Index(line, " ")
								if index > 0 {
									ts, err := time.Parse(time.RFC3339, line[:index])
									if err == nil {
										latestTS = ts
										// 日志输出通常自带带时间戳的头部，因此去掉 PodLogOptions 添加的时间戳。
										line = line[index+1:]
									}
								}
								if logWithPrefix != nil {
									_, _ = fmt.Fprintf(logWithPrefix, "%s%s\n", prefix, line)
								}
								if logWithoutPrefix != nil {
									_, _ = fmt.Fprintln(logWithoutPrefix, line)
								}
								if output != nil {
									_, _ = output.Write([]byte(line + "\n"))
								}
							}
						}

						if !latestTS.IsZero() {
							m.Lock()
							defer m.Unlock()
							latest[id] = &meta.Time{Time: latestTS}
						}
					}()
				}
			}
		}

		// 通过 watch 事件判断是否可开始记录日志并记录有意义的事件；
		// 同时定期检查，以防失败之后没有后续 Pod 变更事件。
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		check()
		for {
			select {
			case <-watcher.ResultChan():
				check()
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}

func maybeClose(writer io.Writer) {
	if closer, ok := writer.(io.Closer); ok {
		_ = closer.Close()
	}
}

// logsForPod 开始读取指定 Pod 的日志。若 Pod 有多个容器，必须设置 opts.Container。
// context 结束时会停止读取。
// 若 Pod 在流式读取期间被删除，流中会包含格式化错误消息，并以如下内容结束：
//
//	rpc error: code = Unknown desc = Error: No such container: 41a...
func logsForPod(ctx context.Context, cs clientset.Interface, ns, pod string, opts *v1.PodLogOptions) (io.ReadCloser, error) {
	return cs.CoreV1().Pods(ns).GetLogs(pod, opts).Stream(ctx)
}

// WatchPods 打印指定 namespace 的 Pod 状态事件；namespace 为空时打印所有 namespace 的事件。
// 若调用方不希望在停止 watch 时关闭文件，closer 可以为 nil。
func WatchPods(ctx context.Context, cs clientset.Interface, ns string, to io.Writer, toCloser io.Closer) (finalErr error) {
	defer func() {
		if finalErr != nil && toCloser != nil {
			toCloser.Close()
		}
	}()

	pods, err := cs.CoreV1().Pods(ns).Watch(ctx, meta.ListOptions{})
	if err != nil {
		return fmt.Errorf("cannot create Pod watcher: %w", err)
	}
	defer func() {
		if finalErr != nil {
			pods.Stop()
		}
	}()

	events, err := cs.CoreV1().Events(ns).Watch(ctx, meta.ListOptions{})
	if err != nil {
		return fmt.Errorf("cannot create Event watcher: %w", err)
	}

	go func() {
		defer func() {
			pods.Stop()
			events.Stop()
			if toCloser != nil {
				toCloser.Close()
			}
		}()
		timeFormat := "15:04:05.000"
		for {
			select {
			case e := <-pods.ResultChan():
				if e.Object == nil {
					continue
				}

				pod, ok := e.Object.(*v1.Pod)
				if !ok {
					continue
				}
				buffer := new(bytes.Buffer)
				fmt.Fprintf(buffer,
					"%s pod: %s: %s/%s %s: %s %v\n",
					time.Now().Format(timeFormat),
					e.Type,
					pod.Namespace,
					pod.Name,
					pod.Status.Phase,
					pod.Status.Reason,
					pod.Status.Conditions,
				)
				for _, cst := range pod.Status.ContainerStatuses {
					fmt.Fprintf(buffer, "   %s: ", cst.Name)
					if cst.State.Waiting != nil {
						fmt.Fprintf(buffer, "WAITING: %s - %s",
							cst.State.Waiting.Reason,
							cst.State.Waiting.Message,
						)
					} else if cst.State.Running != nil {
						fmt.Fprintf(buffer, "RUNNING")
					} else if cst.State.Terminated != nil {
						fmt.Fprintf(buffer, "TERMINATED: %s - %s",
							cst.State.Terminated.Reason,
							cst.State.Terminated.Message,
						)
					}
					fmt.Fprintf(buffer, "\n")
				}
				to.Write(buffer.Bytes())
			case e := <-events.ResultChan():
				if e.Object == nil {
					continue
				}

				event, ok := e.Object.(*v1.Event)
				if !ok {
					continue
				}
				to.Write([]byte(fmt.Sprintf("%s event: %s/%s %s: %s %s: %s (%v - %v)\n",
					time.Now().Format(timeFormat),
					event.InvolvedObject.APIVersion,
					event.InvolvedObject.Kind,
					event.InvolvedObject.Name,
					event.Source.Component,
					event.Type,
					event.Message,
					event.FirstTimestamp,
					event.LastTimestamp,
				)))
			case <-ctx.Done():
				to.Write([]byte(fmt.Sprintf("%s ==== stopping pod watch ====\n",
					time.Now().Format(timeFormat))))
				return
			}
		}
	}()

	return nil
}
