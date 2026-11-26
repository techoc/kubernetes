/*
Copyright 2023 The Kubernetes Authors.

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

package e2enode

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
)

type execCommand struct {
	// ExitCode 是容器的退出状态
	ExitCode int
	// StartDelay 是容器启动前应延迟的时间
	StartDelay int
	// Delay 是容器退出前应延迟的时间
	Delay int
	// 设置 LoopForever 会让命令每秒记录一次日志，循环直到
	// 被终止
	LoopForever bool
	// TerminationSeconds 是容器收到 SIGTERM 后
	// 终止所需的时间。
	TerminationSeconds int
	// ContainerName 是要追加日志的容器名称。为空时，
	// 使用 ExecCommand 中指定的名称。
	ContainerName string
	// LoopPeriod 是执行循环的时间间隔。
	LoopPeriod float32
}

// ExecCommand 返回要在容器中执行的命令，它实现
// execCommand 并把活动记录到容器专属日志中，该日志在
// 容器重启后仍保留。最终日志写入容器日志，以便
// 测试框架在容器执行期间取回。
// 同时写入 /proc/1/fd/1，让 lifecycle hook handler 的日志
// 也能被捕获。
func ExecCommand(name string, c execCommand) []string {
	var cmd bytes.Buffer
	// 所有输出格式为：
	// time-since-boot timestamp container-name message

	// busybox 的 time 命令不支持亚秒显示。
	// 必须用 nmeter 获取毫秒部分。
	timeCmd := "`date -u +%FT$(nmeter -d0 '%3t' | head -n1)Z`"
	containerName := name
	if c.ContainerName != "" {
		containerName = c.ContainerName
	}
	containerLog := fmt.Sprintf("/persistent/%s.log", containerName)

	fmt.Fprintf(&cmd, "touch %s; ", containerLog)
	if c.ContainerName == "" {
		fmt.Fprintf(&cmd, "cat %s >> /proc/1/fd/1; ", containerLog)
	}

	fmt.Fprintf(&cmd, "echo %s '%s Starting %d' | tee -a %s >> /proc/1/fd/1; ", timeCmd, name, c.StartDelay, containerLog)
	fmt.Fprintf(&cmd, "_term() { sleep %d; echo %s '%s Exiting' | tee -a %s >> /proc/1/fd/1; exit %d; }; ", c.TerminationSeconds, timeCmd, name, containerLog, c.ExitCode)
	fmt.Fprintf(&cmd, "trap _term TERM; ")
	if c.StartDelay != 0 {
		fmt.Fprint(&cmd, sleepCommand(c.StartDelay))
	}
	// 可以检查 started 文件看容器是否已启动
	fmt.Fprintf(&cmd, "echo %s '%s Started' | tee -a %s >> /proc/1/fd/1; ", timeCmd, name, containerLog)
	fmt.Fprintf(&cmd, "touch started; ")
	fmt.Fprintf(&cmd, "echo %s '%s Delaying %d' | tee -a %s >> /proc/1/fd/1; ", timeCmd, name, c.Delay, containerLog)
	if c.Delay != 0 {
		fmt.Fprint(&cmd, sleepCommand(c.Delay))
	}

	loopPeriod := float32(1)
	if c.LoopPeriod != 0 {
		loopPeriod = c.LoopPeriod
	}
	if c.LoopForever {
		fmt.Fprintf(&cmd, "while true; do echo %s '%s Looping' | tee -a %s >> /proc/1/fd/1 ; sleep %f ; done; ", timeCmd, name, containerLog, loopPeriod)
	}
	fmt.Fprintf(&cmd, "echo %s '%s Exiting'  | tee -a %s >> /proc/1/fd/1; ", timeCmd, name, containerLog)
	fmt.Fprintf(&cmd, "exit %d", c.ExitCode)
	return e2epod.GenerateScriptCmd(cmd.String())
}

// sleepCommand 返回一个命令，它在后台睡眠指定秒数
// 并等待其完成，好让父进程
// 处理信号。
func sleepCommand(seconds int) string {
	return fmt.Sprintf("exec sleep %d & wait $!; ", seconds)
}

type containerOutput struct {
	// 看到消息的时间，精确到秒
	timestamp     time.Time
	containerName string
	command       string
}
type containerOutputList []containerOutput

func (o containerOutputList) String() string {
	var b bytes.Buffer
	for i, v := range o {
		fmt.Fprintf(&b, "%d) %s %s %s\n", i, v.timestamp, v.containerName, v.command)
	}
	return b.String()
}

// 如果容器没有同时运行，RunTogether 返回错误
func (o containerOutputList) RunTogether(lhs, rhs string) error {
	if err := o.RunTogetherLhsFirst(lhs, rhs); err != nil {
		if err := o.RunTogetherLhsFirst(rhs, lhs); err != nil {
			return err
		}
	}
	return nil
}

// 如果容器没有同时运行，或 rhs 在 lhs 之前启动，RunTogetherLhsFirst 返回错误
func (o containerOutputList) RunTogetherLhsFirst(lhs, rhs string) error {
	lhsStart := o.findIndex(lhs, "Started", 0)
	if lhsStart == -1 {
		return fmt.Errorf("couldn't find that %s ever started, got\n%v", lhs, o)
	}

	rhsStart := o.findIndex(rhs, "Started", lhsStart+1)
	if rhsStart == -1 {
		return fmt.Errorf("couldn't find that %s ever started, got\n%v", rhs, o)
	}

	lhsExit := o.findIndex(lhs, "Exiting", lhsStart+1)
	if lhsExit == -1 {
		return fmt.Errorf("couldn't find that %s ever exited, got\n%v", lhs, o)
	}

	if rhsStart > lhsExit {
		return fmt.Errorf("expected %s to start before exiting %s, got\n%v", rhs, lhs, o)
	}

	rhsExit := o.findIndex(rhs, "Exiting", rhsStart+1)
	if rhsExit == -1 {
		return fmt.Errorf("couldn't find that %s ever exited, got\n%v", rhs, o)
	}

	if lhsStart > rhsExit {
		return fmt.Errorf("expected %s to start before exiting %s, got\n%v", lhs, rhs, o)
	}

	return nil
}

// 如果 lhs 没有在 rhs 之前启动，StartsBefore 返回错误
func (o containerOutputList) StartsBefore(lhs, rhs string) error {
	lhsStart := o.findIndex(lhs, "Started", 0)

	if lhsStart == -1 {
		return fmt.Errorf("couldn't find that %s ever started, got\n%v", lhs, o)
	}

	// 即使名称相同（重启场景）也有效
	rhsStart := o.findIndex(rhs, "Starting", lhsStart+1)

	if rhsStart == -1 {
		return fmt.Errorf("couldn't find that %s started after %s, got\n%v", rhs, lhs, o)
	}
	return nil
}

// 如果 lhs 没有在 rhs 之前结束，ExitsBefore 返回错误
func (o containerOutputList) ExitsBefore(lhs, rhs string) error {
	lhsExit := o.findIndex(lhs, "Exiting", 0)

	if lhsExit == -1 {
		return fmt.Errorf("couldn't find that %s ever exited, got\n%v", lhs, o)
	}

	// 即使名称相同（重启场景）也有效
	rhsExit := o.findIndex(rhs, "Exiting", lhsExit+1)

	if rhsExit == -1 {
		return fmt.Errorf("couldn't find that %s starting before %s exited (starting at idx %d), got\n%v", rhs, lhs, lhsExit+1, o)
	}
	return nil
}

// 如果未发现容器已启动，Starts 返回错误
func (o containerOutputList) Starts(name string) error {
	if idx := o.findIndex(name, "Started", 0); idx == -1 {
		return fmt.Errorf("couldn't find that %s ever started, got\n%v", name, o)
	}
	return nil
}

// 如果发现容器已启动，DoesntStart 返回错误
func (o containerOutputList) DoesntStart(name string) error {
	if idx := o.findIndex(name, "Started", 0); idx != -1 {
		return fmt.Errorf("find %s started, but didn't expect to, got\n%v", name, o)
	}
	return nil
}

// 如果未发现容器已退出，Exits 返回错误
func (o containerOutputList) Exits(name string) error {
	if idx := o.findIndex(name, "Exiting", 0); idx == -1 {
		return fmt.Errorf("couldn't find that %s ever exited, got\n%v", name, o)
	}
	return nil
}

// 如果未发现容器已重启，HasRestarted 返回错误
func (o containerOutputList) HasRestarted(name string) error {
	idx := o.findIndex(name, "Starting", 0)
	if idx == -1 {
		return fmt.Errorf("couldn't find that %s ever started, got\n%v", name, o)
	}

	idx = o.findIndex(name, "Starting", idx+1)

	if idx == -1 {
		return fmt.Errorf("couldn't find that %s ever restarted, got\n%v", name, o)
	}

	return nil
}

// 如果发现容器已重启，HasNotRestarted 返回错误
func (o containerOutputList) HasNotRestarted(name string) error {
	idx := o.findIndex(name, "Starting", 0)
	if idx == -1 {
		return fmt.Errorf("couldn't find that %s ever started, got\n%v", name, o)
	}

	idx = o.findIndex(name, "Starting", idx+1)

	if idx != -1 {
		return fmt.Errorf("found that %s restarted but wasn't expected to, got\n%v", name, o)
	}

	return nil
}

type containerOutputIndex int

func (i containerOutputIndex) IsBefore(other containerOutputIndex) error {
	if i >= other {
		return fmt.Errorf("%d should be before %d", i, other)
	}
	return nil
}

func (o containerOutputList) FindIndex(name string, command string, startIdx containerOutputIndex) (containerOutputIndex, error) {
	idx := o.findIndex(name, command, int(startIdx))
	if idx == -1 {
		return -1, fmt.Errorf("couldn't find %s %s, got\n%v", name, command, o)
	}
	return containerOutputIndex(idx), nil
}

func (o containerOutputList) findIndex(name string, command string, startIdx int) int {
	for i, v := range o {
		if i < startIdx {
			continue
		}
		if v.containerName == name && v.command == command {
			return i
		}
	}
	return -1
}
func (o containerOutputList) findLastIndex(name string, command string) int {
	found := -1
	for i, v := range o {
		if v.containerName == name && v.command == command {
			found = i
		}
	}
	return found
}

// TimeOfStart 返回指定容器启动时的 UNIX 时间（毫秒）。
func (o containerOutputList) TimeOfStart(name string) (int64, error) {
	idx := o.findIndex(name, "Starting", 0)
	if idx == -1 {
		return 0, fmt.Errorf("couldn't find that %s ever started, got\n%v", name, o)
	}
	return o[idx].timestamp.UnixMilli(), nil
}

// TimeOfLastLoop 返回指定容器最后一次循环时的 UNIX 时间（毫秒）。
func (o containerOutputList) TimeOfLastLoop(name string) (int64, error) {
	idx := o.findLastIndex(name, "Looping")
	if idx == -1 {
		return 0, fmt.Errorf("couldn't find that %s ever looped, got\n%v", name, o)
	}
	return o[idx].timestamp.UnixMilli(), nil
}

var logRe = regexp.MustCompile(`unable to retrieve container logs for (cri-o|containerd)://[0-9a-f]{64}`)

// parseOutput 合并所有 init 容器和普通容器的
// 容器日志，并解析/排序输出，生成执行日志
func parseOutput(ctx context.Context, f *framework.Framework, pod *v1.Pod) containerOutputList {
	// 累积所有状态
	var statuses []v1.ContainerStatus
	statuses = append(statuses, pod.Status.InitContainerStatuses...)
	statuses = append(statuses, pod.Status.ContainerStatuses...)

	var buf bytes.Buffer
	for _, cs := range statuses {
		log, err := e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, pod.Name, cs.Name)
		if err != nil || log == "" {
			if err != nil {
				framework.Logf("error getting logs for %s: %v", cs.Name, err)
			}
			prevLog, prevErr := e2epod.GetPreviousPodLogs(ctx, f.ClientSet, f.Namespace.Name, pod.Name, cs.Name)
			if prevErr != nil {
				framework.Logf("error getting previous logs for %s: %v", cs.Name, prevErr)
			} else {
				log = prevLog
			}
		}
		buf.WriteString(log)
	}

	// 解析
	sc := bufio.NewScanner(&buf)
	var res containerOutputList
	for sc.Scan() {
		log := sc.Text()

		// 容器日志暂时不可用时，去掉可能的前缀输出
		log = logRe.ReplaceAllString(log, "")

		fields := strings.Fields(log)
		if len(fields) < 3 {
			framework.ExpectNoError(fmt.Errorf("%v should have at least length 3", fields))
		}
		timestamp, err := time.Parse(time.RFC3339, fields[0])
		framework.ExpectNoError(err, "Failed to parse the timestamp, log: %q", log)
		res = append(res, containerOutput{
			timestamp:     timestamp,
			containerName: fields[1],
			command:       fields[2],
		})
	}

	// 用 timeSinceBoot 排序，因为它精度更高
	sort.Slice(res, func(i, j int) bool {
		return res[i].timestamp.Before(res[j].timestamp)
	})
	return res
}

// preparePod 添加一个 empty dir volume 并挂载到每个容器的 /persistent
func preparePod(pod *v1.Pod) {
	var defaultResourceRequirements = v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceMemory: resource.MustParse("15Mi"),
		},
		Limits: v1.ResourceList{
			v1.ResourceMemory: resource.MustParse("35Mi"),
		},
	}

	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		c.Resources = defaultResourceRequirements
		c.VolumeMounts = []v1.VolumeMount{
			{
				Name:      "persistent",
				MountPath: "/persistent",
			},
		}
	}
	for i := range pod.Spec.InitContainers {
		c := &pod.Spec.InitContainers[i]
		c.Resources = defaultResourceRequirements
		c.VolumeMounts = []v1.VolumeMount{
			{
				Name:      "persistent",
				MountPath: "/persistent",
			},
		}
	}

	pod.Spec.Volumes = []v1.Volume{
		{
			Name: "persistent",
			VolumeSource: v1.VolumeSource{
				EmptyDir: &v1.EmptyDirVolumeSource{},
			},
		},
	}
}

// startedPostStartGate 为 PostStart hook 生成一个 LifecycleHandler，它等待主
// 容器进程到达 `Started` 点。
func startedPostStartGate() *v1.LifecycleHandler {
	return &v1.LifecycleHandler{
		Exec: &v1.ExecAction{
			Command: []string{"sh", "-c", "until [ -f started ]; do sleep 0.1; done"},
		},
	}
}
