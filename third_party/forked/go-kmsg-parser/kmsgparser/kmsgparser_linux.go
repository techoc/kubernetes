/*
Copyright 2016 Euan Kemp

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

// Package kmsgparser 实现 Linux `/dev/kmsg` 格式的解析器。
// 格式详情参见：
// https://www.kernel.org/doc/Documentation/ABI/testing/dev-kmsg
// 部分实现参考了 rsyslog 的 contrib 模块：
// https://github.com/rsyslog/rsyslog/blob/v8.22.0/contrib/imkmsg/kmsg.c
//
// 本包从 github.com/euank/go-kmsg-parser/v3 的 v3.0.0 派生：
// https://github.com/euank/go-kmsg-parser/blob/v3.0.0/kmsgparser/kmsgparser.go
// 上游的 Logger 抽象（log.go）已移除，改用 klog 输出日志。
package kmsgparser

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"k8s.io/klog/v2"
)

// Parser 解析 /dev/kmsg 中的内核环形缓冲区。
type Parser interface {
	// SeekEnd 将解析器移动到 kmsg 队列末尾。
	SeekEnd() error
	// Parse 从 kmsg 读取消息，并通过 channel 返回。
	// Parse 返回前总会关闭传入的 channel。
	// 调用解析器的 Close 可取消 Parse。
	//
	// 调用 [Close] 后，调用方应继续排空 channel；且不得自行关闭传入的 channel。
	Parse(chan<- Message) error

	Close() error
}

// Message 表示一条 kmsg 日志，包括由启动时间偏移计算出的时间戳、可能多行的正文等。
// 消息格式详情参见：
// https://www.kernel.org/doc/Documentation/ABI/testing/dev-kmsg
type Message struct {
	Priority       int
	SequenceNumber int
	Timestamp      time.Time
	Message        string
}

// NewParser 按给定 Option 创建解析器。
func NewParser(opts ...Option) (Parser, error) {
	f, err := os.Open("/dev/kmsg")
	if err != nil {
		return nil, err
	}

	bootTime, err := getBootTime()
	if err != nil {
		return nil, err
	}
	p := &parser{
		kmsgReader: f,
		bootTime:   bootTime,
		follow:     true,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p, nil
}

// Option 是 [NewParser] 的配置项。
type Option func(p *parser)

// WithNoFollow 让 [Parser] 读到 kmsg 缓冲区末尾时停止读取并关闭 channel。
func WithNoFollow() func(p *parser) {
	return func(p *parser) {
		p.follow = false
	}
}

type parser struct {
	kmsgReader *os.File
	bootTime   time.Time
	// follow 决定读到 kmsg 缓冲区末尾时是停止还是继续读取，类似 dmesg --follow。
	// 默认为 true。
	follow bool
}

func getBootTime() (time.Time, error) {
	var sysinfo syscall.Sysinfo_t
	err := syscall.Sysinfo(&sysinfo)
	if err != nil {
		return time.Time{}, fmt.Errorf("could not get boot time: %v", err)
	}
	// sysinfo only has seconds
	return time.Now().Add(-1 * (time.Duration(sysinfo.Uptime) * time.Second)), nil
}

func (p *parser) SeekEnd() error {
	_, err := p.kmsgReader.Seek(0, io.SeekEnd)
	return err
}

func (p *parser) Close() error {
	return p.kmsgReader.Close()
}

// Parse 从给定 reader 读取并输出解析后的消息。若 reader 不是真正的 Linux kmsg
// 设备，由于实现依赖 `/dev/kmsg` 的特定行为，解析结果可能不正确。
// 调用方通常在 goroutine 中运行 Parse。
func (p *parser) Parse(msgs chan<- Message) error {
	// follow 模式（类似 dmesg --follow）可用 Go 常规的 epoll + 非阻塞 IO 方式读取。
	if p.follow {
		return p.readFollow(msgs)
	}
	return p.readNofollow(msgs)
}

func (p *parser) readFollow(msgs chan<- Message) error {
	defer close(msgs)
	msg := make([]byte, 8192)
	for {
		// 每次读取会得到一条完整消息。
		// https://www.kernel.org/doc/Documentation/ABI/testing/dev-kmsg
		n, err := p.kmsgReader.Read(msg)
		switch {
		case err == nil:
		case errors.Is(err, syscall.EPIPE):
			klog.Warningf("short read from kmsg; skipping")
			continue
		case errors.Is(err, io.EOF), errors.Is(err, os.ErrClosed):
			// 有人关闭了读取端
			return nil
		default:
			return fmt.Errorf("unexpected kmsg reading error: %w", err)
		}
		msgStr := string(msg[:n])
		message, err := p.parseMessage(msgStr)
		if err != nil {
			return fmt.Errorf("malformed kmsg message: %w", err)
		}
		msgs <- message
	}
}

func (p *parser) readNofollow(msgs chan<- Message) error {
	defer close(msgs)

	rawReader, err := p.kmsgReader.SyscallConn()
	if err != nil {
		return err
	}
	// 必须把 fd 设为非阻塞，才能在不使用 Go 内建 epoll 的情况下检测缓冲区末尾。
	if ctrlErr := rawReader.Control(func(fd uintptr) {
		err = syscall.SetNonblock(int(fd), true)
	}); ctrlErr != nil {
		return fmt.Errorf("error calling control on kmsg reader: %w", ctrlErr)
	}
	if err != nil {
		return fmt.Errorf("unable to set nonblocking on fd: %w", err)
	}
	msg := make([]byte, 8192)
	for {
		// 每次读取会得到一条完整消息。
		// https://www.kernel.org/doc/Documentation/ABI/testing/dev-kmsg
		var err error
		var n int
		readErr := rawReader.Read(func(fd uintptr) bool {
			n, err = syscall.Read(int(fd), msg)
			return true
		})
		if readErr != nil && err == nil {
			err = readErr
		}
		switch {
		case err == nil:
		case errors.Is(err, syscall.EPIPE):
			klog.Warningf("short read from kmsg; skipping")
			continue
		case errors.Is(err, syscall.EAGAIN):
			// nofollow 模式下已到环形缓冲区末尾
			return nil
		default:
			return fmt.Errorf("unexpected kmsg reading error: %w", err)
		}
		msgStr := string(msg[:n])

		message, err := p.parseMessage(msgStr)
		if err != nil {
			return fmt.Errorf("malformed kmsg message: %w", err)
		}
		msgs <- message
	}
}

func (p *parser) parseMessage(input string) (Message, error) {
	// 格式：
	//   PRIORITY,SEQUENCE_NUM,TIMESTAMP,-;MESSAGE
	parts := strings.SplitN(input, ";", 2)
	if len(parts) != 2 {
		return Message{}, fmt.Errorf("invalid kmsg; must contain a ';'")
	}

	metadata, message := parts[0], parts[1]

	metadataParts := strings.Split(metadata, ",")
	if len(metadataParts) < 3 {
		return Message{}, fmt.Errorf("invalid kmsg: must contain at least 3 ',' separated pieces at the start")
	}

	priority, sequence, timestamp := metadataParts[0], metadataParts[1], metadataParts[2]

	prioNum, err := strconv.Atoi(priority)
	if err != nil {
		return Message{}, fmt.Errorf("could not parse %q as priority: %v", priority, err)
	}

	sequenceNum, err := strconv.Atoi(sequence)
	if err != nil {
		return Message{}, fmt.Errorf("could not parse %q as sequence number: %v", priority, err)
	}

	timestampUsFromBoot, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return Message{}, fmt.Errorf("could not parse %q as timestamp: %v", priority, err)
	}
	// 时间戳是相对启动时间的微秒偏移量。
	msgTime := p.bootTime.Add(time.Duration(timestampUsFromBoot) * time.Microsecond)

	return Message{
		Priority:       prioNum,
		SequenceNumber: sequenceNum,
		Timestamp:      msgTime,
		Message:        message,
	}, nil
}
