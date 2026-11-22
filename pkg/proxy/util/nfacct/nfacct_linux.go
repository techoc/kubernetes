//go:build linux

/*
Copyright 2024 The Kubernetes Authors.

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

package nfacct

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"syscall"

	"github.com/vishvananda/netlink/nl"
	"golang.org/x/sys/unix"

	"k8s.io/client-go/util/retry"
	"k8s.io/kubernetes/pkg/proxy/util"
)

// MaxLength 表示 nfacct 计数器名称允许的最大长度。
const MaxLength = 31

// nf netlink nfacct 命令字，必须与内核头文件中的定义严格一致。
// (definition: https://github.com/torvalds/linux/blob/v6.7/include/uapi/linux/netfilter/nfnetlink_acct.h#L9-L16)
const (
	// NFNL_MSG_ACCT_NEW
	cmdNew = 0
	// NFNL_MSG_ACCT_GET
	cmdGet = 1
)

// nf netlink nfacct 属性类型，必须与内核头文件中的定义严格一致。
// (definition: https://github.com/torvalds/linux/blob/v6.7/include/uapi/linux/netfilter/nfnetlink_acct.h#L24-L35)
const (
	// NFACCT_NAME
	attrName = 1
	// NFACCT_PKTS
	attrPackets = 2
	// NFACCT_BYTES
	attrBytes = 3
)

// runner 实现 Interface，所有 netlink 通信都委托给 handler 完成。
type runner struct {
	handler handler
}

// New 返回一个新的 Interface。若内核不支持 netfilter_nfacct 子系统则返回错误。
func New() (Interface, error) {
	hndlr, err := newNetlinkHandler()
	if err != nil {
		return nil, err
	}

	rnr, err := newInternal(hndlr)
	if err != nil {
		return nil, err
	}

	// 通过尝试读取计数器来探测当前内核是否支持 nfacct：
	// 支持时 GET 要么成功，要么因计数器不存在返回 ENOENT。
	_, err = rnr.Get("IMayExist")
	if err != nil && !errors.Is(err, ErrObjectNotFound) {
		return nil, ErrNotSupported
	}
	return rnr, nil
}

// newInternal 使用给定的 handler 构造 Interface，便于测试时注入模拟实现。
func newInternal(hndlr handler) (Interface, error) {
	return &runner{handler: hndlr}, nil

}

// Ensure 确保指定名称的计数器存在，不存在时创建。
func (r *runner) Ensure(name string) error {
	// 已存在的计数器直接视为满足要求，幂等创建避免重复 Add 返回 EEXIST。
	counter, err := r.Get(name)
	if counter != nil {
		return nil
	}

	if err != nil && errors.Is(err, ErrObjectNotFound) {
		return handleError(r.Add(name))
	} else if err != nil {
		return handleError(err)
	} else {
		return ErrUnexpected
	}
}

// Add 创建一个指定名称的 nfacct 计数器。
func (r *runner) Add(name string) error {
	if name == "" {
		return ErrEmptyName
	}
	// 内核的 NFACCT_NAME 属性使用零终止字符串，超长名称会被截断或拒绝。
	if len(name) > MaxLength {
		return ErrNameExceedsMaxLength
	}

	req := r.handler.newRequest(cmdNew, unix.NLM_F_REQUEST|unix.NLM_F_CREATE|unix.NLM_F_ACK)
	req.AddData(nl.NewRtAttr(attrName, nl.ZeroTerminated(name)))
	_, err := req.Execute(unix.NETLINK_NETFILTER, 0)
	if err != nil {
		return handleError(err)
	}
	return nil
}

// Get 按名称查询 nfacct 计数器，计数器不存在时返回 ErrObjectNotFound。
func (r *runner) Get(name string) (*Counter, error) {
	if len(name) > MaxLength {
		return nil, ErrNameExceedsMaxLength
	}

	req := r.handler.newRequest(cmdGet, unix.NLM_F_REQUEST|unix.NLM_F_ACK)
	req.AddData(nl.NewRtAttr(attrName, nl.ZeroTerminated(name)))
	msgs, err := req.Execute(unix.NETLINK_NETFILTER, 0)
	if err != nil {
		return nil, handleError(err)
	}

	var counter *Counter
	for _, msg := range msgs {
		counter, err = decode(msg, true)
		if err != nil {
			return nil, handleError(err)
		}
	}
	return counter, nil
}

// List 列出所有 nfacct 计数器；转储过程被信号打断时，返回已收到的部分结果。
func (r *runner) List() ([]*Counter, error) {
	var err error
	var msgs [][]byte
	// DUMP 可能被 EINTR 中断，此时重试；其他错误不做重试。
	err = retry.OnError(util.MaxAttemptsEINTR, util.ShouldRetryOnEINTR, func() error {
		req := r.handler.newRequest(cmdGet, unix.NLM_F_REQUEST|unix.NLM_F_DUMP)
		msgs, err = req.Execute(unix.NETLINK_NETFILTER, 0)
		return err
	})

	if err != nil && !errors.Is(err, unix.EINTR) {
		return nil, handleError(err)
	}

	counters := make([]*Counter, 0)
	for _, msg := range msgs {
		counter, err := decode(msg, true)
		if err != nil {
			return nil, handleError(err)
		}
		counters = append(counters, counter)
	}
	return counters, err
}

// ErrObjectNotFound 表示计数器不存在，由内核的 ENOENT 转换而来。
var ErrObjectNotFound = errors.New("object not found")

// ErrObjectAlreadyExists 表示计数器已存在，由内核的 EBUSY 转换而来。
var ErrObjectAlreadyExists = errors.New("object already exists")

// ErrNameExceedsMaxLength 表示名称超过 MaxLength 限制。
var ErrNameExceedsMaxLength = fmt.Errorf("object name exceeds the maximum allowed length of %d characters", MaxLength)

// ErrEmptyName 表示计数器名称为空。
var ErrEmptyName = errors.New("object name cannot be empty")

// ErrUnexpected 用于包装未能归类到已知内核错误码的错误。
var ErrUnexpected = errors.New("unexpected error")

// ErrNotSupported 表示当前内核不支持 nfacct 子系统。
var ErrNotSupported = errors.New("nfacct sub-system not available")

// handleError 将内核返回的系统错误映射为包内定义的可判别错误，便于调用方用 errors.Is 判断。
func handleError(err error) error {
	switch {
	case err == nil:
		return nil
	// ENOENT 表示计数器不存在，探测内核支持情况时属于正常结果。
	case errors.Is(err, syscall.ENOENT):
		return ErrObjectNotFound
	// 内核用 EBUSY 表示同名计数器已经存在。
	case errors.Is(err, syscall.EBUSY):
		return ErrObjectAlreadyExists
	default:
		return fmt.Errorf("%s: %s", ErrUnexpected.Error(), err.Error())
	}
}

// decode 解析 netlink 字节流并构造 Counter。strict 在生产环境中必须为 true，
// 仅测试时可传 false：严格模式下若名称、包数、字节数三个属性有任一未处理完，
// 就认为字节流解析失败并返回错误。
//
// netlink 消息格式参见：
// https://docs.kernel.org/userspace-api/netlink/intro.html
//
// 消息组成：
//   - netfilter 通用消息 [4 字节]
//     struct nfgenmsg（定义：https://github.com/torvalds/linux/blob/v6.7/include/uapi/linux/netfilter/nfnetlink.h#L32-L38）
//   - 属性 [变长，每个属性起始处需按 4 字节对齐]
//     struct nlattr（定义：https://github.com/torvalds/linux/blob/v6.7/include/uapi/linux/netlink.h#L220-L232）
//
// 属性组成：
//   - 长度 [2 字节]
//     包含长度字段自身、类型字段以及不含填充的实际数据所占的字节数。
//   - 类型 [2 字节]
//   - 数据 [变长]
//   - 填充 [可选]
//
// 示例：Counter{Name: "dummy-metric", Packets: 123, Bytes: 54321} 对应的 netlink 消息：
//
//	struct nfgenmsg{
//	    __u8  nfgen_family: AF_NETLINK
//	    __u8    version:    nl.NFNETLINK_V0
//	    __be16  res_id:     nl.NFNETLINK_V0
//	}
//
//	struct nlattr{
//	    __u16 nla_len:      13
//	    __u16 nla_type:     NFACCT_NAME
//	    char data:          dummy-metric\0
//	}
//
//	(padding:)
//	    data:               \0\0\0
//
//	struct nlattr{
//	    __u16 nla_len:      12
//	    __u16 nla_type:     NFACCT_PKTS
//	    __u64: data:        123
//	}
//
//	struct nlattr{
//	    __u16 nla_len:      12
//	    __u16 nla_type:     NFACCT_BYTES
//	    __u64: data:        54321
//	}
func decode(msg []byte, strict bool) (*Counter, error) {
	counter := &Counter{}
	reader := bytes.NewReader(msg)
	// 前 4 字节是与计数器内容无关的 netfilter 通用消息头，必须跳过。
	if _, err := reader.Seek(nl.SizeofNfgenmsg, io.SeekCurrent); err != nil {
		return nil, err
	}

	// 记录已处理的必需属性个数，严格模式下据此判断字节流是否完整。
	var attrsProcessed int

	// netlink 属性的长度与类型各占 2 字节。
	var length, attrType uint16

	// 跳过通用消息头后剩下的都是 struct nlattr 属性，逐个解析以填充 Counter。
	for reader.Len() > 0 {
		// 属性采用 LTV（长度、类型、值）格式，顺序读取即可定位下一个属性。

		// 第 1 步：读取 2 字节长度。
		if err := binary.Read(reader, binary.NativeEndian, &length); err != nil {
			return nil, err
		}

		// 第 2 步：读取 2 字节类型。
		if err := binary.Read(reader, binary.NativeEndian, &attrType); err != nil {
			return nil, err
		}

		// 第 3 步：修正长度。nla_len 包含刚读过的 4 字节头部，
		// 因此数据部分实际长度要减去 4。
		length -= 4

		// 第 4 步：读取变长的值。只有先根据属性类型确定数据类型，才能正确解码，
		// 变长数据按第 3 步得到的修正长度分配空间。
		switch attrType {
		case attrName:
			// NFACCT_NAME 是变长字段，按修正后的长度读取；末尾的零终止符不属于名称。
			data := make([]byte, length)
			if err := binary.Read(reader, binary.NativeEndian, data); err != nil {
				return nil, err
			}
			// 去掉内核附加的零终止符，否则名称比较会失败。
			counter.Name = string(data[:length-1])
			attrsProcessed++
		case attrPackets:
			// NFACCT_PKTS 固定为 8 字节，按网络字节序读成 uint64。
			if err := binary.Read(reader, binary.BigEndian, &counter.Packets); err != nil {
				return nil, err
			}
			attrsProcessed++
		case attrBytes:
			// NFACCT_BYTES 固定为 8 字节，按网络字节序读成 uint64。
			if err := binary.Read(reader, binary.BigEndian, &counter.Bytes); err != nil {
				return nil, err
			}
			attrsProcessed++
		default:
			// 未知属性只跳过其值，保证后续属性仍能从正确偏移开始解析。
			if _, err := reader.Seek(int64(length), io.SeekCurrent); err != nil {
				return nil, err
			}
		}

		// 每个属性都按 4 字节对齐，例如长度为 9 时需跳过 3 字节填充，
		// 否则下一个属性会被从填充字节开始误读。
		// （参考：https://github.com/torvalds/linux/blob/v6.7/include/uapi/linux/netlink.h#L220-L227）
		if length%4 != 0 {
			padding := 4 - length%4
			if _, err := reader.Seek(int64(padding), io.SeekCurrent); err != nil {
				return nil, err
			}
		}
	}

	// 严格模式下必须收齐三个属性，否则消息不完整，不能交给调用方使用。
	if strict && attrsProcessed != 3 {
		return nil, errors.New("failed to decode byte-stream")
	}
	return counter, nil
}
