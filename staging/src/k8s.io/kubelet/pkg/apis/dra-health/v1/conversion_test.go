/*
Copyright The Kubernetes Authors.

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

// 本文件守护 v1 与 v1alpha1 之间手写转换的正确性：kubelet 通过转换包装同时服务两个版本，
// 一旦两个 proto 的字段或取值出现偏差，旧版本客户端拿到的健康状态就会失真。

package v1

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"path"
	"testing"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	v1alpha1 "k8s.io/kubelet/pkg/apis/dra-health/v1alpha1"
)

// sampleResponse 构造一条同时含健康与不健康设备的响应，覆盖转换时两种健康状态的取值。
func sampleResponse() *NodeWatchResourcesResponse {
	return &NodeWatchResourcesResponse{
		Devices: []*DeviceHealth{
			{
				Device:                    &DeviceIdentifier{PoolName: "pool1", DeviceName: "dev1"},
				Health:                    HealthStatus_HEALTHY,
				LastUpdatedTime:           42,
				HealthCheckTimeoutSeconds: 30,
				Message:                   "all good",
			},
			{
				Device:          &DeviceIdentifier{PoolName: "pool2", DeviceName: "dev2"},
				Health:          HealthStatus_UNHEALTHY,
				LastUpdatedTime: 7,
				Message:         "broken",
			},
		},
	}
}

// TestNodeWatchResourcesResponseToV1Alpha1 逐字段比对转换结果，
// 确保旧版本客户端拿到的设备标识与健康信息不丢失。
func TestNodeWatchResourcesResponseToV1Alpha1(t *testing.T) {
	in := sampleResponse()
	out := NodeWatchResourcesResponseToV1Alpha1(in)
	if len(out.GetDevices()) != len(in.GetDevices()) {
		t.Fatalf("device count: want %d, got %d", len(in.GetDevices()), len(out.GetDevices()))
	}
	for i, want := range in.GetDevices() {
		got := out.GetDevices()[i]
		if got.GetDevice().GetPoolName() != want.GetDevice().GetPoolName() ||
			got.GetDevice().GetDeviceName() != want.GetDevice().GetDeviceName() {
			t.Errorf("device[%d] identifier mismatch: want %v, got %v", i, want.GetDevice(), got.GetDevice())
		}
		if int32(got.GetHealth()) != int32(want.GetHealth()) {
			t.Errorf("device[%d] health: want %v, got %v", i, want.GetHealth(), got.GetHealth())
		}
		if got.GetLastUpdatedTime() != want.GetLastUpdatedTime() ||
			got.GetHealthCheckTimeoutSeconds() != want.GetHealthCheckTimeoutSeconds() ||
			got.GetMessage() != want.GetMessage() {
			t.Errorf("device[%d] field mismatch: want %+v, got %+v", i, want, got)
		}
	}
}

// TestHealthStatusValuesMatch 校验两个版本的枚举数值一致：转换代码直接做 int32 强转，
// 数值一旦错位，健康状态就会被静默解释成另一种含义。
func TestHealthStatusValuesMatch(t *testing.T) {
	// 两种枚举的数值必须一致，int32 转换才有意义。
	// 必须正确。
	//
	// 强转依赖数值相同；枚举定义顺序若被调整，这里会先于线上流量报错。
	cases := []struct {
		v1    HealthStatus
		alpha v1alpha1.HealthStatus
	}{
		{HealthStatus_UNKNOWN, v1alpha1.HealthStatus_UNKNOWN},
		{HealthStatus_HEALTHY, v1alpha1.HealthStatus_HEALTHY},
		{HealthStatus_UNHEALTHY, v1alpha1.HealthStatus_UNHEALTHY},
	}
	for _, c := range cases {
		if int32(c.v1) != int32(c.alpha) {
			t.Errorf("enum value mismatch: v1 %v=%d, v1alpha1 %v=%d", c.v1, c.v1, c.alpha, c.alpha)
		}
	}
}

// 当 v1 与 v1alpha1 的消息描述出现分歧时 requireSameSchema 失败：
// 例如一侧新增字段、另一侧没有，或字段号、名字、类型、
// 基数不同。v1alpha1 兼容服务依赖两份 proto 在线路上完全一致。
//
// 按字段号查找而不是按位置比对，才能发现“字段号变了”这类在线上最难排查的偏差。
func requireSameSchema(t *testing.T, a, b protoreflect.MessageDescriptor) {
	t.Helper()
	aFields, bFields := a.Fields(), b.Fields()
	for i := 0; i < aFields.Len(); i++ {
		fd := aFields.Get(i)
		other := bFields.ByNumber(fd.Number())
		if other == nil {
			t.Fatalf("field %d (%s) of %s has no counterpart in %s", fd.Number(), fd.Name(), a.FullName(), b.FullName())
		}
		if fd.Name() != other.Name() || fd.Kind() != other.Kind() || fd.Cardinality() != other.Cardinality() {
			t.Fatalf("field %d differs between %s (%s %s %s) and %s (%s %s %s)",
				fd.Number(),
				a.FullName(), fd.Name(), fd.Kind(), fd.Cardinality(),
				b.FullName(), other.Name(), other.Kind(), other.Cardinality())
		}
		if fd.Kind() == protoreflect.MessageKind {
			requireSameSchema(t, fd.Message(), other.Message())
		}
	}
	if aFields.Len() != bFields.Len() {
		t.Fatalf("%s has %d fields, %s has %d", a.FullName(), aFields.Len(), b.FullName(), bFields.Len())
	}
}

// TestSchemasMatch 用于发现只在 v1 proto 中加字段、
// （反向亦然）。
//
// 转换按线格式等价实现；任何一侧单独加字段都会破坏这个前提。
func TestSchemasMatch(t *testing.T) {
	requireSameSchema(t,
		(&NodeWatchResourcesRequest{}).ProtoReflect().Descriptor(),
		(&v1alpha1.NodeWatchResourcesRequest{}).ProtoReflect().Descriptor())
	requireSameSchema(t,
		(&NodeWatchResourcesResponse{}).ProtoReflect().Descriptor(),
		(&v1alpha1.NodeWatchResourcesResponse{}).ProtoReflect().Descriptor())
}

// fillMessage 通过反射把所有字段填成非零值，
// 并递归处理嵌套与重复消息。由于字段是动态发现的，
// 以后新增的字段会自动被填充，无需修改测试。
//
// 用反射遍历描述符而非写死字段，是为了让新加字段自动进入回归范围。
func fillMessage(t *testing.T, m protoreflect.Message) {
	t.Helper()
	fields := m.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		switch {
		case fd.IsMap():
			t.Fatalf("map field %s not supported by fillMessage, extend it", fd.FullName())
		case fd.IsList():
			list := m.Mutable(fd).List()
			v := list.NewElement()
			if fd.Kind() == protoreflect.MessageKind {
				fillMessage(t, v.Message())
			} else {
				v = nonZeroScalar(t, fd)
			}
			list.Append(v)
		case fd.Kind() == protoreflect.MessageKind:
			fillMessage(t, m.Mutable(fd).Message())
		default:
			m.Set(fd, nonZeroScalar(t, fd))
		}
	}
}

// nonZeroScalar 为任意标量生成一个非零值：零值字段在 protobuf 里不会被序列化，
// 用零值测试等于什么都没测。
func nonZeroScalar(t *testing.T, fd protoreflect.FieldDescriptor) protoreflect.Value {
	t.Helper()
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(true)
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(fmt.Sprintf("value-%d", fd.Number()))
	case protoreflect.BytesKind:
		return protoreflect.ValueOfBytes([]byte{byte(fd.Number())})
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return protoreflect.ValueOfInt32(int32(fd.Number()) + 1)
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return protoreflect.ValueOfInt64(int64(fd.Number()) + 1)
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return protoreflect.ValueOfUint32(uint32(fd.Number()) + 1)
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return protoreflect.ValueOfUint64(uint64(fd.Number()) + 1)
	case protoreflect.FloatKind:
		return protoreflect.ValueOfFloat32(float32(fd.Number()) + 0.5)
	case protoreflect.DoubleKind:
		return protoreflect.ValueOfFloat64(float64(fd.Number()) + 0.5)
	case protoreflect.EnumKind:
		values := fd.Enum().Values()
		return protoreflect.ValueOfEnum(values.Get(values.Len() - 1).Number())
	default:
		t.Fatalf("field %s has kind %s not supported by fillMessage, extend it", fd.FullName(), fd.Kind())
		return protoreflect.Value{}
	}
}

// TestConversionFidelity 用来发现转换代码丢字段或转错字段。
// v1 响应的每个字段都经反射填成非零值，
// 若两边都加了字段却忘了写转换，
// 若两边都加了字段却忘了写转换，测试就会失败：两份 proto 线路相同，
// 无损转换必须序列化出与输入完全相同的字节。
// 两个方向都会检查。
//
// 比较序列化字节而非结构体相等：字节相同才算真正保真，字段默认值差异不会蒙混过关。
func TestConversionFidelity(t *testing.T) {
	in := &NodeWatchResourcesResponse{}
	fillMessage(t, in.ProtoReflect())

	out := NodeWatchResourcesResponseToV1Alpha1(in)
	back := NodeWatchResourcesResponseFromV1Alpha1(out)

	opts := proto.MarshalOptions{Deterministic: true}
	inBytes, err := opts.Marshal(in)
	if err != nil {
		t.Fatalf("marshal v1: %v", err)
	}
	outBytes, err := opts.Marshal(out)
	if err != nil {
		t.Fatalf("marshal v1alpha1: %v", err)
	}
	backBytes, err := opts.Marshal(back)
	if err != nil {
		t.Fatalf("marshal round-tripped v1: %v", err)
	}
	if string(inBytes) != string(outBytes) {
		t.Errorf("conversion to v1alpha1 is not lossless:\nv1 input: %v\nv1alpha1 output: %v", in, out)
	}
	if string(inBytes) != string(backBytes) {
		t.Errorf("conversion from v1alpha1 is not lossless:\nv1 input: %v\nround-tripped v1: %v", in, back)
	}
}

// TestConversionNilSafe 守住两个边界：nil 输入要原样返回 nil，缺 DeviceIdentifier 的设备
// 不能被解引用成 panic，因为健康检查数据来自外部插件，字段可能缺失。
func TestConversionNilSafe(t *testing.T) {
	if got := NodeWatchResourcesResponseToV1Alpha1(nil); got != nil {
		t.Errorf("expected nil, got %v", got)
	}
	if got := NodeWatchResourcesResponseFromV1Alpha1(nil); got != nil {
		t.Errorf("expected nil, got %v", got)
	}
	// 设备没有 DeviceIdentifier 时不得 panic，且应保持为 nil。
	got := NodeWatchResourcesResponseToV1Alpha1(&NodeWatchResourcesResponse{Devices: []*DeviceHealth{{Health: HealthStatus_UNKNOWN}}})
	if got.GetDevices()[0].GetDevice() != nil {
		t.Errorf("expected nil device identifier, got %v", got.GetDevices()[0].GetDevice())
	}
}

// fakeV1HealthServer 是最简 v1 健康服务：发送一次响应后关闭流。
//
// 发一条就关流，正好让测试能同时验证数据转发和流结束语义。
type fakeV1HealthServer struct {
	UnimplementedDRAResourceHealthServer
}

func (f *fakeV1HealthServer) NodeWatchResources(_ *NodeWatchResourcesRequest, srv DRAResourceHealth_NodeWatchResourcesServer) error {
	return srv.Send(sampleResponse())
}

// TestV1ServerWrapper 验证通过 [V1ServerWrapper] 暴露的 v1 健康服务
// 能被真实的 v1alpha1 客户端经 gRPC 连接消费；
// 这正是 kubeletplugin 辅助包向尚不支持 v1 的 kubelet
// 提供旧版 v1alpha1 API 的方式。
//
// 走真实 unix socket 而不是直接调包装方法，是为了连 gRPC 服务注册和流转发一起验证。
func TestV1ServerWrapper(t *testing.T) {
	addr := path.Join(t.TempDir(), "drahealth.sock")
	listener, err := net.Listen("unix", addr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	v1alpha1.RegisterDRAResourceHealthServer(server, V1ServerWrapper{Server: &fakeV1HealthServer{}})
	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()

	conn, err := grpc.NewClient("unix:"+addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := v1alpha1.NewDRAResourceHealthClient(conn).NodeWatchResources(ctx, &v1alpha1.NodeWatchResourcesRequest{})
	if err != nil {
		t.Fatalf("NodeWatchResources: %v", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	want := NodeWatchResourcesResponseToV1Alpha1(sampleResponse())
	if len(resp.GetDevices()) != len(want.GetDevices()) {
		t.Fatalf("device count: want %d, got %d", len(want.GetDevices()), len(resp.GetDevices()))
	}
	for i, w := range want.GetDevices() {
		g := resp.GetDevices()[i]
		if g.GetDevice().GetPoolName() != w.GetDevice().GetPoolName() ||
			g.GetDevice().GetDeviceName() != w.GetDevice().GetDeviceName() ||
			g.GetHealth() != w.GetHealth() ||
			g.GetLastUpdatedTime() != w.GetLastUpdatedTime() ||
			g.GetHealthCheckTimeoutSeconds() != w.GetHealthCheckTimeoutSeconds() ||
			g.GetMessage() != w.GetMessage() {
			t.Errorf("device[%d] mismatch: want %+v, got %+v", i, w, g)
		}
	}

	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Errorf("expected io.EOF after server closed the stream, got %v", err)
	}
}
