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

package options

import (
	"context"
	"fmt"
	"net"
	"os"

	"github.com/spf13/pflag"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	"google.golang.org/grpc"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apiserver/pkg/apis/apiserver"
	"k8s.io/apiserver/pkg/apis/apiserver/install"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/egressselector"
	"k8s.io/apiserver/pkg/util/feature"
	tracing "k8s.io/component-base/tracing"
	tracingapi "k8s.io/component-base/tracing/api/v1"
	"k8s.io/utils/path"
)

// apiserverService 是 OTel resource 中的 service.name，用于在追踪后端区分组件。
const apiserverService = "apiserver"

var (
	// cfgScheme 只注册 apiserver 内部配置类型，用于解码 tracing 配置文件。
	cfgScheme = runtime.NewScheme()
	// EnableStrict 让未知字段直接报错，避免配置拼写错误被静默忽略。
	codecs = serializer.NewCodecFactory(cfgScheme, serializer.EnableStrict)
)

func init() {
	// 显式安装 noop MeterProvider，避免并不使用的 OTel 指标造成内存泄漏（否则默认全局 provider 会保留指标状态）：
	// https://github.com/open-telemetry/opentelemetry-go-contrib/issues/5190
	otel.SetMeterProvider(noop.NewMeterProvider())
}

func init() {
	install.Install(cfgScheme)
}

// TracingOptions contain configuration options for tracing
// exporters
type TracingOptions struct {
	// ConfigFile is the file path with api-server tracing configuration.
	ConfigFile string
}

// NewTracingOptions creates a new instance of TracingOptions
func NewTracingOptions() *TracingOptions {
	return &TracingOptions{}
}

// AddFlags 将与追踪相关的 flag 添加到指定 FlagSet；nil receiver 时直接返回，便于嵌入到可选的子选项中。
func (o *TracingOptions) AddFlags(fs *pflag.FlagSet) {
	if o == nil {
		return
	}

	fs.StringVar(&o.ConfigFile, "tracing-config-file", o.ConfigFile,
		"File with apiserver tracing configuration.")
}

// ApplyTo 用选项填充 Tracing 配置：未配置时保持 TracerProvider 为空（即不启用追踪）。
func (o *TracingOptions) ApplyTo(es *egressselector.EgressSelector, c *server.Config) error {
	if o == nil || o.ConfigFile == "" {
		return nil
	}

	traceConfig, err := ReadTracingConfiguration(o.ConfigFile)
	if err != nil {
		return fmt.Errorf("failed to read tracing config: %v", err)
	}

	errs := tracingapi.ValidateTracingConfiguration(traceConfig, feature.DefaultFeatureGate, nil)
	if len(errs) > 0 {
		return fmt.Errorf("failed to validate tracing configuration: %v", errs.ToAggregate())
	}

	opts := []otlptracegrpc.Option{}
	if es != nil {
		// 仅在启用 egressselector 时才使用其 dialer：追踪端点位于 ControlPlane 网络，导出流量必须走该网络，避免绕过集群的出站策略。
		egressDialer, err := es.Lookup(egressselector.ControlPlane.AsNetworkContext())
		if err != nil {
			return err
		}
		if egressDialer != nil {
			otelDialer := func(ctx context.Context, addr string) (net.Conn, error) {
				return egressDialer(ctx, "tcp", addr)
			}
			opts = append(opts, otlptracegrpc.WithDialOption(grpc.WithContextDialer(otelDialer)))
		}
	}

	// APIServerID 作为实例 ID，便于在多副本中定位具体实例的 span。
	resourceOpts := []resource.Option{
		resource.WithAttributes(
			semconv.ServiceNameKey.String(apiserverService),
			semconv.ServiceInstanceIDKey.String(c.APIServerID),
		),
	}
	tp, err := tracing.NewProvider(context.Background(), traceConfig, opts, resourceOpts)
	if err != nil {
		return err
	}
	c.TracerProvider = tp
	// 回环客户端同样需要注入 tracing 包装，否则内部调用链会断开。
	if c.LoopbackClientConfig != nil {
		c.LoopbackClientConfig.Wrap(tracing.WrapperFor(c.TracerProvider))
	}
	return nil
}

// Validate verifies flags passed to TracingOptions.
func (o *TracingOptions) Validate() (errs []error) {
	if o == nil || o.ConfigFile == "" {
		return
	}

	// 跟随符号链接检查文件存在性，与运行时读取行为保持一致。
	if exists, err := path.Exists(path.CheckFollowSymlink, o.ConfigFile); !exists {
		errs = append(errs, fmt.Errorf("tracing-config-file %s does not exist", o.ConfigFile))
	} else if err != nil {
		errs = append(errs, fmt.Errorf("error checking if tracing-config-file %s exists: %v", o.ConfigFile, err))
	}
	return
}

// ReadTracingConfiguration reads the tracing configuration from a file
func ReadTracingConfiguration(configFilePath string) (*tracingapi.TracingConfiguration, error) {
	if configFilePath == "" {
		return nil, fmt.Errorf("tracing config file was empty")
	}
	data, err := os.ReadFile(configFilePath)
	if err != nil {
		return nil, fmt.Errorf("unable to read tracing configuration from %q: %v", configFilePath, err)
	}
	internalConfig := &apiserver.TracingConfiguration{}
	// 该解码调用支持 json/yaml 等各种格式，并把所有已注册版本解码为内部版本；先解码到内部版本再对外暴露，使调用方只依赖稳定的内部类型。
	if err := runtime.DecodeInto(codecs.UniversalDecoder(), data, internalConfig); err != nil {
		return nil, fmt.Errorf("unable to decode tracing configuration data: %v", err)
	}
	return &internalConfig.TracingConfiguration, nil
}
