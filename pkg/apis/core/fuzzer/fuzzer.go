/*
Copyright 2017 The Kubernetes Authors.

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

package fuzzer

import (
	"reflect"
	"strconv"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/randfill"
)

// Funcs 返回 core API 组的 fuzz 函数集合，供 round-trip 测试使用。
// 各自定义函数负责生成“合法”的对象：满足字段间的同步约束、默认值和校验规则，
// 否则外部/内部版本转换往返时会出现差异。
var Funcs = func(codecs runtimeserializer.CodecFactory) []interface{} {
	return []interface{}{
		func(q *resource.Quantity, c randfill.Continue) {
			*q = *resource.NewQuantity(c.Int63n(1000), resource.DecimalExponent)
		},
		func(j *core.ObjectReference, c randfill.Continue) {
			// 对象引用的 APIVersion 与 Kind 在内存中必须保持为空（它们由序列化器填充），
			// 因此需要自定义随机化逻辑，避免 fuzz 出非空值导致往返不一致。
			j.APIVersion = c.String(0)
			j.Kind = c.String(0)
			j.Namespace = c.String(0)
			j.Name = c.String(0)
			j.ResourceVersion = strconv.FormatUint(c.Uint64(), 10)
			j.FieldPath = c.String(0)
		},
		func(j *core.PodExecOptions, c randfill.Continue) {
			j.Stdout = true
			j.Stderr = true
		},
		func(j *core.PodAttachOptions, c randfill.Continue) {
			j.Stdout = true
			j.Stderr = true
		},
		func(j *core.PodPortForwardOptions, c randfill.Continue) {
			if c.Bool() {
				j.Ports = make([]int32, c.Intn(10))
				for i := range j.Ports {
					j.Ports[i] = c.Int31n(65535)
				}
			}
		},
		func(s *core.ServiceAccountTokenProjection, c randfill.Continue) {
			c.FillNoCustom(s)
			if s.ExpirationSeconds == nil {
				// 该字段会被默认值填充，合法对象始终带有取值，fuzz 时也要补上。
				s.ExpirationSeconds = ptr.To[int64](3600)
			}
		},
		func(s *core.PodSpec, c randfill.Continue) {
			c.FillNoCustom(s)
			// 该字段有默认值，需要显式设置以免影响往返一致性。
			ttl := int64(30)
			if c.Bool() {
				ttl = int64(c.Uint32())
			}
			s.TerminationGracePeriodSeconds = &ttl

			// DeprecatedServiceAccount 是 ServiceAccountName 的旧别名，默认值填充会保证两者一致，
			// 因此这里也直接复用同一取值。
			s.DeprecatedServiceAccount = s.ServiceAccountName //nolint:staticcheck // SA1019 DeprecatedServiceAccount must be fuzzed for backward compatibility

			c.Fill(s.SecurityContext)

			if s.SecurityContext == nil {
				s.SecurityContext = new(core.PodSecurityContext)
			}
			if s.Affinity == nil {
				s.Affinity = new(core.Affinity)
			}
			if s.SchedulerName == "" {
				s.SchedulerName = v1.DefaultSchedulerName
			}
			if s.EnableServiceLinks == nil {
				enableServiceLinks := v1.DefaultEnableServiceLinks
				s.EnableServiceLinks = &enableServiceLinks
			}
		},
		func(s *core.PodStatus, c randfill.Continue) {
			c.Fill(&s)
			s.HostIPs = []core.HostIP{{IP: s.HostIP}}
			// 默认值填充会保证 PodIP 与 PodIPs[0] 同步，fuzz 时也据此构造一致的取值。
			if len(s.PodIP) > 0 {
				s.PodIPs = []core.PodIP{{IP: s.PodIP}}
			} else {
				s.PodIPs = nil
			}
		},
		func(j *core.PodPhase, c randfill.Continue) {
			statuses := []core.PodPhase{core.PodPending, core.PodRunning, core.PodFailed, core.PodUnknown}
			*j = statuses[c.Rand.Intn(len(statuses))]
		},
		func(j *core.Binding, c randfill.Continue) {
			c.Fill(&j.ObjectMeta)
			j.Target.Name = c.String(0)
		},
		func(j *core.ReplicationController, c randfill.Continue) {
			c.FillNoCustom(j)

			// 与默认值填充逻辑保持一致。
			if j.Spec.Template != nil {
				if len(j.Labels) == 0 {
					j.Labels = j.Spec.Template.Labels
				}
				if len(j.Spec.Selector) == 0 {
					j.Spec.Selector = j.Spec.Template.Labels
				}
			}
		},
		func(j *core.ReplicationControllerSpec, c randfill.Continue) {
			c.FillNoCustom(j) // 只 fuzz 自身字段，避免递归调用本函数
			//j.TemplateRef = nil // 往返测试需要该字段为 nil

			// 与默认值填充逻辑保持一致。
			if j.Replicas == nil {
				replicas := int32(0)
				j.Replicas = &replicas
			}
		},
		func(j *core.List, c randfill.Continue) {
			c.FillNoCustom(j) // 只 fuzz 自身字段，避免递归调用本函数
			// TODO: uncomment when round trip starts from a versioned object
			if false { //j.Items == nil {
				j.Items = []runtime.Object{}
			}
		},
		func(q *core.ResourceRequirements, c randfill.Continue) {
			randomQuantity := func() resource.Quantity {
				var q resource.Quantity
				c.Fill(&q)
				// 预先计算字符串，避免基准测试中把转换开销计入。
				_ = q.String()
				return q
			}
			q.Limits = make(core.ResourceList)
			q.Requests = make(core.ResourceList)
			cpuLimit := randomQuantity()
			q.Limits[core.ResourceCPU] = cpuLimit.DeepCopy()
			q.Requests[core.ResourceCPU] = cpuLimit.DeepCopy()
			memoryLimit := randomQuantity()
			q.Limits[core.ResourceMemory] = memoryLimit.DeepCopy()
			q.Requests[core.ResourceMemory] = memoryLimit.DeepCopy()
			storageLimit := randomQuantity()
			q.Limits[core.ResourceStorage] = storageLimit.DeepCopy()
			q.Requests[core.ResourceStorage] = storageLimit.DeepCopy()
		},
		func(q *core.LimitRangeItem, c randfill.Continue) {
			var cpuLimit resource.Quantity
			c.Fill(&cpuLimit)

			q.Type = core.LimitTypeContainer
			q.Default = make(core.ResourceList)
			q.Default[core.ResourceCPU] = cpuLimit.DeepCopy()

			q.DefaultRequest = make(core.ResourceList)
			q.DefaultRequest[core.ResourceCPU] = cpuLimit.DeepCopy()

			q.Max = make(core.ResourceList)
			q.Max[core.ResourceCPU] = cpuLimit.DeepCopy()

			q.Min = make(core.ResourceList)
			q.Min[core.ResourceCPU] = cpuLimit.DeepCopy()

			q.MaxLimitRequestRatio = make(core.ResourceList)
			q.MaxLimitRequestRatio[core.ResourceCPU] = resource.MustParse("10")
		},
		func(p *core.PullPolicy, c randfill.Continue) {
			policies := []core.PullPolicy{core.PullAlways, core.PullNever, core.PullIfNotPresent}
			*p = policies[c.Rand.Intn(len(policies))]
		},
		func(rp *core.RestartPolicy, c randfill.Continue) {
			policies := []core.RestartPolicy{core.RestartPolicyAlways, core.RestartPolicyNever, core.RestartPolicyOnFailure}
			*rp = policies[c.Rand.Intn(len(policies))]
		},
		// DownwardAPIVolumeFile 需要专门的 fuzz 函数：FieldRef 的 APIVersion 必须被默认值填充，
		// 否则外部/内部版本往返会失败。
		func(m *core.DownwardAPIVolumeFile, c randfill.Continue) {
			m.Path = c.String(0)
			versions := []string{"v1"}
			m.FieldRef = &core.ObjectFieldSelector{}
			m.FieldRef.APIVersion = versions[c.Rand.Intn(len(versions))]
			m.FieldRef.FieldPath = c.String(0)
			c.Fill(m.Mode)
			if m.Mode != nil {
				*m.Mode &= 0777
			}
		},
		func(s *core.SecretVolumeSource, c randfill.Continue) {
			c.FillNoCustom(s) // 只 fuzz 自身字段，避免递归调用本函数

			if c.Bool() {
				opt := c.Bool()
				s.Optional = &opt
			}
			// DefaultMode 有默认值，合法对象始终会设置它，且取值应在 0 到 0777 之间。
			var mode int32
			c.Fill(&mode)
			mode &= 0777
			s.DefaultMode = &mode
		},
		func(cm *core.ConfigMapVolumeSource, c randfill.Continue) {
			c.FillNoCustom(cm) // 只 fuzz 自身字段，避免递归调用本函数

			if c.Bool() {
				opt := c.Bool()
				cm.Optional = &opt
			}
			// DefaultMode 有默认值，合法对象始终会设置它，且取值应在 0 到 0777 之间。
			var mode int32
			c.Fill(&mode)
			mode &= 0777
			cm.DefaultMode = &mode
		},
		func(d *core.DownwardAPIVolumeSource, c randfill.Continue) {
			c.FillNoCustom(d) // 只 fuzz 自身字段，避免递归调用本函数

			// DefaultMode 有默认值，合法对象始终会设置它，且取值应在 0 到 0777 之间。
			var mode int32
			c.Fill(&mode)
			mode &= 0777
			d.DefaultMode = &mode
		},
		func(s *core.ProjectedVolumeSource, c randfill.Continue) {
			c.FillNoCustom(s) // 只 fuzz 自身字段，避免递归调用本函数

			// DefaultMode 有默认值，合法对象始终会设置它，且取值应在 0 到 0777 之间。
			var mode int32
			c.Fill(&mode)
			mode &= 0777
			s.DefaultMode = &mode
		},
		func(k *core.KeyToPath, c randfill.Continue) {
			c.FillNoCustom(k) // 只 fuzz 自身字段，避免递归调用本函数
			k.Key = c.String(0)
			k.Path = c.String(0)

			// Mode 不是必填项，但一旦设置就必须落在 0 到 0777 的范围内。
			if k.Mode != nil {
				*k.Mode &= 0777
			}
		},
		func(vs *core.VolumeSource, c randfill.Continue) {
			// 卷源的各字段是互斥的，必须且只能设置一个。
			v := reflect.ValueOf(vs).Elem()
			i := int(c.Uint64() % uint64(v.NumField()))
			t := v.Field(i).Addr()
			for v.Field(i).IsNil() {
				c.Fill(t.Interface())
			}
		},
		func(i *core.ISCSIVolumeSource, c randfill.Continue) {
			i.ISCSIInterface = c.String(0)
			if i.ISCSIInterface == "" {
				i.ISCSIInterface = "default"
			}
		},
		func(i *core.ISCSIPersistentVolumeSource, c randfill.Continue) {
			i.ISCSIInterface = c.String(0)
			if i.ISCSIInterface == "" {
				i.ISCSIInterface = "default"
			}
		},
		func(i *core.PersistentVolumeClaimSpec, c randfill.Continue) {
			// 与 pkg/apis/core/v1/defaults.go 中的默认值填充保持一致。
			volumeMode := core.PersistentVolumeMode(c.String(0))
			if volumeMode == "" {
				volumeMode = core.PersistentVolumeFilesystem
			}
			i.VolumeMode = &volumeMode
		},
		func(d *core.DNSPolicy, c randfill.Continue) {
			policies := []core.DNSPolicy{core.DNSClusterFirst, core.DNSDefault}
			*d = policies[c.Rand.Intn(len(policies))]
		},
		func(p *core.Protocol, c randfill.Continue) {
			protocols := []core.Protocol{core.ProtocolTCP, core.ProtocolUDP, core.ProtocolSCTP}
			*p = protocols[c.Rand.Intn(len(protocols))]
		},
		func(p *core.ServiceAffinity, c randfill.Continue) {
			types := []core.ServiceAffinity{core.ServiceAffinityClientIP, core.ServiceAffinityNone}
			*p = types[c.Rand.Intn(len(types))]
		},
		func(p *core.ServiceType, c randfill.Continue) {
			types := []core.ServiceType{core.ServiceTypeClusterIP, core.ServiceTypeNodePort, core.ServiceTypeLoadBalancer}
			*p = types[c.Rand.Intn(len(types))]
		},
		func(p *core.IPFamily, c randfill.Continue) {
			types := []core.IPFamily{core.IPv4Protocol, core.IPv6Protocol}
			selected := types[c.Rand.Intn(len(types))]
			*p = selected
		},
		func(p *core.ServiceExternalTrafficPolicy, c randfill.Continue) {
			types := []core.ServiceExternalTrafficPolicy{core.ServiceExternalTrafficPolicyCluster, core.ServiceExternalTrafficPolicyLocal}
			*p = types[c.Rand.Intn(len(types))]
		},
		func(p *core.ServiceInternalTrafficPolicy, c randfill.Continue) {
			types := []core.ServiceInternalTrafficPolicy{core.ServiceInternalTrafficPolicyCluster, core.ServiceInternalTrafficPolicyLocal}
			*p = types[c.Rand.Intn(len(types))]
		},
		func(ct *core.Container, c randfill.Continue) {
			c.FillNoCustom(ct)                                          // 只 fuzz 自身字段，避免递归调用本函数
			ct.TerminationMessagePath = "/" + ct.TerminationMessagePath // 该字段不能为空
			ct.TerminationMessagePolicy = "File"
		},
		func(ep *core.EphemeralContainer, c randfill.Continue) {
			c.FillNoCustom(ep)                                                                   // 只 fuzz 自身字段，避免递归调用本函数
			ep.EphemeralContainerCommon.TerminationMessagePath = "/" + ep.TerminationMessagePath // 该字段不能为空
			ep.EphemeralContainerCommon.TerminationMessagePolicy = "File"
		},
		func(p *core.Probe, c randfill.Continue) {
			c.FillNoCustom(p)
			// 这些字段都有默认值（为 0 时会被默认填充为 1）。
			intFieldsWithDefaults := [...]string{"TimeoutSeconds", "PeriodSeconds", "SuccessThreshold", "FailureThreshold"}
			v := reflect.ValueOf(p).Elem()
			for _, field := range intFieldsWithDefaults {
				f := v.FieldByName(field)
				if f.Int() == 0 {
					f.SetInt(1)
				}
			}
		},
		func(ev *core.EnvVar, c randfill.Continue) {
			ev.Name = c.String(0)
			if c.Bool() {
				ev.Value = c.String(0)
			} else {
				ev.ValueFrom = &core.EnvVarSource{}
				ev.ValueFrom.FieldRef = &core.ObjectFieldSelector{}

				versions := []schema.GroupVersion{
					{Group: "admission.k8s.io", Version: "v1alpha1"},
					{Group: "apps", Version: "v1beta1"},
					{Group: "apps", Version: "v1beta2"},
					{Group: "foo", Version: "v42"},
				}

				ev.ValueFrom.FieldRef.APIVersion = versions[c.Rand.Intn(len(versions))].String()
				ev.ValueFrom.FieldRef.FieldPath = c.String(0)
			}
		},
		func(ev *core.EnvFromSource, c randfill.Continue) {
			if c.Bool() {
				ev.Prefix = "p_"
			}
			if c.Bool() {
				c.Fill(&ev.ConfigMapRef)
			} else {
				c.Fill(&ev.SecretRef)
			}
		},
		func(cm *core.ConfigMapEnvSource, c randfill.Continue) {
			c.FillNoCustom(cm) // 只 fuzz 自身字段，避免递归调用本函数
			if c.Bool() {
				opt := c.Bool()
				cm.Optional = &opt
			}
		},
		func(s *core.SecretEnvSource, c randfill.Continue) {
			c.FillNoCustom(s) // 只 fuzz 自身字段，避免递归调用本函数
		},
		func(sc *core.SecurityContext, c randfill.Continue) {
			c.FillNoCustom(sc) // 只 fuzz 自身字段，避免递归调用本函数
			if c.Bool() {
				priv := c.Bool()
				sc.Privileged = &priv
			}

			if c.Bool() {
				sc.Capabilities = &core.Capabilities{
					Add:  make([]core.Capability, 0),
					Drop: make([]core.Capability, 0),
				}
				c.Fill(&sc.Capabilities.Add)
				c.Fill(&sc.Capabilities.Drop)
			}
		},
		func(s *core.Secret, c randfill.Continue) {
			c.FillNoCustom(s) // 只 fuzz 自身字段，避免递归调用本函数
			s.Type = core.SecretTypeOpaque
		},
		func(r *core.RBDVolumeSource, c randfill.Continue) {
			r.RBDPool = c.String(0)
			if r.RBDPool == "" {
				r.RBDPool = "rbd"
			}
			r.RadosUser = c.String(0)
			if r.RadosUser == "" {
				r.RadosUser = "admin"
			}
			r.Keyring = c.String(0)
			if r.Keyring == "" {
				r.Keyring = "/etc/ceph/keyring"
			}
		},
		func(r *core.RBDPersistentVolumeSource, c randfill.Continue) {
			r.RBDPool = c.String(0)
			if r.RBDPool == "" {
				r.RBDPool = "rbd"
			}
			r.RadosUser = c.String(0)
			if r.RadosUser == "" {
				r.RadosUser = "admin"
			}
			r.Keyring = c.String(0)
			if r.Keyring == "" {
				r.Keyring = "/etc/ceph/keyring"
			}
		},
		func(obj *core.HostPathVolumeSource, c randfill.Continue) {
			c.FillNoCustom(obj)
			types := []core.HostPathType{core.HostPathUnset, core.HostPathDirectoryOrCreate, core.HostPathDirectory,
				core.HostPathFileOrCreate, core.HostPathFile, core.HostPathSocket, core.HostPathCharDev, core.HostPathBlockDev}
			typeVol := types[c.Rand.Intn(len(types))]
			if obj.Type == nil {
				obj.Type = &typeVol
			}
		},
		func(pv *core.PersistentVolume, c randfill.Continue) {
			c.FillNoCustom(pv) // 只 fuzz 自身字段，避免递归调用本函数
			types := []core.PersistentVolumePhase{core.VolumeAvailable, core.VolumePending, core.VolumeBound, core.VolumeReleased, core.VolumeFailed}
			pv.Status.Phase = types[c.Rand.Intn(len(types))]
			pv.Status.Message = c.String(0)
			reclamationPolicies := []core.PersistentVolumeReclaimPolicy{core.PersistentVolumeReclaimRecycle, core.PersistentVolumeReclaimRetain}
			pv.Spec.PersistentVolumeReclaimPolicy = reclamationPolicies[c.Rand.Intn(len(reclamationPolicies))]
			volumeModes := []core.PersistentVolumeMode{core.PersistentVolumeFilesystem, core.PersistentVolumeBlock}
			pv.Spec.VolumeMode = &volumeModes[c.Rand.Intn(len(volumeModes))]
		},
		func(pvc *core.PersistentVolumeClaim, c randfill.Continue) {
			c.FillNoCustom(pvc) // 只 fuzz 自身字段，避免递归调用本函数
			types := []core.PersistentVolumeClaimPhase{core.ClaimBound, core.ClaimPending, core.ClaimLost}
			pvc.Status.Phase = types[c.Rand.Intn(len(types))]
			volumeModes := []core.PersistentVolumeMode{core.PersistentVolumeFilesystem, core.PersistentVolumeBlock}
			pvc.Spec.VolumeMode = &volumeModes[c.Rand.Intn(len(volumeModes))]
		},
		func(obj *core.AzureDiskVolumeSource, c randfill.Continue) {
			if obj.CachingMode == nil {
				obj.CachingMode = new(core.AzureDataDiskCachingMode)
				*obj.CachingMode = core.AzureDataDiskCachingReadWrite
			}
			if obj.Kind == nil {
				obj.Kind = new(core.AzureDataDiskKind)
				*obj.Kind = core.AzureSharedBlobDisk
			}
			if obj.FSType == nil {
				obj.FSType = new(string)
				*obj.FSType = "ext4"
			}
			if obj.ReadOnly == nil {
				obj.ReadOnly = new(bool)
				*obj.ReadOnly = false
			}
		},
		func(sio *core.ScaleIOVolumeSource, c randfill.Continue) {
			sio.StorageMode = c.String(0)
			if sio.StorageMode == "" {
				sio.StorageMode = "ThinProvisioned"
			}
			sio.FSType = c.String(0)
			if sio.FSType == "" {
				sio.FSType = "xfs"
			}
		},
		func(sio *core.ScaleIOPersistentVolumeSource, c randfill.Continue) {
			sio.StorageMode = c.String(0)
			if sio.StorageMode == "" {
				sio.StorageMode = "ThinProvisioned"
			}
			sio.FSType = c.String(0)
			if sio.FSType == "" {
				sio.FSType = "xfs"
			}
		},
		func(s *core.NamespaceSpec, c randfill.Continue) {
			s.Finalizers = []core.FinalizerName{core.FinalizerKubernetes}
		},
		func(s *core.Namespace, c randfill.Continue) {
			c.FillNoCustom(s) // 只 fuzz 自身字段，避免递归调用本函数
			// 与 name 到 label 的默认值填充逻辑保持一致。
			if len(s.Name) > 0 {
				if s.Labels == nil {
					s.Labels = map[string]string{}
				}
				s.Labels["kubernetes.io/metadata.name"] = s.Name
			}
		},
		func(s *core.NamespaceStatus, c randfill.Continue) {
			s.Phase = core.NamespaceActive
		},
		func(http *core.HTTPGetAction, c randfill.Continue) {
			c.FillNoCustom(http)            // 只 fuzz 自身字段，避免递归调用本函数
			http.Path = "/" + http.Path     // 路径不能为空
			http.Scheme = "x" + http.Scheme // 协议不能为空
		},
		func(ss *core.ServiceSpec, c randfill.Continue) {
			c.FillNoCustom(ss) // 只 fuzz 自身字段，避免递归调用本函数
			if len(ss.Ports) == 0 {
				// Service 至少要有一个端口，否则对象不合法。
				ss.Ports = append(ss.Ports, core.ServicePort{})
				c.Fill(&ss.Ports[0])
			}
			for i := range ss.Ports {
				switch ss.Ports[i].TargetPort.Type {
				case intstr.Int:
					ss.Ports[i].TargetPort.IntVal = 1 + ss.Ports[i].TargetPort.IntVal%65535 // 端口号不能为 0
				case intstr.String:
					ss.Ports[i].TargetPort.StrVal = "x" + ss.Ports[i].TargetPort.StrVal // 命名端口不能为空
				}
			}
			types := []core.ServiceAffinity{core.ServiceAffinityNone, core.ServiceAffinityClientIP}
			ss.SessionAffinity = types[c.Rand.Intn(len(types))]
			switch ss.SessionAffinity {
			case core.ServiceAffinityClientIP:
				timeoutSeconds := int32(c.Rand.Intn(int(core.MaxClientIPServiceAffinitySeconds)))
				ss.SessionAffinityConfig = &core.SessionAffinityConfig{
					ClientIP: &core.ClientIPConfig{
						TimeoutSeconds: &timeoutSeconds,
					},
				}
			case core.ServiceAffinityNone:
				ss.SessionAffinityConfig = nil
			}
			if ss.AllocateLoadBalancerNodePorts == nil {
				ss.AllocateLoadBalancerNodePorts = ptr.To(true)
			}
		},
		func(s *core.NodeStatus, c randfill.Continue) {
			c.FillNoCustom(s)
			s.Allocatable = s.Capacity
		},
		func(e *core.Event, c randfill.Continue) {
			c.FillNoCustom(e)
			e.EventTime = metav1.MicroTime{Time: time.Unix(1, 1000)}
			if e.Series != nil {
				e.Series.LastObservedTime = metav1.MicroTime{Time: time.Unix(3, 3000)}
			}
		},
		func(j *core.GRPCAction, c randfill.Continue) {
			empty := ""
			if j.Service == nil {
				j.Service = &empty
			}
			switch c.Intn(3) {
			case 0:
				j.Mode = nil
			case 1:
				mode := core.GRPCProbeModePlaintext
				j.Mode = &mode
			default:
				mode := core.GRPCProbeModeTLS
				j.Mode = &mode
			}
		},
		func(j *core.LoadBalancerStatus, c randfill.Continue) {
			ipMode := core.LoadBalancerIPModeVIP
			for i := range j.Ingress {
				if j.Ingress[i].IPMode == nil {
					j.Ingress[i].IPMode = &ipMode
				}
			}
		},
	}
}
