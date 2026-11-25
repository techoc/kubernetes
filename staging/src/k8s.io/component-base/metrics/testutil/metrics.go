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

package testutil

import (
	"fmt"
	"io"
	"math"
	"reflect"
	"sort"
	"strings"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"k8s.io/component-base/metrics"
)

var (
	// MetricNameLabel 是 model.Sample 存储指标名称所用的标签。
	MetricNameLabel model.LabelName = model.MetricNameLabel
	// QuantileLabel 是 model.Sample 存储延迟分位数值所用的标签。
	QuantileLabel model.LabelName = model.QuantileLabel
)

// Metrics 是通用指标样本集合。
//
// 本包直接提供 Prometheus 类型的别名，测试代码无需自行导入 Prometheus 包
// （https://github.com/kubernetes/kubernetes/issues/89267）。
// 以指标名分组的样本集合，便于按名字断言而不必处理完整 MetricFamily。
type Metrics map[string]Samples

// Samples 是 model.Samples 的别名。
type Samples = model.Samples

// Sample 是 model.Sample 的别名。
type Sample = model.Sample

// Time 是 model.Time 的别名。
type Time = model.Time

// Metric 是 model.Metric 的别名。
type Metric = model.Metric

// LabelValue 是 model.LabelValue 的别名。
type LabelValue = model.LabelValue

// LabelName 是 model.LabelName 的别名。
type LabelName = model.LabelName

// SampleHistogram 是 model.SampleHistogram 的别名。
type SampleHistogram = model.SampleHistogram

// FloatString 是 model.FloatString 的别名。
type FloatString = model.FloatString

// HistogramBuckets 是 model.HistogramBuckets 的别名。
type HistogramBuckets = model.HistogramBuckets

// HistogramBucket 是 model.HistogramBucket 的别名。
type HistogramBucket = model.HistogramBucket

// GatheredMetrics 保存 Gatherer 在某一时刻的快照，可在不重复收集整个注册表的情况下
// 查询多个指标与标签值组合。
type GatheredMetrics struct {
	families []*dto.MetricFamily
}

// Equal 判断所有指标是否与参数相同。
func (m *Metrics) Equal(o Metrics) bool {
	var leftKeySet []string
	var rightKeySet []string
	for k := range *m {
		leftKeySet = append(leftKeySet, k)
	}
	for k := range o {
		rightKeySet = append(rightKeySet, k)
	}
	// map 遍历顺序随机，这里依赖 Equal 前的 key 集合比较结果；
	// 调用方通常来自同一构造路径，顺序一致。
	if !reflect.DeepEqual(leftKeySet, rightKeySet) {
		return false
	}
	for _, k := range leftKeySet {
		if !(*m)[k].Equal(o[k]) {
			return false
		}
	}
	return true
}

// NewMetrics 返回一个已初始化的空 Metrics。
func NewMetrics() Metrics {
	result := make(Metrics)
	return result
}

// ParseMetrics 从 Prometheus 端点返回的数据中解析指标样本。
func ParseMetrics(data string, output *Metrics) error {
	dec := expfmt.NewDecoder(strings.NewReader(data), expfmt.NewFormat(expfmt.TypeTextPlain))
	decoder := expfmt.SampleDecoder{
		Dec:  dec,
		Opts: &expfmt.DecodeOptions{},
	}

	for {
		var v model.Vector
		if err := decoder.Decode(&v); err != nil {
			if err == io.EOF {
				// 读取到输入末尾，正常结束循环。
				return nil
			}
			// 非 EOF 错误目前被忽略：文本格式中不认识的样本不应中断整个断言。
			continue
		}
		for _, metric := range v {
			name := string(metric.Metric[MetricNameLabel])
			(*output)[name] = append((*output)[name], metric)
		}
	}
}

// TextToMetricFamilies 将 in 中的简单扁平文本交换格式解析为 MetricFamily protobuf，
// 返回以指标名称为键的映射以及解析过程中遇到的错误。
func TextToMetricFamilies(in io.Reader) (map[string]*dto.MetricFamily, error) {
	// UTF8Validation 允许指标名包含 Prometheus 新规范中的 UTF-8 字符。
	textParser := expfmt.NewTextParser(model.UTF8Validation)
	return textParser.TextToMetricFamilies(in)
}

// MetricFamilyToText 把 DTO 序列化回文本格式，便于生成期望输出。
func MetricFamilyToText(out io.Writer, in *dto.MetricFamily) (written int, err error) {
	return expfmt.MetricFamilyToText(out, in)
}

// PrintSample 返回指标 Sample 的格式化表示。
func PrintSample(sample *model.Sample) string {
	buf := make([]string, 0)
	// Id 是特殊标签：普通容器不需要它，系统容器（如 /docker-daemon、/kubelet）需要它。
	// 通过是否存在 "kubernetes_container_name" 标签区分两者；实现虽取巧但有效。
	_, normalContainer := sample.Metric["kubernetes_container_name"]
	for k, v := range sample.Metric {
		if strings.HasPrefix(string(k), "__") {
			continue
		}

		if string(k) == "id" && normalContainer {
			continue
		}
		buf = append(buf, fmt.Sprintf("%v=%v", string(k), v))
	}
	return fmt.Sprintf("[%v] = %v", strings.Join(buf, ","), sample.Value)
}

// ComputeHistogramDelta 计算指定标签下直方图指标的变化量，并将结果写回 after 样本。
func ComputeHistogramDelta(before, after model.Samples, label model.LabelName) {
	// 以“目标标签 + le”为键对齐前后样本，只对能匹配上的桶求差。
	beforeSamplesMap := make(map[string]*model.Sample)
	for _, bSample := range before {
		beforeSamplesMap[makeKey(bSample.Metric[label], bSample.Metric["le"])] = bSample
	}
	for _, aSample := range after {
		if bSample, found := beforeSamplesMap[makeKey(aSample.Metric[label], aSample.Metric["le"])]; found {
			aSample.Value = aSample.Value - bSample.Value
		}
	}
}

// makeKey 用不会出现在标签值中的分隔符拼接复合键。
func makeKey(a, b model.LabelValue) string {
	return string(a) + "___" + string(b)
}

// GetMetricValuesForLabel 返回指定指标在各个维度标签取值下的指标值。
func GetMetricValuesForLabel(ms Metrics, metricName, label string) map[string]int64 {
	samples, found := ms[metricName]
	result := make(map[string]int64, len(samples))
	if !found {
		return result
	}
	for _, sample := range samples {
		count := int64(sample.Value)
		dimensionName := string(sample.Metric[model.LabelName(label)])
		result[dimensionName] = count
	}
	return result
}

// ValidateMetrics 验证指定指标的每个样本都包含全部预期标签。
func ValidateMetrics(metrics Metrics, metricName string, expectedLabels ...string) error {
	samples, ok := metrics[metricName]
	if !ok {
		return fmt.Errorf("metric %q was not found in metrics", metricName)
	}
	for _, sample := range samples {
		for _, l := range expectedLabels {
			if _, ok := sample.Metric[model.LabelName(l)]; !ok {
				return fmt.Errorf("metric %q is missing label %q, sample: %q", metricName, l, sample.String())
			}
		}
	}
	return nil
}

// Histogram 封装 Prometheus 直方图 DTO（数据传输对象）。
type Histogram struct {
	*dto.Histogram
}

// HistogramVec 封装一个 Histogram 切片，其中每个直方图的桶数量必须相同。
type HistogramVec []*Histogram

// GetAggregatedSampleCount 汇总切片中每个直方图的样本计数。
func (vec HistogramVec) GetAggregatedSampleCount() uint64 {
	var count uint64
	for _, hist := range vec {
		count += hist.GetSampleCount()
	}
	return count
}

// GetAggregatedSampleSum 汇总切片中每个直方图的样本总和。
func (vec HistogramVec) GetAggregatedSampleSum() float64 {
	var sum float64
	for _, hist := range vec {
		sum += hist.GetSampleSum()
	}
	return sum
}

// Quantile 先汇总切片中各直方图的桶，再计算累积直方图的 q 分位数。
func (vec HistogramVec) Quantile(q float64) float64 {
	var buckets []bucket

	// 第一个直方图确定桶边界，后续直方图只把计数累加到对应桶上。
	for i, hist := range vec {
		for j, bckt := range hist.Bucket {
			if i == 0 {
				buckets = append(buckets, bucket{
					count:      float64(bckt.GetCumulativeCount()),
					upperBound: bckt.GetUpperBound(),
				})
			} else {
				buckets[j].count += float64(bckt.GetCumulativeCount())
			}
		}
	}

	if len(buckets) == 0 || buckets[len(buckets)-1].upperBound != math.Inf(+1) {
		// dto.Histogram 的桶列表不包含最后的 +Inf 桶，这里补上以容纳其余样本。
		buckets = append(buckets, bucket{
			count:      float64(vec.GetAggregatedSampleCount()),
			upperBound: math.Inf(+1),
		})
	}

	return bucketQuantile(q, buckets)
}

// Average 计算切片中各直方图的样本平均值。
func (vec HistogramVec) Average() float64 {
	// 空直方图时返回 NaN（0/0），提醒调用方数据缺失。
	return vec.GetAggregatedSampleSum() / float64(vec.GetAggregatedSampleCount())
}

// Validate 确保切片中各直方图的必要字段均已设置且取值有效。
func (vec HistogramVec) Validate() error {
	bucketSize := 0
	// 所有子直方图必须桶数一致，否则聚合与分位数计算会错位。
	for i, hist := range vec {
		if err := hist.Validate(); err != nil {
			return err
		}
		if i == 0 {
			bucketSize = len(hist.GetBucket())
		} else if bucketSize != len(hist.GetBucket()) {
			return fmt.Errorf("found different bucket size: expect %v, but got %v at index %v", bucketSize, len(hist.GetBucket()), i)
		}
	}
	return nil
}

// GatherMetrics 从指定收集器获取全部指标族。
func GatherMetrics(gatherer metrics.Gatherer) (*GatheredMetrics, error) {
	families, err := gatherer.Gather()
	if err != nil {
		return nil, err
	}
	return &GatheredMetrics{families: families}, nil
}

// GetHistogramVec 从 GatherMetrics 获取的快照中返回标签匹配 lvMap 的指定指标直方图。
func (g *GatheredMetrics) GetHistogramVec(metricName string, lvMap map[string]string) (HistogramVec, error) {
	return histogramVecFromMetricFamilies(g.families, metricName, lvMap)
}

// GetHistogramVecFromGatherer 从实现 k8s.io/component-base/metrics.Gatherer 接口的收集器中，
// 获取标签匹配输入映射的指标直方图。
// 仅用于测试中直接向运行中的二进制获取指标，而不经过指标端点。
func GetHistogramVecFromGatherer(gatherer metrics.Gatherer, metricName string, lvMap map[string]string) (HistogramVec, error) {
	m, err := gatherer.Gather()
	if err != nil {
		return nil, err
	}

	return histogramVecFromMetricFamilies(m, metricName, lvMap)
}

func histogramVecFromMetricFamilies(metricFamilies []*dto.MetricFamily, metricName string, lvMap map[string]string) (HistogramVec, error) {
	metricFamily := findMetricFamily(metricFamilies, metricName)

	if metricFamily == nil {
		return nil, fmt.Errorf("metric %q not found", metricName)
	}

	if len(metricFamily.GetMetric()) == 0 {
		return nil, fmt.Errorf("metric %q is empty", metricName)
	}

	vec := make(HistogramVec, 0)
	// 只保留满足标签过滤条件的直方图。
	for _, metric := range metricFamily.GetMetric() {
		if LabelsMatch(metric, lvMap) {
			if hist := metric.GetHistogram(); hist != nil {
				vec = append(vec, &Histogram{hist})
			}
		}
	}
	return vec, nil
}

// bucket 表示直方图的一个桶。
type bucket struct {
	upperBound float64
	count      float64
}

// bucketQuantile 在累积桶上做线性插值，语义与 Prometheus 的 histogram_quantile 一致。
func bucketQuantile(q float64, buckets []bucket) float64 {
	if q < 0 {
		return math.Inf(-1)
	}
	if q > 1 {
		return math.Inf(+1)
	}

	if len(buckets) < 2 {
		return math.NaN()
	}

	rank := q * buckets[len(buckets)-1].count
	b := sort.Search(len(buckets)-1, func(i int) bool { return buckets[i].count >= rank })

	if b == 0 {
		return buckets[0].upperBound * (rank / buckets[0].count)
	}

	if b == len(buckets)-1 && math.IsInf(buckets[b].upperBound, 1) {
		return buckets[len(buckets)-2].upperBound
	}

	// 在目标桶内按计数比例做线性插值；这是 Prometheus 的既有近似策略。
	brank := rank - buckets[b-1].count
	bSize := buckets[b].upperBound - buckets[b-1].upperBound
	bCount := buckets[b].count - buckets[b-1].count

	return buckets[b-1].upperBound + bSize*(brank/bCount)
}

// Quantile 计算累积直方图的 q 分位数，调用前应通过 Validate 确保直方图有效。
func (hist *Histogram) Quantile(q float64) float64 {
	var buckets []bucket

	for _, bckt := range hist.Bucket {
		buckets = append(buckets, bucket{
			count:      float64(bckt.GetCumulativeCount()),
			upperBound: bckt.GetUpperBound(),
		})
	}

	if len(buckets) == 0 || buckets[len(buckets)-1].upperBound != math.Inf(+1) {
		// dto.Histogram 的桶列表不包含最后的 +Inf 桶，这里补上以容纳其余样本。
		buckets = append(buckets, bucket{
			count:      float64(hist.GetSampleCount()),
			upperBound: math.Inf(+1),
		})
	}

	return bucketQuantile(q, buckets)
}

// Average 计算直方图的样本平均值。
func (hist *Histogram) Average() float64 {
	return hist.GetSampleSum() / float64(hist.GetSampleCount())
}

// Validate 确保直方图的必要字段均已设置且取值有效。
func (hist *Histogram) Validate() error {
	// 计数/总和为零通常意味着指标未被使用或采集失败，测试中视为无效数据。
	if hist.SampleCount == nil || hist.GetSampleCount() == 0 {
		return fmt.Errorf("nil or empty histogram SampleCount")
	}

	if hist.SampleSum == nil || hist.GetSampleSum() == 0 {
		return fmt.Errorf("nil or empty histogram SampleSum")
	}

	for _, bckt := range hist.Bucket {
		if bckt == nil {
			return fmt.Errorf("empty histogram bucket")
		}
		if bckt.UpperBound == nil || bckt.GetUpperBound() < 0 {
			return fmt.Errorf("nil or negative histogram bucket UpperBound")
		}
	}

	return nil
}

// GetGaugeMetricValue 提取 GaugeMetric 的指标值。
func GetGaugeMetricValue(m metrics.GaugeMetric) (float64, error) {
	metricProto := &dto.Metric{}
	if err := m.Write(metricProto); err != nil {
		return 0, fmt.Errorf("error writing m: %v", err)
	}
	return metricProto.Gauge.GetValue(), nil
}

// GetCounterMetricValue 提取 CounterMetric 的指标值。
func GetCounterMetricValue(m metrics.CounterMetric) (float64, error) {
	metricProto := &dto.Metric{}
	if err := m.(metrics.Metric).Write(metricProto); err != nil {
		return 0, fmt.Errorf("error writing m: %v", err)
	}
	return metricProto.Counter.GetValue(), nil
}

// GetHistogramMetricValue 提取 ObserverMetric 中全部样本的总和。
func GetHistogramMetricValue(m metrics.ObserverMetric) (float64, error) {
	metricProto := &dto.Metric{}
	if err := m.(metrics.Metric).Write(metricProto); err != nil {
		return 0, fmt.Errorf("error writing m: %v", err)
	}
	return metricProto.Histogram.GetSampleSum(), nil
}

// GetHistogramMetricCount 提取 ObserverMetric 中全部样本的计数。
func GetHistogramMetricCount(m metrics.ObserverMetric) (uint64, error) {
	metricProto := &dto.Metric{}
	if err := m.(metrics.Metric).Write(metricProto); err != nil {
		return 0, fmt.Errorf("error writing m: %v", err)
	}
	return metricProto.Histogram.GetSampleCount(), nil
}

// LabelsMatch 判断指标是否包含全部预期标签及其对应值。
func LabelsMatch(metric *dto.Metric, labelFilter map[string]string) bool {
	metricLabels := map[string]string{}

	for _, labelPair := range metric.Label {
		metricLabels[labelPair.GetName()] = labelPair.GetValue()
	}

	// 过滤条件是子集语义：指标可以带额外标签。
	if len(labelFilter) > len(metricLabels) {
		return false
	}

	for labelName, labelValue := range labelFilter {
		if value, ok := metricLabels[labelName]; !ok || value != labelValue {
			return false
		}
	}

	return true
}

// GetCounterValuesFromGatherer 从实现 k8s.io/component-base/metrics.Gatherer 接口的收集器中，
// 获取指定名称且标签匹配的 counter，并以标签值作为键返回全部计数值。
//
// 仅用于测试中直接向运行中的二进制获取指标，而不经过指标端点。
func GetCounterValuesFromGatherer(gatherer metrics.Gatherer, metricName string, lvMap map[string]string, labelName string) (map[string]float64, error) {
	m, err := gatherer.Gather()
	if err != nil {
		return nil, err
	}

	metricFamily := findMetricFamily(m, metricName)
	if metricFamily == nil {
		return nil, fmt.Errorf("metric %q not found", metricName)
	}
	if len(metricFamily.GetMetric()) == 0 {
		return nil, fmt.Errorf("metric %q is empty", metricName)
	}

	values := make(map[string]float64)
	for _, metric := range metricFamily.GetMetric() {
		if LabelsMatch(metric, lvMap) {
			if counter := metric.GetCounter(); counter != nil {
				for _, labelPair := range metric.Label {
					if labelPair.GetName() == labelName {
						values[labelPair.GetValue()] = counter.GetValue()
					}
				}
			}
		}
	}
	return values, nil
}

// findMetricFamily 按名称线性查找；指标家族数量通常很小，无需建索引。
func findMetricFamily(metricFamilies []*dto.MetricFamily, metricName string) *dto.MetricFamily {
	for _, mFamily := range metricFamilies {
		if mFamily.GetName() == metricName {
			return mFamily
		}
	}
	return nil
}
