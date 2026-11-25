/*
Copyright 2016 The Kubernetes Authors.

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

package version

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	apimachineryversion "k8s.io/apimachinery/pkg/version"
)

// Version 是版本号的不透明表示。
type Version struct {
	components    []uint
	semver        bool
	preRelease    string
	buildMetadata string
}

var (
	// versionMatchRE 把版本字符串拆分为数字部分与 “额外” 部分。
	versionMatchRE = regexp.MustCompile(`^\s*v?([0-9]+(?:\.[0-9]+)*)(.*)*$`)
	// extraMatchRE 把 versionMatchRE 的 “额外” 部分拆分为 semver 的 pre-release 与 build metadata；
	// 它不校验 pre-release 的 “无前导零” 约束（由 parse 另行检查）。
	extraMatchRE = regexp.MustCompile(`^(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?\s*$`)
)

// parse 解析版本字符串。semver 为 true 时要求严格的 X.Y.Z 形式并解析 pre-release/build metadata；
// 为 false 时只要求至少两段数字，其余内容忽略。
func parse(str string, semver bool) (*Version, error) {
	parts := versionMatchRE.FindStringSubmatch(str)
	if parts == nil {
		return nil, fmt.Errorf("could not parse %q as version", str)
	}
	numbers, extra := parts[1], parts[2]

	components := strings.Split(numbers, ".")
	if (semver && len(components) != 3) || (!semver && len(components) < 2) {
		return nil, fmt.Errorf("illegal version string %q", str)
	}

	v := &Version{
		components: make([]uint, len(components)),
		semver:     semver,
	}
	for i, comp := range components {
		// 主版本号（以及 semver 的所有段）不允许前导零。
		if (i == 0 || semver) && strings.HasPrefix(comp, "0") && comp != "0" {
			return nil, fmt.Errorf("illegal zero-prefixed version component %q in %q", comp, str)
		}
		num, err := strconv.ParseUint(comp, 10, 0)
		if err != nil {
			return nil, fmt.Errorf("illegal non-numeric version component %q in %q: %v", comp, str, err)
		}
		v.components[i] = uint(num)
	}

	if semver && extra != "" {
		extraParts := extraMatchRE.FindStringSubmatch(extra)
		if extraParts == nil {
			return nil, fmt.Errorf("could not parse pre-release/metadata (%s) in version %q", extra, str)
		}
		v.preRelease, v.buildMetadata = extraParts[1], extraParts[2]

		// 纯数字的 pre-release 段同样不允许前导零（如 "01" 非法）。
		for _, comp := range strings.Split(v.preRelease, ".") {
			if _, err := strconv.ParseUint(comp, 10, 0); err == nil {
				if strings.HasPrefix(comp, "0") && comp != "0" {
					return nil, fmt.Errorf("illegal zero-prefixed version component %q in %q", comp, str)
				}
			}
		}
	}

	return v, nil
}

// HighestSupportedVersion 返回受支持的最高版本；假定最高支持版本必须是 v1.x。
// 会跳过解析失败与 major > 1 的版本；找不到任何 v1.x 版本时返回错误。
func HighestSupportedVersion(versions []string) (*Version, error) {
	if len(versions) == 0 {
		return nil, errors.New("empty array for supported versions")
	}

	var (
		highestSupportedVersion *Version
		theErr                  error
	)

	// 逆序遍历，尽量保留首个解析错误用于最终报错。
	for i := len(versions) - 1; i >= 0; i-- {
		currentHighestVer, err := ParseGeneric(versions[i])
		if err != nil {
			theErr = err
			continue
		}

		// 只接受 v1.x；更高的 major 不在支持范围内。
		if currentHighestVer.Major() > 1 {
			continue
		}

		if highestSupportedVersion == nil || highestSupportedVersion.LessThan(currentHighestVer) {
			highestSupportedVersion = currentHighestVer
		}
	}

	if highestSupportedVersion == nil {
		return nil, fmt.Errorf(
			"could not find a highest supported version from versions (%v) reported: %+v",
			versions, theErr)
	}

	if highestSupportedVersion.Major() != 1 {
		return nil, fmt.Errorf("highest supported version reported is %v, must be v1.x", highestSupportedVersion)
	}

	return highestSupportedVersion, nil
}

// ParseGeneric 解析 “通用” 版本字符串：至少两段以点分隔的数字（第一段不能有前导零），
// 其后可跟任意不解释的数据（与最后一个数字段之间不要求标点分隔）。
// 为方便使用，忽略首尾空白，且版本前可带字母 "v"。另见 ParseSemantic。
func ParseGeneric(str string) (*Version, error) {
	return parse(str, false)
}

// MustParseGeneric 同 ParseGeneric，但解析失败时 panic。
func MustParseGeneric(str string) *Version {
	v, err := ParseGeneric(str)
	if err != nil {
		panic(err)
	}
	return v
}

// Parse 优先按 ParseSemantic 解析以保留更多信息；失败时回退到 ParseGeneric。
func Parse(str string) (*Version, error) {
	v, err := parse(str, true)
	if err != nil {
		return parse(str, false)
	}
	return v, err
}

// MustParse 同 Parse，但解析失败时 panic。
func MustParse(str string) *Version {
	v, err := Parse(str)
	if err != nil {
		panic(err)
	}
	return v
}

// ParseMajorMinor 解析 “通用” 版本字符串，并返回只含 major/minor 的版本。
func ParseMajorMinor(str string) (*Version, error) {
	v, err := ParseGeneric(str)
	if err != nil {
		return nil, err
	}
	return MajorMinor(v.Major(), v.Minor()), nil
}

// MustParseMajorMinor 同 ParseMajorMinor，但解析失败时 panic。
func MustParseMajorMinor(str string) *Version {
	v, err := ParseMajorMinor(str)
	if err != nil {
		panic(err)
	}
	return v
}

// ParseSemantic 解析严格遵循 Semantic Versioning 规范（http://semver.org/）的版本字符串
// （忽略首尾空白，并允许前缀 "v"）。不保证符合 semver 语法的版本请用 ParseGeneric。
func ParseSemantic(str string) (*Version, error) {
	return parse(str, true)
}

// MustParseSemantic 同 ParseSemantic，但解析失败时 panic。
func MustParseSemantic(str string) *Version {
	v, err := ParseSemantic(str)
	if err != nil {
		panic(err)
	}
	return v
}

// MajorMinor 返回由给定 major/minor 构成的版本（无 patch、非 semver）。
func MajorMinor(major, minor uint) *Version {
	return &Version{components: []uint{major, minor}}
}

// Major 返回主版本号。
func (v *Version) Major() uint {
	return v.components[0]
}

// Minor 返回次版本号。
func (v *Version) Minor() uint {
	return v.components[1]
}

// Patch 在 v 为语义化版本时返回修订号，否则返回 0。
func (v *Version) Patch() uint {
	if len(v.components) < 3 {
		return 0
	}
	return v.components[2]
}

// BuildMetadata 在 v 为语义化版本时返回 build metadata，否则返回空串。
func (v *Version) BuildMetadata() string {
	return v.buildMetadata
}

// PreRelease 在 v 为语义化版本时返回 pre-release 信息，否则返回空串。
func (v *Version) PreRelease() string {
	return v.preRelease
}

// Components 返回版本号的各数字段。
func (v *Version) Components() []uint {
	return v.components
}

// WithMajor 返回把主版本号替换为指定值的版本副本（含 patch 段）。
func (v *Version) WithMajor(major uint) *Version {
	result := *v
	result.components = []uint{major, v.Minor(), v.Patch()}
	return &result
}

// WithMinor 返回把次版本号替换为指定值的版本副本（含 patch 段）。
func (v *Version) WithMinor(minor uint) *Version {
	result := *v
	result.components = []uint{v.Major(), minor, v.Patch()}
	return &result
}

// OffsetMinor 返回在原 minor 上加上 offset 的版本（major 不变、无 patch 段）。
// 若 -offset >= 当前 minor，则 minor 取 0。
func (v *Version) OffsetMinor(offset int) *Version {
	var minor uint
	if offset >= 0 {
		minor = v.Minor() + uint(offset)
	} else {
		diff := uint(-offset)
		if diff < v.Minor() {
			minor = v.Minor() - diff
		}
	}
	return MajorMinor(v.Major(), minor)
}

// SubtractMinor 返回向前回退 diff 个 minor 的版本（major 不变、无 patch 段）。
// 若 diff >= 当前 minor，则 minor 取 0。
func (v *Version) SubtractMinor(diff uint) *Version {
	return v.OffsetMinor(-int(diff))
}

// AddMinor 返回向后前进 diff 个 minor 的版本（major 不变、无 patch 段）。
func (v *Version) AddMinor(diff uint) *Version {
	return v.OffsetMinor(int(diff))
}

// WithPatch 返回把 patch 号替换为指定值的版本副本。
func (v *Version) WithPatch(patch uint) *Version {
	result := *v
	result.components = []uint{v.Major(), v.Minor(), patch}
	return &result
}

// WithPreRelease 返回把 pre-release 替换为指定值的版本副本；传入空串时原样返回。
func (v *Version) WithPreRelease(preRelease string) *Version {
	if len(preRelease) == 0 {
		return v
	}
	result := *v
	result.components = []uint{v.Major(), v.Minor(), v.Patch()}
	result.preRelease = preRelease
	return &result
}

// WithBuildMetadata 返回把 build metadata 替换为指定值的版本副本。
func (v *Version) WithBuildMetadata(buildMetadata string) *Version {
	result := *v
	result.components = []uint{v.Major(), v.Minor(), v.Patch()}
	result.buildMetadata = buildMetadata
	return &result
}

// String 把 Version 转回字符串；注意经 ParseGeneric 解析的版本不会包含末尾未被解释的部分。
func (v *Version) String() string {
	if v == nil {
		return "<nil>"
	}
	var buffer bytes.Buffer

	for i, comp := range v.components {
		if i > 0 {
			buffer.WriteString(".")
		}
		buffer.WriteString(fmt.Sprintf("%d", comp))
	}
	if v.preRelease != "" {
		buffer.WriteString("-")
		buffer.WriteString(v.preRelease)
	}
	if v.buildMetadata != "" {
		buffer.WriteString("+")
		buffer.WriteString(v.buildMetadata)
	}

	return buffer.String()
}

// compareInternal 在 v 小于 other 时返回 -1，大于时返回 1，相等时返回 0。
// 先逐段比较数字；数字段相同后，任一方非零的多余段使其更大；
// 只有双方都是 semver 时才继续比较 pre-release。
func (v *Version) compareInternal(other *Version) int {

	vLen := len(v.components)
	oLen := len(other.components)
	for i := 0; i < vLen && i < oLen; i++ {
		switch {
		case other.components[i] < v.components[i]:
			return 1
		case other.components[i] > v.components[i]:
			return -1
		}
	}

	// 公共段相同但一方有更多段时：若多出的段不全是零，则该方更大。
	switch {
	case oLen < vLen && !onlyZeros(v.components[oLen:]):
		return 1
	case oLen > vLen && !onlyZeros(other.components[vLen:]):
		return -1
	}

	if !v.semver || !other.semver {
		return 0
	}

	// semver 规则：有 pre-release 的版本小于无 pre-release 的版本。
	switch {
	case v.preRelease == "" && other.preRelease != "":
		return 1
	case v.preRelease != "" && other.preRelease == "":
		return -1
	case v.preRelease == other.preRelease: // 包括两者都为空
		return 0
	}

	// 逐段比较 pre-release：纯数字段按数值比较，其余按字典序比较。
	vPR := strings.Split(v.preRelease, ".")
	oPR := strings.Split(other.preRelease, ".")
	for i := 0; i < len(vPR) && i < len(oPR); i++ {
		vNum, err := strconv.ParseUint(vPR[i], 10, 0)
		if err == nil {
			oNum, err := strconv.ParseUint(oPR[i], 10, 0)
			if err == nil {
				switch {
				case oNum < vNum:
					return 1
				case oNum > vNum:
					return -1
				default:
					continue
				}
			}
		}
		if oPR[i] < vPR[i] {
			return 1
		} else if oPR[i] > vPR[i] {
			return -1
		}
	}

	// 前缀相同：段数更多的一方更大。
	switch {
	case len(oPR) < len(vPR):
		return 1
	case len(oPR) > len(vPR):
		return -1
	}

	return 0
}

// onlyZeros 在数组包含任何非零元素时返回 false。
func onlyZeros(array []uint) bool {
	for _, num := range array {
		if num != 0 {
			return false
		}
	}
	return true
}

// EqualTo 判断 v 是否等于给定版本；两者均为 nil 视为相等。
func (v *Version) EqualTo(other *Version) bool {
	if v == nil {
		return other == nil
	}
	if other == nil {
		return false
	}
	return v.compareInternal(other) == 0
}

// AtLeast 判断 v 是否不低于给定最低版本。双方都是语义化版本时使用 semver 比较算法；
// 否则只比较数字段，缺失段视为 0（即 "1.4" 等于 "1.4.0"）。
func (v *Version) AtLeast(min *Version) bool {
	return v.compareInternal(min) != -1
}

// LessThan 判断 v 是否小于给定版本（与 AtLeast 正好相反，用于 “v 是否太旧” 的语义）。
func (v *Version) LessThan(other *Version) bool {
	return v.compareInternal(other) == -1
}

// GreaterThan 判断 v 是否大于给定版本。
func (v *Version) GreaterThan(other *Version) bool {
	return v.compareInternal(other) == 1
}

// Compare 把 v 与版本字符串比较（按 v 是否为 semver 选择解析方式）。
// 成功时返回 -1（v 更小）、1（v 更大）或 0（相等）。
func (v *Version) Compare(other string) (int, error) {
	ov, err := parse(other, v.semver)
	if err != nil {
		return 0, err
	}
	return v.compareInternal(ov), nil
}

// WithInfo 返回版本对象的副本。
// Deprecated: Info 字段已从 Version 结构体中移除，该方法不再修改 Version 对象。
func (v *Version) WithInfo(info apimachineryversion.Info) *Version {
	result := *v
	return &result
}

// Info 返回组件的版本信息。
// Deprecated: 请改用 effective version 的 Info()。
func (v *Version) Info() *apimachineryversion.Info {
	if v == nil {
		return nil
	}
	// 若 info 为空，或其 major/minor 与实际不符，则以实际 major/minor 为准。
	return &apimachineryversion.Info{
		Major:      Itoa(v.Major()),
		Minor:      Itoa(v.Minor()),
		GitVersion: v.String(),
	}
}

// Itoa 把 uint 转为十进制字符串；0 返回空串（用于 major/minor 字段的约定）。
func Itoa(i uint) string {
	if i == 0 {
		return ""
	}
	return strconv.Itoa(int(i))
}
