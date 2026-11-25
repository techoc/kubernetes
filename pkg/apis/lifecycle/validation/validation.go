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

package validation

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/api/validate"
	"k8s.io/apimachinery/pkg/api/validation"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	validation2 "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	apivalidation "k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/kubernetes/pkg/apis/lifecycle"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
)

const defaultEvictionResponderCount = 1 // EvictionResponderImperativeEviction
// 在 .pod.spec.evictionResponders 之外再多出一个，用于计入默认 responder
const maxEvictionResponders = apivalidation.MaxPodEvictionResponders + defaultEvictionResponderCount

// ValidateEvictionRequest 校验一个 EvictionRequest。
func ValidateEvictionRequest(evictionRequest *lifecycle.EvictionRequest) field.ErrorList {
	validateLongName := func(fldPath *field.Path, name string) field.ErrorList {
		return validate.LongName(context.Background(), operation.Operation{}, fldPath, &name, nil).MarkCoveredByDeclarative()
	}
	allErrs := apivalidation.ValidateObjectMetaWithOpts(&evictionRequest.ObjectMeta, true, validateLongName, field.NewPath("metadata"))
	// Spec 的校验由 DV（Validate_EvictionRequestSpec）覆盖
	return allErrs
}

// ValidateEvictionRequestUpdate 校验一个 EvictionRequest。
func ValidateEvictionRequestUpdate(evictionRequest, oldEvictionRequest *lifecycle.EvictionRequest) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMetaUpdate(&evictionRequest.ObjectMeta, &oldEvictionRequest.ObjectMeta, field.NewPath("metadata"))
	// Spec 的校验由 DV（Validate_EvictionRequestSpec）覆盖
	return allErrs
}

// ValidateEvictionRequestStatusUpdate 校验 EvictionRequest Status 的更新。
func ValidateEvictionRequestStatusUpdate(evictionRequest, oldEvictionRequest *lifecycle.EvictionRequest) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMetaUpdate(&evictionRequest.ObjectMeta, &oldEvictionRequest.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateEvictionRequestStatus(&evictionRequest.Status, &oldEvictionRequest.Status, field.NewPath("status"))...)
	return allErrs
}

// ValidateEvictionRequestStatus 校验一个 EvictionRequest Status。
func ValidateEvictionRequestStatus(status, oldStatus *lifecycle.EvictionRequestStatus, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	// observedGeneration 已由 DV 覆盖

	// 条件列表。
	conditionsPath := fldPath.Child("conditions")
	// conditions 的校验已由 DV 覆盖
	allErrs = append(allErrs, validation2.ValidateConditions(status.Conditions, conditionsPath)...)

	return allErrs
}

// ValidateEviction 校验一个 Eviction。
func ValidateEviction(eviction *lifecycle.Eviction) field.ErrorList {
	validateLongName := func(fldPath *field.Path, name string) field.ErrorList {
		return validate.LongName(context.Background(), operation.Operation{}, fldPath, &name, nil).MarkCoveredByDeclarative()
	}
	allErrs := apivalidation.ValidateObjectMetaWithOpts(&eviction.ObjectMeta, true, validateLongName, field.NewPath("metadata"))
	// Spec 的校验由 DV（Validate_EvictionSpec）覆盖
	return allErrs
}

// ValidateEvictionUpdate 校验一个 Eviction。
func ValidateEvictionUpdate(eviction, oldEviction *lifecycle.Eviction) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMetaUpdate(&eviction.ObjectMeta, &oldEviction.ObjectMeta, field.NewPath("metadata"))
	// Spec 的校验由 DV（Validate_EvictionSpec）覆盖
	return allErrs
}

type EvictionStatusValidationOptions struct {
	Clock clock.PassiveClock
}

// ValidateEvictionStatusUpdate 校验 Eviction Status 的更新。
func ValidateEvictionStatusUpdate(eviction, oldEviction *lifecycle.Eviction, opts EvictionStatusValidationOptions) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMetaUpdate(&eviction.ObjectMeta, &oldEviction.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateEvictionStatus(&eviction.Status, &oldEviction.Status, field.NewPath("status"), opts)...)
	return allErrs
}

// ValidateEvictionStatus 校验一个 Eviction Status。
func ValidateEvictionStatus(status, oldStatus *lifecycle.EvictionStatus, fldPath *field.Path, opts EvictionStatusValidationOptions) field.ErrorList {
	var allErrs field.ErrorList

	// observedGeneration 已由 DV 覆盖

	// 条件列表。
	conditionsPath := fldPath.Child("conditions")
	// conditions 的校验已由 DV 覆盖
	allErrs = append(allErrs, validation2.ValidateConditions(status.Conditions, conditionsPath)...)
	isEvicted := meta.IsStatusConditionTrue(status.Conditions, string(lifecycle.EvictionConditionTargetEvicted))
	isFailed := meta.IsStatusConditionTrue(status.Conditions, string(lifecycle.EvictionConditionFailed))
	for _, oldCondition := range oldStatus.Conditions {
		if oldCondition.Type == string(lifecycle.EvictionConditionTargetEvicted) || oldCondition.Type == string(lifecycle.EvictionConditionFailed) {
			newCondition := meta.FindStatusCondition(status.Conditions, oldCondition.Type)
			if oldCondition.Status == v1.ConditionTrue && (newCondition == nil || newCondition.Status == v1.ConditionFalse || oldCondition.Reason != newCondition.Reason) {
				allErrs = append(allErrs, field.Invalid(conditionsPath, status.Conditions, fmt.Sprintf("%s condition status cannot be reverted", oldCondition.Type)))
				if oldCondition.Type == string(lifecycle.EvictionConditionTargetEvicted) {
					isEvicted = true // 后续校验不要使用非法的 condition 状态
				}
				if oldCondition.Type == string(lifecycle.EvictionConditionFailed) {
					isFailed = true // 后续校验不要使用非法的 condition 状态
				}
			}
		}
	}

	// requesters 已由 DV（Validate_EvictionStatus）覆盖

	// targetResponders 和 responders
	allErrs = append(allErrs, ValidateAllEvictionStatusResponderFields(status, oldStatus, fldPath, EvictionStatusRespondersValidationOptions{
		Clock:     opts.Clock,
		IsEvicted: isEvicted,
		IsFailed:  isFailed,
	})...)

	return allErrs
}

type EvictionStatusRespondersValidationOptions struct {
	Clock     clock.PassiveClock
	IsEvicted bool
	IsFailed  bool
}

// ValidateAllEvictionStatusResponderFields 校验 .status.targetResponders 和 .status.responders
//
// 预期会有多个参与方更新 status（eviction-controller、responders，以及更新 conditions 的 requesters）。
// 我们必须模拟 eviction-controller 的行为，以防止其他行为异常/恶意的参与方进行非法更新。
//  1. 应当首先设置 .status.targetResponders 和 .status.responders。
//  2. Responder 的状态从 Inactive 转为 Active，再从 Active 转为 Interrupted、Canceled 或 Complete，
//     按照 responder 优先级顺序逐步推进。该过程不可逆。
//  3. 当 .status.responders[].completionTime 被设置或心跳超时时，
//     Responder 的状态从 Active 转为 Interrupted、Canceled 或 Complete。该过程不可逆。
//  4. .status.responders 中的条目一旦设置就不能被移除。只有活跃的 responder 才能修改它。
//  5. 控制器可以通过 conditions 将 Eviction 标记为 Evicted 或 Canceled。此时我们必须允许
//     从 activeResponders 中移除，并阻止新增任何条目。
func ValidateAllEvictionStatusResponderFields(status, oldStatus *lifecycle.EvictionStatus, fldPath *field.Path, opts EvictionStatusRespondersValidationOptions) field.ErrorList {
	var allErrs field.ErrorList
	heartbeatDeadline := time.Minute * 20
	allowedTimeSkew := time.Second * 30
	allowedMaxExpectedCompletionTime := time.Hour * 24 * 365 * 10 // 10 年

	statusResponders := append(make([]lifecycle.ResponderStatus, 0, len(status.Responders)), status.Responders...)

	// targetResponders
	allErrs = append(allErrs, ValidateEvictionTargetResponders(status.TargetResponders, oldStatus.TargetResponders, fldPath.Child("targetResponders"), ValidateEvictionTargetRespondersOptions{
		Clock:             opts.Clock,
		IsFailed:          opts.IsFailed,
		IsEvicted:         opts.IsEvicted,
		StatusResponders:  statusResponders,
		HeartbeatDeadline: heartbeatDeadline,
		AllowedTimeSkew:   allowedTimeSkew,
	})...)

	// responders（responder 列表）
	allErrs = append(allErrs, ValidateEvictionStatusResponders(status.Responders, oldStatus.Responders, fldPath.Child("responders"), ValidateEvictionStatusRespondersOptions{
		Clock:                     opts.Clock,
		TargetResponders:          status.TargetResponders,
		AllowedTimeSkew:           allowedTimeSkew,
		MaxExpectedCompletionTime: allowedMaxExpectedCompletionTime,
	})...)

	return allErrs
}

type ValidateEvictionTargetRespondersOptions struct {
	Clock             clock.PassiveClock
	IsEvicted         bool
	IsFailed          bool
	StatusResponders  []lifecycle.ResponderStatus
	HeartbeatDeadline time.Duration
	AllowedTimeSkew   time.Duration
}

func ValidateEvictionTargetResponders(targetResponders, oldTargetResponders []lifecycle.TargetResponder, fldPath *field.Path, opts ValidateEvictionTargetRespondersOptions) field.ErrorList {
	var allErrs field.ErrorList
	if len(oldTargetResponders) != 0 {
		if len(targetResponders) != len(oldTargetResponders) {
			return append(allErrs, field.Invalid(fldPath, targetResponders, "must preserve the same length and the same keys"))
		}
		targetRespondersNames := sets.New[string]()
		for _, responder := range targetResponders {
			targetRespondersNames.Insert(responder.Name)
		}
		for _, oldTargetResponder := range oldTargetResponders {
			if !targetRespondersNames.Has(oldTargetResponder.Name) {
				return append(allErrs, field.Invalid(fldPath, targetResponders, "must preserve the same keys"))
			}
		}
	}
	if len(targetResponders) == 0 {
		// 尚未初始化
		return allErrs
	}

	if len(targetResponders) > maxEvictionResponders {
		// 已由声明式校验覆盖；提前退出
		return allErrs
	}

	// 模拟声明式错误以便提前退出
	uniqueErrors := validate.ValSliceUnique(context.TODO(), operation.Operation{}, fldPath, targetResponders, nil,
		func(a *lifecycle.TargetResponder, b *lifecycle.TargetResponder) bool { return a.Name == b.Name })
	if len(uniqueErrors) > 0 {
		// 存在重复项时，检查状态转换没有意义
		return allErrs
	}
	statusResponders := map[string]*lifecycle.ResponderStatus{}
	for _, responder := range opts.StatusResponders {
		statusResponders[responder.Name] = &responder
	}
	targetRespondersOriginalIdx := map[string]int{}
	for i, responder := range targetResponders {
		targetRespondersOriginalIdx[responder.Name] = i
	}

	// 先对 responders 排序，然后使用有序推进来简化处理。
	// 这意味着索引靠前的先被激活（Activated），索引最后的最后处理。
	sortedOldTargetResponders := append([]lifecycle.TargetResponder{}, oldTargetResponders...)
	SortTargetResponders(sortedOldTargetResponders)

	sortedTargetResponders := append([]lifecycle.TargetResponder{}, targetResponders...)
	SortTargetResponders(sortedTargetResponders)

	lastActiveIdx := -1
	for i, responder := range sortedOldTargetResponders {
		if responder.State == lifecycle.ResponderStateActive {
			lastActiveIdx = i
			break
		}
	}
	hasNeverBeenActive := len(sortedOldTargetResponders) == 0 // 应当在首次同步时激活（Activate）
	hasFoundLastActive := lastActiveIdx != -1
	activeChanged := hasFoundLastActive && sortedTargetResponders[lastActiveIdx].State != lifecycle.ResponderStateActive
	isFinal := opts.IsEvicted || opts.IsFailed

	lastOldResponderState := lifecycle.ResponderStateInactive
	if len(sortedOldTargetResponders) > 0 {
		lastOldResponderState = sortedOldTargetResponders[len(sortedOldTargetResponders)-1].State
	}
	isFinalOrAllProcessed := isFinal || lastOldResponderState == lifecycle.ResponderStateCompleted ||
		lastOldResponderState == lifecycle.ResponderStateCanceled ||
		lastOldResponderState == lifecycle.ResponderStateInterrupted

	for i, responder := range sortedTargetResponders {
		expectedStates := sets.New[lifecycle.ResponderStateType]()

		expectedStatesReason := ""
		switch {
		//
		case i < lastActiveIdx ||
			(len(sortedOldTargetResponders) > 0 && !hasFoundLastActive && isFinalOrAllProcessed):
			// 已处理过的 responder 必须保持其状态。
			expectedStates.Insert(sortedOldTargetResponders[i].State)
			expectedStatesReason = "final state is immutable"
		case i == lastActiveIdx && activeChanged:
			// 若 Active 状态的 responder 发生变化，它必须进入终态。
			expectedStates.Insert(lifecycle.ResponderStateInterrupted,
				lifecycle.ResponderStateCanceled,
				lifecycle.ResponderStateCompleted)
			expectedStatesReason = "this responder must reach a final state"
		case i == lastActiveIdx:
			// 未发生变化的 Active responder 可以保持 Active。
			expectedStates.Insert(lifecycle.ResponderStateActive)
			expectedStatesReason = "the eviction request stays active"
			if isFinal {
				// 最后一个 responder 必须在设置终态 condition 之前完成。
				expectedStates.Clear().Insert(lifecycle.ResponderStateInterrupted,
					lifecycle.ResponderStateCanceled,
					lifecycle.ResponderStateCompleted)
				expectedStatesReason = "the eviction request has finished processing"
			}
		case i == 0 && hasNeverBeenActive,
			i == lastActiveIdx+1 && activeChanged:
			// 当前一个 responder 进入终态后，下一个 responder 必须转为 Active 状态。
			expectedStates.Insert(lifecycle.ResponderStateActive)
			expectedStatesReason = "this responder is next in line"
			if isFinal {
				// 若已处理完毕，则不要激活下一个 responder。
				expectedStates.Clear().Insert(lifecycle.ResponderStateInactive)
				expectedStatesReason = "the eviction request has finished processing"
			}
		default:
			expectedStates.Insert(lifecycle.ResponderStateInactive)
			expectedStatesReason = "the previously active has not finished processing"
		}

		allErrs = append(allErrs, ValidateTargetResponder(responder, fldPath.Index(targetRespondersOriginalIdx[responder.Name]), ValidateEvictionTargetResponderOptions{
			Clock:                opts.Clock,
			IsEvicted:            opts.IsEvicted,
			IsFailed:             opts.IsFailed,
			responderStatus:      statusResponders[responder.Name],
			expectedStates:       expectedStates,
			expectedStatesReason: expectedStatesReason,
			HeartbeatDeadline:    opts.HeartbeatDeadline,
			AllowedTimeSkew:      opts.AllowedTimeSkew,
		})...)
	}
	return allErrs
}

type ValidateEvictionTargetResponderOptions struct {
	Clock                clock.PassiveClock
	IsEvicted            bool
	IsFailed             bool
	responderStatus      *lifecycle.ResponderStatus
	expectedStates       sets.Set[lifecycle.ResponderStateType]
	expectedStatesReason string
	HeartbeatDeadline    time.Duration
	AllowedTimeSkew      time.Duration
}

func ValidateTargetResponder(evictionResponder lifecycle.TargetResponder, fldPath *field.Path, opts ValidateEvictionTargetResponderOptions) field.ErrorList {
	var allErrs field.ErrorList

	// name 已由 DV 覆盖

	statusResponderPath := field.NewPath("status", "responders")
	if opts.responderStatus == nil {
		msg := fmt.Sprintf("%q has to be tracked in %s first", evictionResponder.Name, statusResponderPath)
		return append(allErrs, field.Invalid(fldPath, evictionResponder, msg))
	}

	// priority（优先级）
	const reservedKubernetesRespondersPriority = 1000
	priorityAssignedToK8sResponders := map[string]int32{
		lifecycle.EvictionResponderImperativeEviction: 100,
	}
	priorityPath := fldPath.Child("priority")
	// validate.Maximum 已由 DV 覆盖
	// validate.Minimum 已由 DV 覆盖
	if evictionResponder.Priority != nil && *evictionResponder.Priority >= 0 {
		if expectedPriority, ok := priorityAssignedToK8sResponders[evictionResponder.Name]; ok {
			if *evictionResponder.Priority != expectedPriority {
				allErrs = append(allErrs, field.Invalid(priorityPath, *evictionResponder.Priority, fmt.Sprintf("core k8s responder is expected to have priority: %d", expectedPriority)))
			}
		} else if *evictionResponder.Priority < reservedKubernetesRespondersPriority {
			// 每个核心 responder 都应当已分配 priority——见 priorityAssignedToK8sResponders
			allErrs = append(allErrs, field.Invalid(priorityPath, *evictionResponder.Priority, "priorities 0-999 are reserved for responders with *.k8s.io suffix"))
		}
	}

	// state（状态）
	statePath := fldPath.Child("state")
	// validate.RequiredValue 已由 DV 覆盖
	// validate.Enum 已由 DV 覆盖

	// 检查状态转换是否被允许
	if !opts.expectedStates.Has(evictionResponder.State) {
		var expectedValues []string
		for _, ev := range sets.List(opts.expectedStates) {
			expectedValues = append(expectedValues, string(ev))
		}
		msg := fmt.Sprintf("must be one of: %s", strings.Join(expectedValues, ", "))
		if len(opts.expectedStatesReason) > 0 {
			msg = fmt.Sprintf("%s, because %s", msg, opts.expectedStatesReason)
		}
		allErrs = append(allErrs, field.Invalid(statePath, evictionResponder.State, msg))
		// 提前返回，因为下面的校验依赖正确的 state
		return allErrs
	}

	// 必须存在于 status responders 中

	// 若 responder 已不再活跃（.status.responders），检查心跳截止时间。
	// 如果 Eviction 已进入终态（failed 或 evicted），可以跳过该检查。
	// StartTime 的存在性在 ValidateEvictionStatusResponder 中校验
	if evictionResponder.State != lifecycle.ResponderStateActive && opts.responderStatus.StartTime != nil &&
		opts.responderStatus.CompletionTime == nil && !opts.IsEvicted && !opts.IsFailed {
		heartbeat := opts.responderStatus.StartTime
		if opts.responderStatus.HeartbeatTime != nil {
			heartbeat = opts.responderStatus.HeartbeatTime
		}
		if opts.Clock.Now().Before(heartbeat.Add(opts.HeartbeatDeadline).Add(-opts.AllowedTimeSkew)) {
			msg := fmt.Sprintf("must stay Active because the responder is in progress and it should report %s or %s", statusResponderPath.Child("heartbeatTime"), statusResponderPath.Child("completionTime"))
			allErrs = append(allErrs, field.Forbidden(statePath, msg))
		}
	}
	if evictionResponder.State == lifecycle.ResponderStateCompleted && opts.responderStatus.CompletionTime == nil {
		msg := fmt.Sprintf("cannot become Completed because the responder didn't report %s", statusResponderPath.Child("completionTime"))
		allErrs = append(allErrs, field.Invalid(statePath, evictionResponder.State, msg))
	}
	return allErrs
}

type ValidateEvictionStatusRespondersOptions struct {
	Clock                     clock.PassiveClock
	TargetResponders          []lifecycle.TargetResponder
	AllowedTimeSkew           time.Duration
	MaxExpectedCompletionTime time.Duration
}

func ValidateEvictionStatusResponders(statusResponders, oldStatusResponders []lifecycle.ResponderStatus, fldPath *field.Path, opts ValidateEvictionStatusRespondersOptions) field.ErrorList {
	var allErrs field.ErrorList

	if len(opts.TargetResponders) != len(statusResponders) {
		return append(allErrs, field.Invalid(fldPath, statusResponders, "must be the same length as status.targetResponders and contain the same keys"))
	}
	statusRespondersNames := sets.New[string]()
	for _, responder := range statusResponders {
		statusRespondersNames.Insert(responder.Name)
	}
	targetResponders := map[string]*lifecycle.TargetResponder{}
	for i, targetResponder := range opts.TargetResponders {
		targetResponders[targetResponder.Name] = &opts.TargetResponders[i]
		if !statusRespondersNames.Has(targetResponder.Name) {
			return append(allErrs, field.Invalid(fldPath, statusResponders, "must contain the same keys as status.targetResponders"))
		}
	}
	if len(statusResponders) == 0 {
		// statusResponders 和 TargetResponders 尚未初始化
		return allErrs
	}

	if len(statusResponders) > maxEvictionResponders {
		// TooMany 由声明式校验处理——检测到后提前返回
		return allErrs
	}
	// validate.ValSliceUnique 已由 DV 覆盖——模拟一个错误
	uniqueErrors := validate.ValSliceUnique(context.TODO(), operation.Operation{}, fldPath, statusResponders, nil,
		func(a *lifecycle.ResponderStatus, b *lifecycle.ResponderStatus) bool { return a.Name == b.Name })
	if len(uniqueErrors) > 0 {
		// 存在重复项时，逐个检查 responder 状态没有意义，因为我们依赖 target responder 和 oldStatusResponder
		return allErrs
	}

	oldStatusRespondersMap := map[string]*lifecycle.ResponderStatus{}
	for i := range oldStatusResponders {
		oldStatusRespondersMap[oldStatusResponders[i].Name] = &oldStatusResponders[i] // +k8s:verify-mutation:reason=clone
	}
	for i, responder := range statusResponders {
		targetResponder := targetResponders[responder.Name] // key 存在性已在上方检查
		responderPath := fldPath.Index(i)
		allErrs = append(allErrs, ValidateEvictionStatusResponder(&responder, oldStatusRespondersMap[responder.Name], responderPath,
			EvictionStatusResponderValidationOptions{
				Clock:                     opts.Clock,
				responderState:            targetResponder.State,
				AllowedTimeSkew:           opts.AllowedTimeSkew,
				MaxExpectedCompletionTime: opts.MaxExpectedCompletionTime,
			},
		)...)
	}
	return allErrs
}

type EvictionStatusResponderValidationOptions struct {
	responderState            lifecycle.ResponderStateType
	AllowedTimeSkew           time.Duration
	MaxExpectedCompletionTime time.Duration
	Clock                     clock.PassiveClock
}

func ValidateEvictionStatusResponder(status, oldStatus *lifecycle.ResponderStatus, fldPath *field.Path, opts EvictionStatusResponderValidationOptions) field.ErrorList {
	var allErrs field.ErrorList
	oldDefaultedStatus := lifecycle.ResponderStatus{}
	if oldStatus != nil {
		oldDefaultedStatus = *oldStatus // +k8s:verify-mutation:reason=clone
	}

	if oldStatus != nil && !validate.SemanticDeepEqual(status, oldStatus) && opts.responderState != lifecycle.ResponderStateActive {
		// 不可变；对 ResponderStatus 的修改仅允许由活跃 responder 或在初始化期间进行
		return append(allErrs, field.Invalid(fldPath, status, validation.FieldImmutableErrorMsg).WithOrigin("immutable"))
	}

	// name 已由 DV 覆盖
	// 同名 targetResponder 的存在性校验在 ValidateEvictionStatusResponders 中完成

	// startTime（开始时间）
	startTimePath := fldPath.Child("startTime")
	// 一旦设置即不可变
	if oldDefaultedStatus.StartTime != nil && !oldDefaultedStatus.StartTime.Equal(status.StartTime) {
		// validate.NoUnset 和 validate.NoModify 检查已由 DV 覆盖
	} else if status.StartTime == nil && opts.responderState == lifecycle.ResponderStateActive {
		allErrs = append(allErrs, field.Required(startTimePath, "is required for an active responder"))
	} else if status.StartTime != nil && !oldDefaultedStatus.StartTime.Equal(status.StartTime) && !timeNear(status.StartTime.Time, opts.Clock.Now(), opts.AllowedTimeSkew) {
		allErrs = append(allErrs, field.Invalid(startTimePath, status.StartTime, "must be set to the present time"))
	}

	// heartbeatTime（心跳时间）
	heartbeatTimePath := fldPath.Child("heartbeatTime")
	if oldDefaultedStatus.HeartbeatTime != nil && status.HeartbeatTime == nil {
		allErrs = append(allErrs, field.Required(heartbeatTimePath, "is required once set"))
	}
	if !oldDefaultedStatus.HeartbeatTime.Equal(status.HeartbeatTime) && status.HeartbeatTime != nil {
		if status.StartTime == nil {
			allErrs = append(allErrs, field.Invalid(heartbeatTimePath, status.HeartbeatTime, fmt.Sprintf("cannot be set before %s is set", startTimePath.String())))
		} else if status.HeartbeatTime.Before(oldDefaultedStatus.HeartbeatTime) {
			// 由于允许时钟偏差，这种情况仍有可能发生
			allErrs = append(allErrs, field.Invalid(heartbeatTimePath, status.HeartbeatTime, "cannot be decreased"))
		} else if status.HeartbeatTime.Before(status.StartTime) {
			// 由于允许时钟偏差，这种情况仍有可能发生
			allErrs = append(allErrs, field.Invalid(heartbeatTimePath, status.HeartbeatTime, fmt.Sprintf("must occur after %s", startTimePath.String())))
		} else if !timeNear(status.HeartbeatTime.Time, opts.Clock.Now(), opts.AllowedTimeSkew) {
			allErrs = append(allErrs, field.Invalid(heartbeatTimePath, status.HeartbeatTime, "must be set to the present time"))
		}
	}

	// expectedCompletionTime（预期完成时间）
	expectedCompletionTimePath := fldPath.Child("expectedCompletionTime")
	if !oldDefaultedStatus.ExpectedCompletionTime.Equal(status.ExpectedCompletionTime) && status.ExpectedCompletionTime != nil {
		if status.StartTime == nil {
			allErrs = append(allErrs, field.Invalid(expectedCompletionTimePath, status.ExpectedCompletionTime, fmt.Sprintf("cannot be set before %s is set", startTimePath.String())))
		} else if status.ExpectedCompletionTime.Before(status.StartTime) {
			// 由于允许时钟偏差，这种情况仍有可能发生
			allErrs = append(allErrs, field.Invalid(expectedCompletionTimePath, status.ExpectedCompletionTime, fmt.Sprintf("must occur after %s", startTimePath.String())))
		} else if status.ExpectedCompletionTime.Time.Before(opts.Clock.Now().Add(-opts.AllowedTimeSkew)) {
			allErrs = append(allErrs, field.Invalid(expectedCompletionTimePath, status.ExpectedCompletionTime, "cannot be set to the past time"))
		} else if status.ExpectedCompletionTime.Time.After(opts.Clock.Now().Add(opts.MaxExpectedCompletionTime)) {
			allErrs = append(allErrs, field.Invalid(expectedCompletionTimePath, status.ExpectedCompletionTime, "must complete within 10 years")) // 合理性检查（sanity check）
		}
	}

	// completionTime（完成时间）
	completionTimePath := fldPath.Child("completionTime")
	// 一旦设置即不可变
	if oldDefaultedStatus.CompletionTime != nil && !oldDefaultedStatus.CompletionTime.Equal(status.CompletionTime) {
		// validate.NoUnset 和 validate.NoModify 检查已由 DV 覆盖
	} else if status.CompletionTime != nil {
		if status.StartTime == nil {
			allErrs = append(allErrs, field.Invalid(completionTimePath, status.CompletionTime, fmt.Sprintf("cannot be set before %s is set", startTimePath.String())))
		} else if status.CompletionTime.Before(status.StartTime) {
			allErrs = append(allErrs, field.Invalid(completionTimePath, status.CompletionTime, fmt.Sprintf("must occur after %s", startTimePath.String())))
		} else if !oldDefaultedStatus.CompletionTime.Equal(status.CompletionTime) && !timeNear(status.CompletionTime.Time, opts.Clock.Now(), opts.AllowedTimeSkew) {
			allErrs = append(allErrs, field.Invalid(completionTimePath, status.CompletionTime, "must be set to the present time"))
		}
	}

	// message 已由 DV 覆盖

	return allErrs
}

func timeNear(a, b time.Time, skew time.Duration) bool {
	return a.After(b.Add(-skew)) && a.Before(b.Add(skew))
}

// SortTargetResponders 将优先级最高的 responder 放在靠前的索引位置。
// 若多个 responder 优先级相同，则域名字母序更高（更靠前）的 responder 会被选中。
// 也就是说，先按顶层域名标签进行字母序比较，再依次比较较低的域名标签，
// 最后才比较 key。
func SortTargetResponders(responders []lifecycle.TargetResponder) {
	getComponents := func(domainPrefixedKey string) ([]string, string) {
		segments := strings.SplitN(domainPrefixedKey, "/", 2)
		if len(segments) == 0 {
			return nil, ""
		}
		var domainComponents []string
		var key string
		if len(segments) == 2 {
			key = segments[1]
		}
		domainComponents = strings.Split(segments[0], ".")
		slices.Reverse(domainComponents)
		return domainComponents, key
	}
	slices.SortFunc(responders, func(a, b lifecycle.TargetResponder) int {
		if !ptr.Equal(a.Priority, b.Priority) {
			if a.Priority == nil {
				return 1
			}
			if b.Priority == nil {
				return -1
			}
			return int(ptr.Deref(b.Priority, -1) - ptr.Deref(a.Priority, -1))
		}
		aComponents, aKey := getComponents(a.Name)
		bComponents, bKey := getComponents(b.Name)

		for i, aComponent := range aComponents {
			if i > len(bComponents)-1 {
				// 若 b 已无剩余分量，则 b 的优先级更高
				return 1
			}
			bComponent := bComponents[i]
			cmp := strings.Compare(aComponent, bComponent)
			if cmp != 0 {
				return cmp
			}
		}
		if len(bComponents) > len(aComponents) {
			// 若 a 已无剩余分量，则 a 的优先级更高
			return -1
		}
		return strings.Compare(aKey, bKey)
	})
}
