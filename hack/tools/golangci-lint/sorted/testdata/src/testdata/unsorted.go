package testdata

type UnsortedFeature string

const (
	// 这些 feature 未排序，会触发 linter 报错
	UnsortedFeatureB UnsortedFeature = "UnsortedFeatureB"
	UnsortedFeatureA UnsortedFeature = "UnsortedFeatureA"
	UnsortedFeatureC UnsortedFeature = "UnsortedFeatureC"
)

var (
	// 这些 feature 未排序，会触发 linter 报错
	UnsortedVarFeatureC UnsortedFeature = "VarUnsortedFeatureC"
	UnsortedVarFeatureA UnsortedFeature = "VarUnsortedFeatureA"
	UnsortedVarFeatureB UnsortedFeature = "VarUnsortedFeatureB"
)
