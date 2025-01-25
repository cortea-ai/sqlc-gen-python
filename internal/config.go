package python

type Config struct {
	EmitExactTableNames         bool     `json:"emit_exact_table_names"`
	Package                     string   `json:"package"`
	Out                         string   `json:"out"`
	EmitPydanticModels          bool     `json:"emit_pydantic_models"`
	QueryParameterLimit         *int32   `json:"query_parameter_limit"`
	InflectionExcludeTableNames []string `json:"inflection_exclude_table_names"`
	TablePrefix                 string   `json:"table_prefix"`
	// Best effort approach to enforce filtering on specific fields.Not covered:
	// 	- Associative tables
	// 	- sqlc.embed()
	// 	- json_agg(tbl.*)
	EnforcedFilterFields []string `json:"enforced_filter_fields"`
	// Merge queries defined in different files into one output queries.py file
	MergeQueryFiles bool `json:"merge_query_files"`
	// Use custom pydantic base class for models
	PydanticBaseClass string `json:"pydantic_base_class"`
}

const MODELS_FILENAME = "db_models"
const ENUMS_FILENAME = "db_enums"
const MODEL_DICTS_FILENAME = "db_model_dicts"
const QUERY_DICTS_FILENAME = "db_query_dicts"
