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
}

const MODELS_FILENAME = "db_models"
