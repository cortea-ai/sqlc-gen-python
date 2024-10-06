package python

type Config struct {
	EmitExactTableNames         bool     `json:"emit_exact_table_names"`
	Package                     string   `json:"package"`
	Out                         string   `json:"out"`
	EmitPydanticModels          bool     `json:"emit_pydantic_models"`
	QueryParameterLimit         *int32   `json:"query_parameter_limit"`
	InflectionExcludeTableNames []string `json:"inflection_exclude_table_names"`
	TablePrefix                 string   `json:"table_prefix"`
	// When a query uses a table with RLS enforced fields, it will be required to
	// parametrized those fields. Not covered:
	// 	- Associate tables
	// 	- sqlc.embed()
	// 	- json_agg(tbl.*)
	RLSEnforcedFields []string `json:"rls_enforced_fields"`
	// Merge queries defined in different files into one output queries.py file
	MergeQueryFiles bool `json:"merge_query_files"`
}

const MODELS_FILENAME = "db_models"
