package python

type Config struct {
	EmitExactTableNames         bool     `json:"emit_exact_table_names"`
	Package                     string   `json:"package"`
	Out                         string   `json:"out"`
	EmitPydanticModels          bool     `json:"emit_pydantic_models"`
	QueryParameterLimit         *int32   `json:"query_parameter_limit"`
	InflectionExcludeTableNames []string `json:"inflection_exclude_table_names"`
	TablePrefix                 string   `json:"table_prefix"`
}
