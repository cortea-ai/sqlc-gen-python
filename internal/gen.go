package python

import (
	"context"
	json "encoding/json"
	"errors"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strings"

	"github.com/sqlc-dev/plugin-sdk-go/metadata"
	"github.com/sqlc-dev/plugin-sdk-go/plugin"
	"github.com/sqlc-dev/plugin-sdk-go/sdk"

	pyast "github.com/sqlc-dev/sqlc-gen-python/internal/ast"
	"github.com/sqlc-dev/sqlc-gen-python/internal/inflection"
	"github.com/sqlc-dev/sqlc-gen-python/internal/poet"
	pyprint "github.com/sqlc-dev/sqlc-gen-python/internal/printer"
)

type Constant struct {
	Name  string
	Type  string
	Value string
}

type Enum struct {
	Name      string
	Comment   string
	Constants []Constant
}

type pyType struct {
	InnerType string
	IsArray   bool
	IsNull    bool
}

func (t pyType) Annotation(isFuncSignature bool) *pyast.Node {
	ann := poet.Name(t.InnerType)
	if t.IsArray {
		ann = subscriptNode("List", ann)
	}
	if t.IsNull && isFuncSignature {
		ann = optionalKeywordNode("Optional", ann, t.IsArray)
	}
	if t.IsNull && !isFuncSignature {
		ann = subscriptNode("Optional", ann)
	}
	return ann
}

type Field struct {
	Name    string
	Type    pyType
	Comment string
	// EmbedFields contains the embedded fields that require scanning.
	EmbedFields []Field
}

type Struct struct {
	Table   plugin.Identifier
	Name    string
	Fields  []Field
	Comment string
}

type QueryValue struct {
	Emit   bool
	Name   string
	Struct *Struct
	Typ    pyType
}

// Annotation in function signature
func (v QueryValue) Annotation() *pyast.Node {
	if v.Typ != (pyType{}) {
		return v.Typ.Annotation(true)
	}
	if v.Struct != nil {
		if v.Emit {
			return poet.Name(v.Struct.Name)
		} else {
			return typeRefNode(MODELS_FILENAME, v.Struct.Name)
		}
	}
	panic("no type for QueryValue: " + v.Name)
}

func (v QueryValue) EmitStruct() bool {
	return v.Emit
}

func (v QueryValue) IsStruct() bool {
	return v.Struct != nil
}

func (v QueryValue) isEmpty() bool {
	return v.Typ == (pyType{}) && v.Name == "" && v.Struct == nil
}

func (v QueryValue) RowNode(rowVar string) *pyast.Node {
	if !v.IsStruct() {
		return subscriptNode(
			rowVar,
			constantInt(0),
		)
	}
	call := &pyast.Call{
		Func: v.Annotation(),
	}
	var idx int
	for _, f := range v.Struct.Fields {
		var val *pyast.Node
		var argName string
		if len(f.EmbedFields) > 0 {
			argName = singularize(f.Name)
			var embedFields []*pyast.Keyword
			for _, embed := range f.EmbedFields {
				embedFields = append(embedFields, &pyast.Keyword{
					Arg:   embed.Name,
					Value: subscriptNode(rowVar, constantInt(idx)),
				})
				idx++
			}
			val = &pyast.Node{
				Node: &pyast.Node_Compare{
					Compare: &pyast.Compare{
						Left: &pyast.Node{
							Node: &pyast.Node_Call{
								Call: &pyast.Call{
									Func:     poet.Name(f.Type.InnerType),
									Keywords: embedFields,
								},
							},
						},
						Ops: []*pyast.Node{
							poet.Name(fmt.Sprintf("if row[%d] else", idx-len(f.EmbedFields))),
						},
						Comparators: []*pyast.Node{
							poet.Constant(nil),
						},
					},
				},
			}
		} else {
			argName = f.Name
			val = subscriptNode(rowVar, constantInt(idx))
			idx++
		}
		call.Keywords = append(call.Keywords, &pyast.Keyword{
			Arg:   argName,
			Value: val,
		})

	}
	return &pyast.Node{
		Node: &pyast.Node_Call{
			Call: call,
		},
	}
}

// A struct used to generate methods and fields on the Queries struct
type Query struct {
	Cmd          string
	Comments     []string
	MethodName   string
	FieldName    string
	ConstantName string
	SQL          string
	SourceName   string
	Ret          QueryValue
	Args         []QueryValue
}

func (q Query) AddArgs(args *pyast.Arguments) {
	// A single struct arg does not need to be passed as a keyword argument
	if len(q.Args) == 1 && q.Args[0].IsStruct() {
		args.Args = append(args.Args, &pyast.Arg{
			Arg:        q.Args[0].Name,
			Annotation: q.Args[0].Annotation(),
		})
		return
	}
	var optionalArgs []*pyast.Arg
	for _, a := range q.Args {
		if a.Typ.IsNull {
			optionalArgs = append(optionalArgs, &pyast.Arg{
				Arg:        a.Name,
				Annotation: a.Annotation(),
			})
			continue
		}
		args.KwOnlyArgs = append(args.KwOnlyArgs, &pyast.Arg{
			Arg:        a.Name,
			Annotation: a.Annotation(),
		})
	}
	args.KwOnlyArgs = append(args.KwOnlyArgs, optionalArgs...)
}

func (q Query) ArgNodes() []*pyast.Node {
	args := []*pyast.Node{}
	i := 1
	for _, a := range q.Args {
		if a.isEmpty() {
			continue
		}
		if a.IsStruct() {
			for _, f := range a.Struct.Fields {
				args = append(args, typeRefNode(a.Name, f.Name))
				i++
			}
		} else {
			args = append(args, poet.Name(a.Name))
			i++
		}
	}
	return args
}

func (q Query) ArgDictNode() *pyast.Node {
	dict := &pyast.Dict{}
	i := 1
	for _, a := range q.Args {
		if a.isEmpty() {
			continue
		}
		if a.IsStruct() {
			for _, f := range a.Struct.Fields {
				dict.Keys = append(dict.Keys, poet.Constant(fmt.Sprintf("p%v", i)))
				dict.Values = append(dict.Values, typeRefNode(a.Name, f.Name))
				i++
			}
		} else {
			dict.Keys = append(dict.Keys, poet.Constant(fmt.Sprintf("p%v", i)))
			dict.Values = append(dict.Values, poet.Name(a.Name))
			i++
		}
	}
	if len(dict.Keys) == 0 {
		return nil
	}
	return &pyast.Node{
		Node: &pyast.Node_Dict{
			Dict: dict,
		},
	}
}

func makePyType(req *plugin.GenerateRequest, col *plugin.Column) pyType {
	typ := pyInnerType(req, col)
	return pyType{
		InnerType: typ,
		IsArray:   col.IsArray,
		IsNull:    !col.NotNull,
	}
}

func pyInnerType(req *plugin.GenerateRequest, col *plugin.Column) string {
	switch req.Settings.Engine {
	case "postgresql":
		return postgresType(req, col)
	default:
		log.Println("unsupported engine type")
		return "Any"
	}
}

func modelName(name string, settings *plugin.Settings) string {
	out := ""
	for _, p := range strings.Split(name, "_") {
		out += strings.Title(p)
	}
	return out
}

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

func methodName(name string) string {
	snake := matchFirstCap.ReplaceAllString(name, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

var pyIdentPattern = regexp.MustCompile("[^a-zA-Z0-9_]+")

func pyEnumValueName(value string) string {
	id := strings.Replace(value, "-", "_", -1)
	id = strings.Replace(id, ":", "_", -1)
	id = strings.Replace(id, "/", "_", -1)
	id = pyIdentPattern.ReplaceAllString(id, "")
	return strings.ToUpper(id)
}

func buildEnums(req *plugin.GenerateRequest) []Enum {
	var enums []Enum
	for _, schema := range req.Catalog.Schemas {
		if schema.Name == "pg_catalog" || schema.Name == "information_schema" {
			continue
		}
		for _, enum := range schema.Enums {
			var enumName string
			if schema.Name == req.Catalog.DefaultSchema {
				enumName = enum.Name
			} else {
				enumName = schema.Name + "_" + enum.Name
			}
			e := Enum{
				Name:    modelName(enumName, req.Settings),
				Comment: enum.Comment,
			}
			for _, v := range enum.Vals {
				e.Constants = append(e.Constants, Constant{
					Name:  pyEnumValueName(v),
					Value: v,
					Type:  e.Name,
				})
			}
			enums = append(enums, e)
		}
	}
	if len(enums) > 0 {
		sort.Slice(enums, func(i, j int) bool { return enums[i].Name < enums[j].Name })
	}
	return enums
}

func buildModels(conf Config, req *plugin.GenerateRequest) []Struct {
	var structs []Struct
	for _, schema := range req.Catalog.Schemas {
		if schema.Name == "pg_catalog" || schema.Name == "information_schema" {
			continue
		}
		for _, table := range schema.Tables {
			var tableName string
			if schema.Name == req.Catalog.DefaultSchema {
				tableName = table.Rel.Name
			} else {
				tableName = schema.Name + "_" + table.Rel.Name
			}
			structName := tableName
			if !conf.EmitExactTableNames {
				structName = inflection.Singular(inflection.SingularParams{
					Name:       structName,
					Exclusions: conf.InflectionExcludeTableNames,
				})
			}
			if conf.TablePrefix != "" {
				structName = conf.TablePrefix + strings.ToUpper(structName[:1]) + structName[1:]
			}
			s := Struct{
				Table:   plugin.Identifier{Schema: schema.Name, Name: table.Rel.Name},
				Name:    modelName(structName, req.Settings),
				Comment: table.Comment,
			}
			for _, column := range table.Columns {
				typ := makePyType(req, column) // TODO: This used to call compiler.ConvertColumn?
				typ.InnerType = strings.TrimPrefix(typ.InnerType, "models.")
				s.Fields = append(s.Fields, Field{
					Name:    column.Name,
					Type:    typ,
					Comment: column.Comment,
				})
			}
			structs = append(structs, s)
		}
	}
	if len(structs) > 0 {
		sort.Slice(structs, func(i, j int) bool { return structs[i].Name < structs[j].Name })
	}
	return structs
}

func columnName(c *plugin.Column, pos int) string {
	if c.Name != "" {
		return c.Name
	}
	return fmt.Sprintf("column_%d", pos+1)
}

func paramName(p *plugin.Parameter) string {
	if p.Column.Name != "" {
		return p.Column.Name
	}
	return fmt.Sprintf("dollar_%d", p.Number)
}

type pyColumn struct {
	id int32
	*plugin.Column
	embed *pyEmbed
}

type pyEmbed struct {
	modelType string
	modelName string
	fields    []Field
}

// look through all the structs and attempt to find a matching one to embed
// We need the name of the struct and its field names.
func newPyEmbed(embed *plugin.Identifier, structs []Struct, defaultSchema string) *pyEmbed {
	if embed == nil {
		return nil
	}

	for _, s := range structs {
		embedSchema := defaultSchema
		if embed.Schema != "" {
			embedSchema = embed.Schema
		}

		// compare the other attributes
		if embed.Catalog != s.Table.Catalog || embed.Name != s.Table.Name || embedSchema != s.Table.Schema {
			continue
		}

		fields := make([]Field, len(s.Fields))
		for i, f := range s.Fields {
			fields[i] = f
		}

		return &pyEmbed{
			modelType: s.Name,
			modelName: s.Name,
			fields:    fields,
		}
	}

	return nil
}

func columnsToStruct(req *plugin.GenerateRequest, name string, columns []pyColumn) *Struct {
	gs := Struct{
		Name: name,
	}
	seen := map[string]int32{}
	suffixes := map[int32]int32{}
	for i, c := range columns {
		colName := columnName(c.Column, i)
		fieldName := colName

		// override col with expected model name
		if c.embed != nil {
			colName = c.embed.modelName
		}

		// Track suffixes by the ID of the column, so that columns referring to
		// the same numbered parameter can be reused.
		var suffix int32
		if o, ok := suffixes[c.id]; ok {
			suffix = o
		} else if v := seen[colName]; v > 0 {
			suffix = v + 1
		}
		suffixes[c.id] = suffix
		if suffix > 0 {
			fieldName = fmt.Sprintf("%s_%d", fieldName, suffix)
		}
		f := Field{Name: fieldName}
		if c.embed == nil {
			f.Type = makePyType(req, c.Column)
		} else {
			f.Type = pyType{
				InnerType: MODELS_FILENAME + "." + c.embed.modelType,
				IsArray:   c.IsArray,
				IsNull:    !c.NotNull,
			}
			f.EmbedFields = c.embed.fields
		}
		gs.Fields = append(gs.Fields, f)
		seen[colName]++
	}
	return &gs
}

func buildQueries(conf Config, req *plugin.GenerateRequest, structs []Struct) ([]Query, error) {
	rlsFieldsByTable := make(map[string][]string)
	if len(conf.RLSEnforcedFields) > 0 {
		for i := range structs {
			tableName := structs[i].Table.Name
			for _, f := range structs[i].Fields {
				for _, enforced := range conf.RLSEnforcedFields {
					if f.Name == enforced {
						rlsFieldsByTable[tableName] = append(rlsFieldsByTable[tableName], f.Name)
					}
				}
			}
		}
	}

	qs := make([]Query, 0, len(req.Queries))
	for _, query := range req.Queries {
		if query.Name == "" {
			continue
		}
		if query.Cmd == "" {
			continue
		}
		if query.Cmd == metadata.CmdCopyFrom {
			return nil, errors.New("Support for CopyFrom in Python is not implemented")
		}

		methodName := methodName(query.Name)

		gq := Query{
			Cmd:          query.Cmd,
			Comments:     query.Comments,
			MethodName:   methodName,
			FieldName:    sdk.LowerTitle(query.Name) + "Stmt",
			ConstantName: strings.ToUpper(methodName),
			SQL:          query.Text,
			SourceName:   query.Filename,
		}

		qpl := 4
		if conf.QueryParameterLimit != nil {
			qpl = int(*conf.QueryParameterLimit)
		}
		if qpl < 0 {
			return nil, errors.New("invalid query parameter limit")
		}
		enforcedFields := make(map[string]bool)
		// log.Printf("%v\n\n", query)
		for _, c := range query.Columns {
			// log.Printf("%v\n\n", c)
			if fields, ok := rlsFieldsByTable[c.GetTable().GetName()]; ok {
				for _, f := range fields {
					enforcedFields[f] = false
				}
			}
		}
		if len(query.Params) > qpl || qpl == 0 {
			var cols []pyColumn
			for _, p := range query.Params {
				if _, ok := enforcedFields[p.GetColumn().GetName()]; ok {
					enforcedFields[p.Column.Name] = true
				}
				cols = append(cols, pyColumn{
					id:     p.Number,
					Column: p.Column,
				})
			}
			gq.Args = []QueryValue{{
				Emit:   true,
				Name:   "arg",
				Struct: columnsToStruct(req, query.Name+"Params", cols),
			}}
		} else {
			args := make([]QueryValue, 0, len(query.Params))
			for _, p := range query.Params {
				if _, ok := enforcedFields[p.GetColumn().GetName()]; ok {
					enforcedFields[p.Column.Name] = true
				}
				args = append(args, QueryValue{
					Name: paramName(p),
					Typ:  makePyType(req, p.Column),
				})
			}
			gq.Args = args
		}
		for field, is_enforced := range enforcedFields {
			if !is_enforced {
				return nil, fmt.Errorf("RLS field %s is not filtered in query %s", field, query.Name)
			}
		}
		if len(query.Columns) == 1 && query.Columns[0].EmbedTable == nil {
			c := query.Columns[0]
			gq.Ret = QueryValue{
				Name: columnName(c, 0),
				Typ:  makePyType(req, c),
			}
		} else if len(query.Columns) > 1 {
			var gs *Struct
			var emit bool

			for _, s := range structs {
				if len(s.Fields) != len(query.Columns) {
					continue
				}
				same := true

				for i, f := range s.Fields {
					c := query.Columns[i]
					// HACK: models do not have "models." on their types, so trim that so we can find matches
					trimmedPyType := makePyType(req, c)
					trimmedPyType.InnerType = strings.TrimPrefix(trimmedPyType.InnerType, "models.")
					sameName := f.Name == columnName(c, i)
					sameType := f.Type == trimmedPyType
					sameTable := sdk.SameTableName(c.Table, &s.Table, req.Catalog.DefaultSchema)
					if !sameName || !sameType || !sameTable {
						same = false
					}
				}
				if same {
					gs = &s
					break
				}
			}

			if gs == nil {
				var columns []pyColumn
				for i, c := range query.Columns {
					columns = append(columns, pyColumn{
						id:     int32(i),
						Column: c,
						embed:  newPyEmbed(c.EmbedTable, structs, req.Catalog.DefaultSchema),
					})
				}
				gs = columnsToStruct(req, query.Name+"Row", columns)
				emit = true
			}
			gq.Ret = QueryValue{
				Emit:   emit,
				Name:   "i",
				Struct: gs,
			}
		}

		qs = append(qs, gq)
	}
	sort.Slice(qs, func(i, j int) bool { return qs[i].MethodName < qs[j].MethodName })
	// return nil, errors.New("debug")
	return qs, nil
}

func moduleNode(version, source string) *pyast.Module {
	mod := &pyast.Module{
		Body: []*pyast.Node{
			poet.Comment(
				"Code generated by sqlc. DO NOT EDIT.",
			),
			poet.Comment(
				"versions:",
			),
			poet.Comment(
				"  sqlc " + version,
			),
		},
	}
	if source != "" {
		mod.Body = append(mod.Body,
			poet.Comment(
				"source: "+source,
			),
		)
	}
	return mod
}

func importNode(name string) *pyast.Node {
	return &pyast.Node{
		Node: &pyast.Node_Import{
			Import: &pyast.Import{
				Names: []*pyast.Node{
					{
						Node: &pyast.Node_Alias{
							Alias: &pyast.Alias{
								Name: name,
							},
						},
					},
				},
			},
		},
	}
}

func classDefNode(name string, bases ...*pyast.Node) *pyast.Node {
	return &pyast.Node{
		Node: &pyast.Node_ClassDef{
			ClassDef: &pyast.ClassDef{
				Name:  name,
				Bases: bases,
			},
		},
	}
}

func assignNode(target string, value *pyast.Node) *pyast.Node {
	return &pyast.Node{
		Node: &pyast.Node_Assign{
			Assign: &pyast.Assign{
				Targets: []*pyast.Node{
					poet.Name(target),
				},
				Value: value,
			},
		},
	}
}

func constantInt(value int) *pyast.Node {
	return &pyast.Node{
		Node: &pyast.Node_Constant{
			Constant: &pyast.Constant{
				Value: &pyast.Constant_Int{
					Int: int32(value),
				},
			},
		},
	}
}

func subscriptNode(value string, slice *pyast.Node) *pyast.Node {
	return &pyast.Node{
		Node: &pyast.Node_Subscript{
			Subscript: &pyast.Subscript{
				Value: &pyast.Name{Id: value},
				Slice: slice,
			},
		},
	}
}

func optionalKeywordNode(value string, slice *pyast.Node, isArray bool) *pyast.Node {
	v := &pyast.Node{
		Node: &pyast.Node_Subscript{
			Subscript: &pyast.Subscript{
				Value: &pyast.Name{Id: value},
				Slice: slice,
			},
		},
	}
	keyword := poet.Name("None")
	if isArray {
		keyword = poet.Name("[]")
	}
	return &pyast.Node{
		Node: &pyast.Node_Keyword{
			Keyword: &pyast.Keyword{
				Arg:   string(pyprint.Print(v, pyprint.Options{}).Python),
				Value: keyword,
			},
		},
	}
}

func dataclassNode(name string) *pyast.ClassDef {
	return &pyast.ClassDef{
		Name: name,
		DecoratorList: []*pyast.Node{
			{
				Node: &pyast.Node_Call{
					Call: &pyast.Call{
						Func: poet.Attribute(poet.Name("dataclasses"), "dataclass"),
					},
				},
			},
		},
	}
}

func pydanticNode(name string) *pyast.ClassDef {
	return &pyast.ClassDef{
		Name: name,
		Bases: []*pyast.Node{
			{
				Node: &pyast.Node_Attribute{
					Attribute: &pyast.Attribute{
						Value: &pyast.Node{
							Node: &pyast.Node_Name{
								Name: &pyast.Name{Id: "pydantic"},
							},
						},
						Attr: "BaseModel",
					},
				},
			},
		},
	}
}

func fieldNode(f Field) *pyast.Node {
	target := f.Name
	if len(f.EmbedFields) > 0 {
		target = singularize(target)
	}
	return &pyast.Node{
		Node: &pyast.Node_AnnAssign{
			AnnAssign: &pyast.AnnAssign{
				Target:     &pyast.Name{Id: target},
				Annotation: f.Type.Annotation(false),
				Comment:    f.Comment,
			},
		},
	}
}

func typeRefNode(base string, parts ...string) *pyast.Node {
	n := poet.Name(base)
	for _, p := range parts {
		n = poet.Attribute(n, p)
	}
	return n
}

func connMethodNode(method, name string, params ...*pyast.Node) *pyast.Node {
	args := []*pyast.Node{poet.Name(name)}
	args = append(args, params...)
	return &pyast.Node{
		Node: &pyast.Node_Call{
			Call: &pyast.Call{
				Func: typeRefNode("self", "_conn", method),
				Args: args,
			},
		},
	}
}

func buildImportGroup(specs map[string]importSpec) *pyast.Node {
	var body []*pyast.Node
	for _, spec := range buildImportBlock2(specs) {
		if len(spec.Names) > 0 && spec.Names[0] != "" {
			imp := &pyast.ImportFrom{
				Module: spec.Module,
			}
			for _, name := range spec.Names {
				imp.Names = append(imp.Names, poet.Alias(name))
			}
			body = append(body, &pyast.Node{
				Node: &pyast.Node_ImportFrom{
					ImportFrom: imp,
				},
			})
		} else {
			body = append(body, importNode(spec.Module))
		}
	}
	return &pyast.Node{
		Node: &pyast.Node_ImportGroup{
			ImportGroup: &pyast.ImportGroup{
				Imports: body,
			},
		},
	}
}

func buildModelsTree(ctx *pyTmplCtx, i *importer) *pyast.Node {
	mod := moduleNode(ctx.SqlcVersion, "")
	std, pkg := i.modelImportSpecs()
	mod.Body = append(mod.Body, buildImportGroup(std), buildImportGroup(pkg))

	for _, e := range ctx.Enums {
		def := &pyast.ClassDef{
			Name: e.Name,
			Bases: []*pyast.Node{
				poet.Name("str"),
				poet.Attribute(poet.Name("enum"), "Enum"),
			},
		}
		if e.Comment != "" {
			def.Body = append(def.Body, &pyast.Node{
				Node: &pyast.Node_Expr{
					Expr: &pyast.Expr{
						Value: poet.Constant(e.Comment),
					},
				},
			})
		}
		for _, c := range e.Constants {
			def.Body = append(def.Body, assignNode(c.Name, poet.Constant(c.Value)))
		}
		mod.Body = append(mod.Body, &pyast.Node{
			Node: &pyast.Node_ClassDef{
				ClassDef: def,
			},
		})
	}

	for _, m := range ctx.Models {
		var def *pyast.ClassDef
		if ctx.C.EmitPydanticModels {
			def = pydanticNode(m.Name)
		} else {
			def = dataclassNode(m.Name)
		}
		if m.Comment != "" {
			def.Body = append(def.Body, &pyast.Node{
				Node: &pyast.Node_Expr{
					Expr: &pyast.Expr{
						Value: poet.Constant(m.Comment),
					},
				},
			})
		}
		for _, f := range m.Fields {
			def.Body = append(def.Body, fieldNode(f))
		}
		mod.Body = append(mod.Body, &pyast.Node{
			Node: &pyast.Node_ClassDef{
				ClassDef: def,
			},
		})
	}

	return &pyast.Node{Node: &pyast.Node_Module{Module: mod}}
}

func querierClassDef() *pyast.ClassDef {
	return &pyast.ClassDef{
		Name: "Querier",
		Body: []*pyast.Node{
			{
				Node: &pyast.Node_FunctionDef{
					FunctionDef: &pyast.FunctionDef{
						Name: "__init__",
						Args: &pyast.Arguments{
							Args: []*pyast.Arg{
								{
									Arg: "self",
								},
								{
									Arg:        "conn",
									Annotation: typeRefNode("sqlalchemy", "engine", "Connection"),
								},
							},
						},
						Body: []*pyast.Node{
							{
								Node: &pyast.Node_Assign{
									Assign: &pyast.Assign{
										Targets: []*pyast.Node{
											poet.Attribute(poet.Name("self"), "_conn"),
										},
										Value: poet.Name("conn"),
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func asyncQuerierClassDef() *pyast.ClassDef {
	return &pyast.ClassDef{
		Name: "AsyncQuerier",
		Body: []*pyast.Node{
			{
				Node: &pyast.Node_FunctionDef{
					FunctionDef: &pyast.FunctionDef{
						Name: "__init__",
						Args: &pyast.Arguments{
							Args: []*pyast.Arg{
								{
									Arg: "self",
								},
								{
									Arg:        "conn",
									Annotation: typeRefNode("asyncpg", "pool", "PoolConnectionProxy"),
								},
							},
						},
						Body: []*pyast.Node{
							{
								Node: &pyast.Node_Assign{
									Assign: &pyast.Assign{
										Targets: []*pyast.Node{
											poet.Attribute(poet.Name("self"), "_conn"),
										},
										Value: poet.Name("conn"),
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func buildQueryTree(ctx *pyTmplCtx, i *importer, source string) *pyast.Node {
	mod := moduleNode(ctx.SqlcVersion, source)
	std, pkg := i.queryImportSpecs(source)
	mod.Body = append(mod.Body, buildImportGroup(std), buildImportGroup(pkg))
	mod.Body = append(mod.Body, &pyast.Node{
		Node: &pyast.Node_ImportGroup{
			ImportGroup: &pyast.ImportGroup{
				Imports: []*pyast.Node{
					{
						Node: &pyast.Node_ImportFrom{
							ImportFrom: &pyast.ImportFrom{
								Module: ctx.C.Package,
								Names: []*pyast.Node{
									poet.Alias(MODELS_FILENAME),
								},
							},
						},
					},
				},
			},
		},
	})

	for _, q := range ctx.Queries {
		if !ctx.OutputQuery(q.SourceName) {
			continue
		}
		queryText := fmt.Sprintf("-- name: %s \\\\%s\n%s\n", q.MethodName, q.Cmd, q.SQL)
		mod.Body = append(mod.Body, assignNode(q.ConstantName, poet.Constant(queryText)))
		for _, arg := range q.Args {
			if arg.EmitStruct() {
				var def *pyast.ClassDef
				if ctx.C.EmitPydanticModels {
					def = pydanticNode(arg.Struct.Name)
				} else {
					def = dataclassNode(arg.Struct.Name)
				}
				for _, f := range arg.Struct.Fields {
					def.Body = append(def.Body, fieldNode(f))
				}
				mod.Body = append(mod.Body, poet.Node(def))
			}
		}
		if q.Ret.EmitStruct() {
			var def *pyast.ClassDef
			if ctx.C.EmitPydanticModels {
				def = pydanticNode(q.Ret.Struct.Name)
			} else {
				def = dataclassNode(q.Ret.Struct.Name)
			}
			for _, f := range q.Ret.Struct.Fields {
				def.Body = append(def.Body, fieldNode(f))
			}
			mod.Body = append(mod.Body, poet.Node(def))
		}
	}

	cls := asyncQuerierClassDef()
	for _, q := range ctx.Queries {
		if !ctx.OutputQuery(q.SourceName) {
			continue
		}
		f := &pyast.AsyncFunctionDef{
			Name: q.MethodName,
			Args: &pyast.Arguments{
				Args: []*pyast.Arg{
					{
						Arg: "self",
					},
				},
			},
		}

		q.AddArgs(f.Args)

		switch q.Cmd {
		case ":one":
			fetchrow := connMethodNode("fetchrow", q.ConstantName, q.ArgNodes()...)
			f.Body = append(f.Body, assignNode("row", poet.Await(fetchrow)))

			if isAlwaysReturningInsert(q.SQL) {
				f.Returns = q.Ret.Annotation()
			} else {
				f.Body = append(f.Body, poet.Node(
					&pyast.If{
						Test: poet.Node(
							&pyast.Compare{
								Left: poet.Name("row"),
								Ops: []*pyast.Node{
									poet.Is(),
								},
								Comparators: []*pyast.Node{
									poet.Constant(nil),
								},
							},
						),
						Body: []*pyast.Node{
							poet.Return(
								poet.Constant(nil),
							),
						},
					},
				))
				f.Returns = subscriptNode("Optional", q.Ret.Annotation())
			}
			f.Body = append(f.Body, poet.Return(q.Ret.RowNode("row")))
		case ":many":
			cursor := connMethodNode("cursor", q.ConstantName, q.ArgNodes()...)
			f.Body = append(f.Body,
				poet.Node(
					&pyast.AsyncFor{
						Target: poet.Name("row"),
						Iter:   cursor,
						Body: []*pyast.Node{
							poet.Expr(
								poet.Yield(
									q.Ret.RowNode("row"),
								),
							),
						},
					},
				),
			)
			f.Returns = subscriptNode("AsyncIterator", q.Ret.Annotation())
		case ":exec":
			exec := connMethodNode("execute", q.ConstantName, q.ArgNodes()...)
			f.Body = append(f.Body, poet.Await(exec))
			f.Returns = poet.Constant(nil)
		default:
			panic("unknown cmd " + q.Cmd)
		}

		cls.Body = append(cls.Body, poet.Node(f))
	}
	mod.Body = append(mod.Body, poet.Node(cls))

	return poet.Node(mod)
}

type pyTmplCtx struct {
	SqlcVersion string
	Models      []Struct
	Queries     []Query
	Enums       []Enum
	SourceName  string
	C           Config
}

func (t *pyTmplCtx) OutputQuery(sourceName string) bool {
	if t.C.MergeQueryFiles {
		return true
	}
	return t.SourceName == sourceName
}

func HashComment(s string) string {
	return "# " + strings.ReplaceAll(s, "\n", "\n# ")
}

func Generate(_ context.Context, req *plugin.GenerateRequest) (*plugin.GenerateResponse, error) {
	var conf Config
	if len(req.PluginOptions) > 0 {
		if err := json.Unmarshal(req.PluginOptions, &conf); err != nil {
			return nil, err
		}
	}

	enums := buildEnums(req)
	models := buildModels(conf, req)
	queries, err := buildQueries(conf, req, models)
	if err != nil {
		return nil, err
	}

	i := &importer{
		Models:  models,
		Queries: queries,
		Enums:   enums,
		C:       conf,
	}

	tctx := pyTmplCtx{
		Models:      models,
		Queries:     queries,
		Enums:       enums,
		SqlcVersion: req.SqlcVersion,
		C:           conf,
	}

	output := map[string]string{}
	result := pyprint.Print(buildModelsTree(&tctx, i), pyprint.Options{})
	tctx.SourceName = MODELS_FILENAME + ".py"
	output[MODELS_FILENAME+".py"] = string(result.Python)

	files := map[string]struct{}{}
	if i.C.MergeQueryFiles {
		files["db_queries.sql"] = struct{}{}
	} else {
		for _, q := range queries {
			files[q.SourceName] = struct{}{}
		}
	}

	for source := range files {
		tctx.SourceName = source
		result := pyprint.Print(buildQueryTree(&tctx, i, source), pyprint.Options{})
		name := source
		if !strings.HasSuffix(name, ".py") {
			name = strings.TrimSuffix(name, ".sql")
			name += ".py"
		}
		output[name] = string(result.Python)
	}

	resp := plugin.GenerateResponse{}

	for filename, code := range output {
		resp.Files = append(resp.Files, &plugin.File{
			Name:     filename,
			Contents: []byte(code),
		})
	}

	return &resp, nil
}

func isAlwaysReturningInsert(sql string) bool {
	var hasInsert, hasWhere, hasReturning bool
	for _, w := range strings.Fields(sql) {
		switch strings.ToUpper(w) {
		case "INSERT":
			hasInsert = true
		case "WHERE":
			hasWhere = true
		case "RETURNING":
			hasReturning = true
		}
	}
	return hasInsert && hasReturning && !hasWhere
}

func singularize(name string) string {
	irregulars := map[string]string{
		"men":      "man",
		"women":    "woman",
		"children": "child",
		"teeth":    "tooth",
		"feet":     "foot",
		"geese":    "goose",
		"mice":     "mouse",
		"people":   "person",
		"oxen":     "ox",
		"cacti":    "cactus",
		"alumni":   "alumnus",
	}

	name = strings.ToLower(name)

	if singular, found := irregulars[name]; found {
		return singular
	}

	if len(name) == 0 {
		return name
	}

	if strings.HasSuffix(name, "ies") {
		return name[:len(name)-3] + "y"
	} else if strings.HasSuffix(name, "es") {
		// Special cases for -es
		if strings.HasSuffix(name, "ses") || strings.HasSuffix(name, "xes") || strings.HasSuffix(name, "ches") || strings.HasSuffix(name, "shes") {
			return name[:len(name)-2]
		}
		return name[:len(name)-1]
	} else if strings.HasSuffix(name, "s") && len(name) > 1 {
		return name[:len(name)-1]
	}

	return name
}
