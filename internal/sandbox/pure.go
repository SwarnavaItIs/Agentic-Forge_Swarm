package sandbox

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
)

// The controlled sum-v1 profile evaluates one pure Add expression. Restricting
// its AST prevents init/TestMain/import side effects from bypassing the trusted
// runner. This is deliberately narrower than arbitrary Go repository support.
func pureAdd(source []byte) bool {
	return pureFunction(source, "Add", 2)
}
func pureFunction(source []byte, name string, arity int) bool {
	if len(source) == 0 || len(source) > 8192 || bytes.Contains(source, []byte("//go:")) {
		return false
	}
	file, err := parser.ParseFile(token.NewFileSet(), "sum.go", source, 0)
	if err != nil || file.Name.Name != "main" || len(file.Decls) != 1 {
		return false
	}
	fn, ok := file.Decls[0].(*ast.FuncDecl)
	if !ok || fn.Name.Name != name || fn.Recv != nil || fn.Type.TypeParams != nil || fn.Type.Params == nil || fn.Type.Results == nil || len(fn.Type.Results.List) != 1 || fn.Body == nil || len(fn.Body.List) != 1 {
		return false
	}
	typ, ok := fn.Type.Results.List[0].Type.(*ast.Ident)
	if !ok || typ.Name != "int" {
		return false
	}
	parameters := map[string]bool{}
	for _, field := range fn.Type.Params.List {
		typ, ok = field.Type.(*ast.Ident)
		if !ok || typ.Name != "int" {
			return false
		}
		for _, name := range field.Names {
			if name.Name == "_" || parameters[name.Name] {
				return false
			}
			parameters[name.Name] = true
		}
	}
	if len(parameters) != arity {
		return false
	}
	ret, ok := fn.Body.List[0].(*ast.ReturnStmt)
	if !ok || len(ret.Results) != 1 {
		return false
	}
	nodes := 0
	var expression func(ast.Expr, int) bool
	expression = func(expr ast.Expr, depth int) bool {
		nodes++
		if nodes > 128 || depth > 24 {
			return false
		}
		switch value := expr.(type) {
		case *ast.Ident:
			return parameters[value.Name]
		case *ast.BasicLit:
			if value.Kind != token.INT {
				return false
			}
			_, err := strconv.ParseInt(value.Value, 0, 32)
			return err == nil
		case *ast.ParenExpr:
			return expression(value.X, depth+1)
		case *ast.UnaryExpr:
			return (value.Op == token.ADD || value.Op == token.SUB) && expression(value.X, depth+1)
		case *ast.BinaryExpr:
			return (value.Op == token.ADD || value.Op == token.SUB || value.Op == token.MUL || value.Op == token.QUO || value.Op == token.REM) && expression(value.X, depth+1) && expression(value.Y, depth+1)
		default:
			return false
		}
	}
	return expression(ret.Results[0], 0)
}
