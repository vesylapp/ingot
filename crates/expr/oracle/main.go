// Command exproracle runs Inngest's Go expression package over JSON lines
// ({"expr": ..., "data": ...}) from stdin and prints what it returns for each:
// the Go type and %v of the value, any evaluation error, and whether the
// expression passes the default and the restrictive validation policies.
//
// regen.sh builds it inside an Inngest checkout; it is not part of ingot.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/inngest/inngest/pkg/expressions"
)

type in struct {
	Expr string         `json:"expr"`
	Data map[string]any `json:"data"`
}

func main() {
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	enc := json.NewEncoder(os.Stdout)
	for sc.Scan() {
		var i in
		if err := json.Unmarshal(sc.Bytes(), &i); err != nil {
			fmt.Println(`{"bad":true}`)
			continue
		}
		out := map[string]any{"expr": i.Expr, "data": i.Data}
		func() {
			defer func() {
				if r := recover(); r != nil {
					out["panic"] = fmt.Sprint(r)
				}
			}()
			if i.Data == nil {
				i.Data = map[string]any{}
			}
			val, err := expressions.Evaluate(context.Background(), i.Expr, i.Data)
			if err != nil {
				out["err"] = err.Error()
			}
			out["type"] = fmt.Sprintf("%T", val)
			out["v"] = fmt.Sprintf("%v", val)
			if t, ok := val.(time.Time); ok {
				out["v"] = t.UTC().Format(time.RFC3339Nano)
			}
			if verr := expressions.Validate(context.Background(), expressions.DefaultRestrictiveValidationPolicy(), i.Expr); verr != nil {
				out["restrictive"] = false
			} else {
				out["restrictive"] = true
			}
			if verr := expressions.Validate(context.Background(), nil, i.Expr); verr != nil {
				out["checkerr"] = verr.Error()
			}
		}()
		_ = enc.Encode(out)
	}
}
