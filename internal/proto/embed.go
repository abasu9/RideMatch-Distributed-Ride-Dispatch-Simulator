package proto

import (
	"bytes"
	"embed"
)

//go:embed schemas/*.proto
var schemaFS embed.FS

func mustReadSchemas() map[string]string {
	entries := []string{
		"schemas/driver.proto",
		"schemas/matching.proto",
		"schemas/rider.proto",
	}
	out := make(map[string]string, len(entries))
	for _, path := range entries {
		data, err := schemaFS.ReadFile(path)
		if err != nil {
			panic(err)
		}
		base := path[len("schemas/"):]
		out[base] = string(bytes.Clone(data))
	}
	return out
}
