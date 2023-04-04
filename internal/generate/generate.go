package generate

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"sort"
)

const FileName = "task-config.generated.md"

func Generate() {
	cwd, _ := os.Getwd()
	filename := filepath.Join(cwd, FileName)
	log.Println(filename)
	t := Register{}
	tasks := t.RegisteredTasks()
	keys := make([]string, 0, len(tasks))
	for k := range tasks {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	output := `
### Available Actions

Each task can be executed using REST endpoints. All tasks tags to provide additional
configuration that is also available on a per-task basis:

`
	for _, k := range keys {
		output += fmt.Sprintf(" * %s(#%s)\n", k, reflect.TypeOf(tasks[k].config).Elem().Name())
	}
	output += "\n---\n"

	for _, k := range keys {
		entry := tasks[k]
		val := reflect.ValueOf(entry.config)

		if !val.IsValid() {
			output += fmt.Sprintf("No config found.\n\n")
			output += "\n---\n"
			continue
		}

		typ := reflect.TypeOf(entry.config).Elem()
		output += fmt.Sprintf("Config symbol: `%s`\n\n", typ.Name())
	}

	if err := os.WriteFile(filename, []byte(output), 0600); err != nil {
		log.Print(err.Error())
	}
}
