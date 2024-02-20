package sirius_documentation

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/barkha06/sirius/internal/db"
	"github.com/barkha06/sirius/internal/task_result"
	"github.com/barkha06/sirius/internal/tasks"
	"github.com/barkha06/sirius/internal/tasks/util_sirius"
)

const FileName = "task-config.generated.md"

func Generate() {
	cwd, _ := os.Getwd()
	filename := filepath.Join(cwd, FileName)
	log.Println(filename)
	t := Register{}
	tk := t.RegisteredTasks()
	keys := make([]string, 0, len(tk))
	for k := range tk {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	output := `
### Available Actions

Each task can be executed using REST endpoints. All tasks tags to provide additional
configuration that is also available on a per-task basis:

`
	for _, k := range keys {
		a := strings.Replace(strings.ToLower(k), "/", "", 1)
		output += fmt.Sprintf(" * [%s](#%s)\n", k, a)
	}
	output += "\n---\n"

	for _, k := range keys {
		entry := tk[k]
		x, ok := entry.Config.(tasks.Task)
		if !ok {
			continue
		}
		val := reflect.ValueOf(x)
		output += fmt.Sprintf("#### %s\n\n", k)
		output += fmt.Sprintf(" REST : %s\n\n", entry.HttpMethod)
		output += fmt.Sprintf("Description : %s\n\n", x.Describe())

		if !val.IsValid() {
			output += fmt.Sprintf("No config found.\n\n")

			output += "\n---\n"
			continue
		}

		n := val.Elem().NumField()
		if n == 0 {
			output += fmt.Sprintf("No fields found on struct.\n")
			continue
		}
		output += "| Name | Type | JSON Tag |\n"
		output += "| ---- | ---- | -------- |\n"
		for i := 0; i < val.Elem().NumField(); i++ {
			f := val.Elem().Type().Field(i)
			if _, ok := f.Tag.Lookup("json"); !ok {
				continue
			}
			// doc
			if tagContent, ok := f.Tag.Lookup("doc"); !ok {
				continue
			} else {
				if tagContent == "false" {
					continue
				}
			}
			// Name
			output += "| `" + f.Name + "` "

			// Type
			n := f.Type.Name()
			k := f.Type.Kind().String()
			if n == k {
				output += "| `" + n + "` "
			} else {
				output += "| `" + k + "` "
			}

			// JSON
			for _, tagName := range []string{"json"} {
				if tagContents, ok := f.Tag.Lookup(tagName); ok {
					output += "| `" + tagName + ":" + tagContents + "` "
					continue
				}
				output += "| "
			}
			// Close last table column
			output += " |\n"
		}
		s := tasks.DatabaseInformation{}
		hVal := reflect.ValueOf(&s)
		for i := 0; i < hVal.Elem().NumField(); i++ {
			f := hVal.Elem().Type().Field(i)
			if _, ok := f.Tag.Lookup("json"); !ok {
				continue
			}

			// doc
			if tagContent, ok := f.Tag.Lookup("doc"); !ok {
				continue
			} else {
				if tagContent == "false" {
					continue
				}
			}

			// Name
			output += "| `" + f.Name + "` "

			// Type
			n := f.Type.Name()
			k := f.Type.Kind().String()
			if n == k {
				output += "| `" + n + "` "
			} else {
				output += "| `" + k + "` "
			}

			for _, tagName := range []string{"json"} {
				if tagContents, ok := f.Tag.Lookup(tagName); ok {
					output += "| `" + tagName + ":" + tagContents + "` "
					continue
				}
				output += "| "
			}
			// Close last table column
			output += " |\n"
		}
		output += "\n---\n"
	}

	// END - API ends here

	output += "**Description of Extra Parameters**.\n\n"
	s := db.Extras{}
	hVal := reflect.ValueOf(&s)
	output += "| Name | Type | JSON Tag |\n"
	output += "| ---- | ---- | -------- |\n"
	for i := 0; i < hVal.Elem().NumField(); i++ {
		f := hVal.Elem().Type().Field(i)
		if _, ok := f.Tag.Lookup("json"); !ok {
			continue
		}

		// doc
		if tagContent, ok := f.Tag.Lookup("doc"); !ok {
			continue
		} else {
			if tagContent == "false" {
				continue
			}
		}

		// Name
		output += "| `" + f.Name + "` "

		// Type
		n := f.Type.Name()
		k := f.Type.Kind().String()
		if n == k {
			output += "| `" + n + "` "
		} else {
			output += "| `" + k + "` "
		}

		for _, tagName := range []string{"json"} {
			if tagContents, ok := f.Tag.Lookup(tagName); ok {
				output += "| `" + tagName + ":" + tagContents + "` "
				continue
			}
			output += "| "
		}
		// Close last table column
		output += " |\n"
	}

	output += "\n---\n"
	// End - Extra Parameter ends here

	output += "Possible values for durability :-\n" +
		"1. NONE\n" +
		"2. MAJORITY\n" +
		"3. MAJORITY_AND_PERSIST_TO_ACTIVE\n" +
		"4. PERSIST_TO_MAJORITY\n\n"
	output += "\n---\n"
	// End - Durability helper

	output += "**APIs Response Description**.\n\n" +
		"1. Response after initiating a TASK.\n\n"

	apiRespVal := reflect.ValueOf(&util_sirius.TaskResponse{})
	output += "| Name | Type | JSON Tag |\n"
	output += "| ---- | ---- | -------- |\n"

	for i := 0; i < apiRespVal.Elem().NumField(); i++ {
		f := apiRespVal.Elem().Type().Field(i)
		if _, ok := f.Tag.Lookup("json"); !ok {
			continue
		}
		// Name
		output += "| `" + f.Name + "` "

		// Type
		n := f.Type.Name()
		k := f.Type.Kind().String()
		if n == k {
			output += "| `" + n + "` "
		} else {
			output += "| `" + k + "` "
		}

		// JSON
		for _, tagName := range []string{"json"} {
			if tagContents, ok := f.Tag.Lookup(tagName); ok {
				output += "| `" + tagName + ":" + tagContents + "` "
				continue
			}
			output += "| "
		}
		// Close last table column
		output += " |\n"
	}

	output += "\n---\n"

	output += "2. Response which contains the TASK's result.\n\n"

	taskRespVal := reflect.ValueOf(&task_result.TaskResult{})
	output += "| Name | Type | JSON Tag |\n"
	output += "| ---- | ---- | -------- |\n"

	for i := 0; i < taskRespVal.Elem().NumField(); i++ {
		f := taskRespVal.Elem().Type().Field(i)
		if v, ok := f.Tag.Lookup("json"); !ok || v == "-" {
			continue
		}
		// Name
		output += "| `" + f.Name + "` "

		// Type
		n := f.Type.Name()
		k := f.Type.Kind().String()
		if n == k {
			output += "| `" + n + "` "
		} else {
			output += "| `" + k + "` "
		}

		// JSON
		for _, tagName := range []string{"json"} {
			if tagContents, ok := f.Tag.Lookup(tagName); ok {
				output += "| `" + tagName + ":" + tagContents + "` "
				continue
			}
			output += "| "
		}
		// Close last table column
		output += " |\n"
	}
	output += "\n---\n"

	output += "**Helping nested json values n**.\n\n"
	tt := t.HelperStruct()
	tagKeys := make([]string, 0, len(t.HelperStruct()))
	for k := range tt {
		tagKeys = append(tagKeys, k)
	}
	sort.Strings(tagKeys)

	for _, k := range tagKeys {
		a := strings.Replace(strings.ToLower(k), "/", "", 1)
		output += fmt.Sprintf(" * [%s](#%s)\n", k, a)
	}
	output += "\n---\n"

	for _, k := range tagKeys {
		output += fmt.Sprintf("#### %s\n\n", k)
		s := tt[k]
		hVal := reflect.ValueOf(s)
		output += "| Name | Type | JSON Tag |\n"
		output += "| ---- | ---- | -------- |\n"
		for i := 0; i < hVal.Elem().NumField(); i++ {
			f := hVal.Elem().Type().Field(i)
			if _, ok := f.Tag.Lookup("json"); !ok {
				continue
			}

			// doc
			if tagContent, ok := f.Tag.Lookup("doc"); !ok {
				continue
			} else {
				if tagContent == "false" {
					continue
				}
			}

			// Name
			output += "| `" + f.Name + "` "

			// Type
			n := f.Type.Name()
			k := f.Type.Kind().String()
			if n == k {
				output += "| `" + n + "` "
			} else {
				output += "| `" + k + "` "
			}

			for _, tagName := range []string{"json"} {
				if tagContents, ok := f.Tag.Lookup(tagName); ok {
					output += "| `" + tagName + ":" + tagContents + "` "
					continue
				}
				output += "| "
			}
			// Close last table column
			output += " |\n"
		}
	}
	output += "\n---\n"

	if err := os.WriteFile(filename, []byte(output), 0600); err != nil {
		log.Print(err.Error())
	}
}
