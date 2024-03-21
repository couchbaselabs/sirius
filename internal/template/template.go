package template

import (

	"errors"
	"reflect"
	"strings"
	"unsafe"

	"github.com/bgadrian/fastfaker/faker"
	"github.com/iancoleman/strcase"

)

const (
	MutatedPath             string  = "mutated"
	MutatedPathDefaultValue float64 = 0
	MutateFieldIncrement    float64 = 1
)

type Template interface {
	GenerateDocument(fake *faker.Faker, key string, documentSize int) interface{}
	UpdateDocument(fieldsToChange []string, lastUpdatedDocument interface{}, documentSize int,
		fake *faker.Faker) (interface{}, error)
	Compare(document1 interface{}, document2 interface{}) (bool, error)
	GenerateIndexes(bucketName string, scopeName string, collectionName string) ([]string, error)
	GenerateQueries(bucketName string, scopeName string, collectionName string) ([]string, error)
	GenerateIndexesForSdk() (map[string][]string, error)
	GenerateSubPathAndValue(fake *faker.Faker, subDocSize int) map[string]any
	GetValues(interface{}) (interface{}, error)
}

// InitialiseTemplate returns a template as an interface defined by user request.
func InitialiseTemplate(template string) Template {
	switch strings.ToLower(template) {
	case "person":
		return &Person{}
	case "hotel":
		return &Hotel{}
	case "small":
		return &Small{}
	case "product":
		return &Product{}
	case "person_sql":
		return &PersonSql{}
	case "hotel_sql":
		return &HotelSql{}
	case "small_sql":
		return &SmallSql{}
	default:
		return &Person{}
	}
}

func calculateSizeOfStructRecursive(person interface{}) int {
	value := reflect.ValueOf(person)
	size := int(unsafe.Sizeof(person))

	if value.Kind() != reflect.Struct {
		return size
	}

	numFields := value.NumField()
	for i := 0; i < numFields; i++ {
		field := value.Field(i)
		switch field.Kind() {
		case reflect.String:
			size += len(field.String())
		case reflect.Float64:
			size += int(unsafe.Sizeof(float64(0)))
		case reflect.Int64:
			size += int(unsafe.Sizeof(int64(0)))
		case reflect.Slice:
			// Slice can either be of strings or of a struct.
			if field.Type().Elem().Kind() == reflect.String {
				for j := 0; j < field.Len(); j++ {
					size += len(field.Index(j).String())
				}
			}
			if field.Type().Elem().Kind() == reflect.Struct {
				for j := 0; j < field.Len(); j++ {
					size += calculateSizeOfStruct(field.Index(j).Interface())
				}
			}
		case reflect.Struct:
			size += calculateSizeOfStruct(field.Interface())

		case reflect.Map:
			keys := field.MapKeys()
			for _, key := range keys {
				size += len(key.String()) // Assuming keys are strings
				value := field.MapIndex(key)
				if value.IsValid() {
					size += len(value.String())
				}
			}
		}
	}

	return size
}

func calculateSizeOfStruct(person interface{}) int {
	value := reflect.ValueOf(person)
	size := int(unsafe.Sizeof(person))

	//if value.Kind() != reflect.Struct {
	//	return size
	//}
	// if the value is a pointer then dereference it.
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}

	numFields := value.NumField()
	for i := 0; i < numFields; i++ {
		field := value.Field(i)
		switch field.Kind() {
		case reflect.String:
			size += len(field.String())
		case reflect.Float64:
			size += int(unsafe.Sizeof(float64(0)))
		case reflect.Int64:
			size += int(unsafe.Sizeof(int64(0)))
		case reflect.Slice:
			// Slice can either be of strings or of a struct.
			if field.Type().Elem().Kind() == reflect.String {
				for j := 0; j < field.Len(); j++ {
					size += len(field.Index(j).String())
				}
			}
			if field.Type().Elem().Kind() == reflect.Struct {
				for j := 0; j < field.Len(); j++ {
					size += calculateSizeOfStruct(field.Index(j).Interface())
				}
			}
		case reflect.Struct:
			size += calculateSizeOfStructRecursive(field.Interface())

		case reflect.Map:
			keys := field.MapKeys()
			for _, key := range keys {
				size += len(key.String()) // Assuming keys are strings
				value := field.MapIndex(key)
				if value.IsValid() {
					size += len(value.String())
				}
			}
		}
	}

	return size
}

func StructToMap(obj interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	val := reflect.ValueOf(obj)

	// If it is a pointer, then dereferencing it to get value.
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	typ := val.Type()

	// Iterating through the fields of the struct
	for i := 0; i < val.NumField(); i++ {

		fieldName := typ.Field(i).Name
		// Converting to snake case.
		fieldName = strcase.ToSnake(fieldName)
		fieldValue1 := val.Field(i)
		fieldValueKind := val.Field(i).Kind()
		var fieldValue interface{}

		// If the field is a struct, recursively call structToMap to get the map representation of the nested struct.
		// Otherwise, get the field value directly.
		if fieldValueKind == reflect.Struct {
			fieldValue = StructToMap(val.Field(i).Interface())
		} else if fieldValueKind == reflect.Slice {
			if fieldValue1.Type().Elem().Kind() == reflect.Struct {
				var arr []interface{}
				for j := 0; j < fieldValue1.Len(); j++ {
					arr = append(arr, StructToMap(fieldValue1.Index(j).Interface()))
				}
				fieldValue = arr
			} else {
				var arr []interface{}
				for i := 0; i < fieldValue1.Len(); i++ {
					arr = append(arr, fieldValue1.Index(i).Interface())
				}
				fieldValue = arr
			}
		} else {
			fieldValue = val.Field(i).Interface()
		}

		result[fieldName] = fieldValue
	}
	return result
}

// GetAvroSchema returns the Avro Schema for the given template name.
func GetAvroSchema(templateName string) (string, error) {
	switch strings.ToLower(templateName) {
	case "hotel":
		hotelAvroSchema := `{
			"name": "Hotel",	
			"type": "record",
			"fields": [
				{"name": "id", "type": "string"},
				{"name": "country", "type": "string", "default": ""},
				{"name": "address", "type": "string", "default": ""},
				{"name": "free_parking", "type": "boolean", "default": false},
				{"name": "city", "type": "string", "default": ""},
				{"name": "template_name", "type": "string"},
				{"name": "url", "type": "string", "default": ""},
				{
					"name": "reviews",
					"type": {
						"type": "array",
						"items": {
							"name": "Review",
							"type": "record",
							"fields": [
								{"name": "date", "type": "string", "default": ""},
								{"name": "author", "type": "string", "default": ""},
								{
									"name": "rating",
									"type": "record",
									"fields": [
										{"name": "rating_value", "type": "double", "default": 0.0},
										{"name": "cleanliness", "type": "double", "default": 0.0},
										{"name": "overall", "type": "double", "default": 0.0},
										{"name": "checkin", "type": "double", "default": 0.0},
										{"name": "rooms", "type": "double", "default": 0.0}
									]
								}
							]
						},
						"default": []
					}
				},
				{"name": "phone", "type": "string", "default": ""},
				{"name": "price", "type": "double", "default": 0.0},
				{"name": "avg_rating", "type": "double", "default": 0.0},
				{"name": "free_breakfast", "type": "boolean", "default": false},
				{"name": "name", "type": "string", "default": ""},
				{"name": "public_likes", "type": {"type": "array", "items": "string"}, "default": []},
				{"name": "email", "type": "string", "default": ""},
				{"name": "mutated", "type": "double", "default": 0.0},
				{"name": "padding", "type": "string", "default": ""}
			]
		}`
		return hotelAvroSchema, nil
	case "person":
		personAvroSchema := `{
			"name": "Person",
			"type": "record",
			"fields": [
				{"name": "id", "type": "string"},
				{"name": "template_name", "type": "string", "default": "Person"},
				{"name": "first_name", "type": "string", "default": ""},
				{"name": "age", "type": "double", "default": 0.0},
				{"name": "email", "type": "string", "default": ""},
				{
					"name": "address",
					"type": "record",
					"fields": [
						{"name": "city", "type": "string", "default": ""},
						{"name": "state", "type": "string", "default": ""}
					]
				},
				{"name": "gender", "type": "string", "default": ""},
				{"name": "marital_status", "type": "string", "default": ""},
				{"name": "hobbies", "type": "string", "default": ""},
				{
					"name": "attributes",
					"type": "record",
					"fields": [
						{"name": "weight", "type": "double", "default": 0.0},
						{"name": "height", "type": "double", "default": 0.0},
						{"name": "colour", "type": "string", "default": ""},
						{
							"name": "hair",
							"type": "record",
							"fields": [
								{"name": "hair_type", "type": "string", "default": ""},
								{"name": "colour", "type": "string", "default": ""},
								{"name": "length", "type": "string", "default": ""},
								{"name": "thickness", "type": "string", "default": ""}
							]
						},
						{"name": "body_type", "type": "string", "default": ""}
					]
				},
				{"name": "mutated", "type": "double", "default": 0.0},
				{"name": "padding", "type": "string", "default": ""}
			]
		}`
		return personAvroSchema, nil
	case "product":
		productAvroSchema := `{
			"name": "Product",
			"type": "record",
			"fields": [
				{"name": "id", "type": "string"},
				{"name": "product_name", "type": "string", "default": ""},
				{"name": "product_link", "type": "string", "default": ""},
				{"name": "product_features", "type": "array", "items": "string"},
				{"name": "product_specs", "type": "map", "values": "string"},
				{"name": "product_image_links", "type": "array", "items": "string"},
				{
					"name": "product_reviews",
					"type": {
						"type": "array",
						"items": {
							"name": "ProductReview",
							"type": "record",
							"fields": [
								{"name": "date", "type": "string", "default": ""},
								{"name": "author", "type": "string", "default": ""},
								{
									"name": "product_rating",
									"type": "record",
									"fields": [
										{"name": "rating_value", "type": "double", "default": 0.0},
										{"name": "performance", "type": "double", "default": 0.0},
										{"name": "usability", "type": "double", "default": 0.0},
										{"name": "pricing", "type": "double", "default": 0.0},
										{"name": "build_quality", "type": "double", "default": 0.0}
									]
								}
							]
						},
						"default": []
					}
				},
				{"name": "product_category", "type": "array", "items": "string"},
				{"name": "price", "type": "double", "default": 0.0},
				{"name": "avg_rating", "type": "double", "default": 0.0},
				{"name": "num_sold", "type": "int", "default": 0},
				{"name": "upload_date", "type": "string", "default": ""},
				{"name": "weight", "type": "double", "default": 0.0},
				{"name": "quantity", "type": "int", "default": 0},
				{"name": "seller_name", "type": "string", "default": ""},
				{"name": "seller_location", "type": "string", "default": ""},
				{"name": "seller_verified", "type": "boolean", "default": false},
				{"name": "template_name", "type": "string", "default": "Product"},
				{"name": "mutated", "type": "double", "default": 0.0},
				{"name": "padding", "type": "string", "default": ""}
			]
		}`
		return productAvroSchema, nil
	default:
		return "", errors.New("invalid template name OR avro schema not defined for given template name")
	}
}
