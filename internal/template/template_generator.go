package template

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"
)

type Address struct {
	Street  string `json:"street,omitempty"`
	City    string `json:"city,omitempty"`
	State   string `json:"state,omitempty"`
	Zipcode string `json:"zipcode,omitempty"`
}

type Person struct {
	FirstName string  `json:"firstName,omitempty]"`
	Lastname  string  `json:"lastName,omitempty"`
	Age       int     `json:"age,omitempty"`
	Email     string  `json:"email,omitempty"`
	Address   Address `json:"address,omitempty"`
}

// PersonTemplate define a json template for person information
var personTemplate = map[string]interface{}{
	"firstName": "",
	"lastName":  "",
	"age":       0,
	"email":     "",
	"address": map[string]interface{}{
		"street":  "",
		"city":    "",
		"state":   "",
		"zipcode": "",
	},
}

// Define some possible values for each field
var firstNames = []string{"Alice", "Bob", "Charlie", "Dave", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack", "Karen", "Liam", "Mia", "Nate", "Olivia", "Penny", "Quinn", "Rachel", "Sam", "Tom", "Uma", "Violet", "Wendy", "Xander", "Yara", "Zack"}
var lastNames = []string{"Adams", "Brown", "Clark", "Davis", "Edwards", "Ford", "Garcia", "Hernandez", "Ingram", "Jones", "Kim", "Lee", "Miller", "Nelson", "Owens", "Patel", "Quinn", "Rodriguez", "Smith", "Taylor", "Ulrich", "Vargas", "Williams", "Xu", "Yi", "Zhang"}
var ageChoices = []int{18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100}
var emailChoices = []string{"example.com", "test.com", "world.com", "gmail.com", "yahoo.com", "apple.com"}
var streetChoices = []string{"123 Main St", "456 Elm St", "789 Oak St"}
var cityChoices = []string{"New York", "San Francisco", "Chicago"}
var stateChoices = []string{"NY", "CA", "IL"}
var zipcodeChoices = []string{"12345", "67890", "54321"}

// GenerateRandomJSON generate a random JSON object based on the template
func generateRandomJSON(template map[string]interface{}) map[string]interface{} {
	data := make(map[string]interface{})

	for key, value := range template {
		if subDoc, ok := value.(map[string]interface{}); ok {
			data[key] = generateRandomJSON(subDoc)
		} else {
			switch key {
			case "firstName":
				data[key] = firstNames[rand.Intn(len(firstNames))]
			case "lastName":
				data[key] = lastNames[rand.Intn(len(lastNames))]
			case "age":
				data[key] = ageChoices[rand.Intn(len(ageChoices))]
			case "email":
				firstName := data["firstName"].(string)
				lastName := data["lastName"].(string)
				data[key] = fmt.Sprintf("%s.%s@%s", firstName, lastName, emailChoices[rand.Intn(len(emailChoices))])
			case "street":
				data[key] = streetChoices[rand.Intn(len(streetChoices))]
			case "city":
				data[key] = cityChoices[rand.Intn(len(cityChoices))]
			case "state":
				data[key] = stateChoices[rand.Intn(len(stateChoices))]
			case "zipcode":
				data[key] = zipcodeChoices[rand.Intn(len(zipcodeChoices))]
			}
		}
	}
	return data
}

// GeneratePerson return a Person with random details
func GeneratePerson() (Person, error) {
	rand.Seed(time.Now().UnixNano())

	personData := generateRandomJSON(personTemplate)
	personStr, err := json.Marshal(personData)
	if err != nil {
		return Person{}, err
	}

	p := &Person{}
	if err := json.Unmarshal(personStr, p); err != nil {
		return Person{}, err
	}
	return *p, nil
}
