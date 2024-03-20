package template

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/bgadrian/fastfaker/faker"
)

type HotelSql struct {
	ID            string  `json:"id" bson:"_id"`
	Country       string  `json:"country,omitempty"`
	Address       string  `json:"address,omitempty"`
	FreeParking   bool    `json:"free_parking,omitempty"`
	City          string  `json:"city,omitempty"`
	TemplateType  string  `json:"template_type"`
	URL           string  `json:"url,omitempty"`
	Phone         string  `json:"phone,omitempty"`
	Price         float64 `json:"price,omitempty"`
	AvgRating     float64 `json:"avg_rating,omitempty"`
	FreeBreakfast bool    `json:"free_breakfast,omitempty"`
	Name          string  `json:"name,omitempty"`
	Email         string  `json:"email,omitempty"`
	Mutated       float64 `json:"mutated"`
	Padding       string  `json:"padding"`
	Value         []interface{}
}

func (h *HotelSql) GenerateDocument(fake *faker.Faker, key string, documentSize int) interface{} {

	hNew := HotelSql{}
	hNew.ID = key
	hNew.Country = fake.Country()
	hNew.Address = fake.Address().Address
	hNew.FreeParking = fake.Bool()
	hNew.City = fake.Address().City
	hNew.TemplateType = "hotel_sql"
	hNew.URL = fake.URL()
	hNew.Phone = fake.Phone()
	hNew.Price = fake.Price(1000, 100000)
	hNew.AvgRating = fake.Float64Range(1, 5)
	hNew.FreeBreakfast = fake.Bool()
	hNew.Name = fake.BeerName()
	hNew.Email = fake.URL()
	hNew.Mutated = MutatedPathDefaultValue
	hNew.Value = []interface{}{&hNew.ID, &hNew.Address, &hNew.FreeParking, &hNew.City, &hNew.URL, &hNew.Phone, &hNew.Price, &hNew.AvgRating, &hNew.FreeBreakfast, &hNew.Name,
		&hNew.Email, &hNew.Padding, &hNew.Mutated}

	return &hNew
}

func (h *HotelSql) UpdateDocument(fieldsToChange []string, lastUpdatedDocument interface{}, documentSize int,
	fake *faker.Faker) (interface{}, error) {

	hotel, ok := lastUpdatedDocument.(*HotelSql)
	if !ok {
		return nil, fmt.Errorf("unable to decode last updated document to hotel sql template in update doc")
	}

	checkFields := make(map[string]struct{})
	for _, s := range fieldsToChange {
		checkFields[s] = struct{}{}
	}

	if _, ok := checkFields["country"]; ok || len(checkFields) == 0 {
		hotel.Country = fake.Country()
	}
	if _, ok := checkFields["address"]; ok || len(checkFields) == 0 {
		hotel.Address = fake.Address().Address
	}
	if _, ok := checkFields["free_parking"]; ok || len(checkFields) == 0 {
		hotel.FreeParking = fake.Bool()
	}
	if _, ok := checkFields["city"]; ok || len(checkFields) == 0 {
		hotel.City = fake.Address().City
	}
	if _, ok := checkFields["url"]; ok || len(checkFields) == 0 {
		hotel.URL = fake.URL()
	}
	if _, ok := checkFields["phone"]; ok || len(checkFields) == 0 {
		hotel.Phone = fake.Phone()
	}
	if _, ok := checkFields["price"]; ok || len(checkFields) == 0 {
		hotel.Price = fake.Float64Range(1, 5)
	}
	if _, ok := checkFields["avg_rating"]; ok || len(checkFields) == 0 {
		hotel.AvgRating = fake.Float64Range(1, 5)
	}
	if _, ok := checkFields["free_breakfast"]; ok || len(checkFields) == 0 {
		hotel.FreeBreakfast = fake.Bool()
	}
	if _, ok := checkFields["name"]; ok || len(checkFields) == 0 {
		hotel.Name = fake.BeerName()
	}
	if _, ok := checkFields["email"]; ok || len(checkFields) == 0 {
		hotel.Email = fake.URL()
	}
	hotel.Padding = ""
	currentDocSize := calculateSizeOfStruct(hotel)
	if (currentDocSize) < int(documentSize) {
		hotel.Padding = strings.Repeat("a", int(documentSize)-(currentDocSize))
	}
	values := []interface{}{&hotel.ID, &hotel.Address, &hotel.FreeParking, &hotel.City, &hotel.URL, &hotel.Phone, &hotel.Price, &hotel.AvgRating, &hotel.FreeBreakfast, &hotel.Name,
		&hotel.Email, &hotel.Padding, &hotel.Mutated}
	hotel.Value = values
	return hotel, nil
}

func (h *HotelSql) Compare(document1 interface{}, document2 interface{}) (bool, error) {
	p1, ok := document1.(*HotelSql)
	if !ok {
		return false, fmt.Errorf("unable to decode first document to hotel_sql template")
	}
	p2, ok := document2.(*HotelSql)
	if !ok {
		return false, fmt.Errorf("unable to decode second document to hotel_sql template")
	}

	return reflect.DeepEqual(p1, p2), nil
}

func (h *HotelSql) GenerateIndexes(bucketName string, scopeName string, collectionName string) ([]string, error) {
	return []string{}, errors.New("not implemented")
}

func (h *HotelSql) GenerateQueries(bucketName string, scopeName string, collectionName string) ([]string, error) {
	return []string{}, errors.New("not implemented")
}

func (h *HotelSql) GenerateIndexesForSdk() (map[string][]string, error) {
	return map[string][]string{}, errors.New("not implemented")
}

func (h *HotelSql) GenerateSubPathAndValue(fake *faker.Faker, subDocSize int) map[string]any {
	return map[string]interface{}{
		"_1": strings.Repeat(fake.Letter(), subDocSize),
	}
}
func (h *HotelSql) GetValues(document interface{}) (interface{}, error) {
	hotel, ok := document.(*HotelSql)
	if !ok {
		return nil, fmt.Errorf("unable to decode last updated document to hotel template")
	}
	return hotel.Value, nil
}
