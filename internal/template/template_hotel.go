package template

import (
	"errors"
	"fmt"
	"github.com/bgadrian/fastfaker/faker"
	"reflect"
	"strings"
)

type Rating struct {
	Value       float64 `json:"value,omitempty"`
	Cleanliness float64 `json:"Cleanliness,omitempty"`
	Overall     float64 `json:"Overall,omitempty"`
	CheckIn     float64 `json:"Check in / front desk,omitempty"`
	Rooms       float64 `json:"Rooms,omitempty"`
}
type Review struct {
	Date   string `json:"date,omitempty"`
	Author string `json:"author,omitempty"`
	Rating Rating `json:"rating,omitempty"`
}

type Hotel struct {
	ID            string   `json:"_id" bson:"_id"`
	Country       string   `json:"country,omitempty"`
	Address       string   `json:"address,omitempty"`
	FreeParking   bool     `json:"free_parking,omitempty"`
	City          string   `json:"city,omitempty"`
	Type          string   `json:"type"`
	URL           string   `json:"url,omitempty"`
	Reviews       []Review `json:"reviews,omitempty"`
	Phone         string   `json:"phone,omitempty"`
	Price         float64  `json:"price,omitempty"`
	AvgRating     float64  `json:"avg_rating,omitempty"`
	FreeBreakfast bool     `json:"free_breakfast,omitempty"`
	Name          string   `json:"name,omitempty"`
	PublicLikes   []string `json:"public_likes,omitempty"`
	Email         string   `json:"email,omitempty"`
	Mutated       float64  `json:"mutated"`
	Padding       string   `json:"padding"`
}

func buildReview(fake *faker.Faker, length int32) []Review {
	var r []Review
	for i := 0; i < int(length); i++ {
		r = append(r, Review{
			Date:   fake.DateStr(),
			Author: fake.Name(),
			Rating: Rating{
				Value:       float64(fake.Int32Range(0, 10)),
				Cleanliness: float64(fake.Int32Range(0, 10)),
				Overall:     float64(fake.Int32Range(1, 10)),
				CheckIn:     float64(fake.Int32Range(0, 100)),
				Rooms:       float64(fake.Int32Range(0, 100)),
			},
		})
	}
	return r
}

func buildPublicLikes(fake *faker.Faker, length int32) []string {
	var s []string
	for i := 0; i < int(length); i++ {
		s = append(s, fake.Name())
	}
	return s
}

func (h *Hotel) GenerateDocument(fake *faker.Faker, key string, documentSize int) interface{} {
	hotel := &Hotel{
		ID:            key,
		Country:       fake.Country(),
		Address:       fake.Address().Address,
		FreeParking:   fake.Bool(),
		City:          fake.Address().City,
		Type:          "Hotel",
		URL:           fake.URL(),
		Reviews:       buildReview(fake, fake.Int32Range(1, 3)),
		Phone:         fake.Phone(),
		Price:         fake.Price(1000, 100000),
		AvgRating:     fake.Float64Range(1, 5),
		FreeBreakfast: fake.Bool(),
		Name:          fake.BeerName(),
		PublicLikes:   buildPublicLikes(fake, fake.Int32Range(1, 3)),
		Email:         fake.URL(),
		Mutated:       MutatedPathDefaultValue,
	}

	currentDocSize := calculateSizeOfStruct(hotel)
	if (currentDocSize) < int(documentSize) {
		hotel.Padding = strings.Repeat("a", int(documentSize)-(currentDocSize))
	}
	return hotel

}

func (h *Hotel) UpdateDocument(fieldsToChange []string, lastUpdatedDocument interface{}, documentSize int,
	fake *faker.Faker) (interface{}, error) {

	hotel, ok := lastUpdatedDocument.(*Hotel)
	if !ok {
		return nil, fmt.Errorf("unable to decode last updated document to person template")
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
	if _, ok := checkFields["reviews"]; ok || len(checkFields) == 0 {
		hotel.Reviews = buildReview(fake, fake.Int32Range(1, 3))
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
	if _, ok := checkFields["public_likes"]; ok || len(checkFields) == 0 {
		hotel.PublicLikes = buildPublicLikes(fake, fake.Int32Range(1, 3))
	}
	if _, ok := checkFields["email"]; ok || len(checkFields) == 0 {
		hotel.Email = fake.URL()
	}
	hotel.Padding = ""
	currentDocSize := calculateSizeOfStruct(hotel)
	if (currentDocSize) < int(documentSize) {
		hotel.Padding = strings.Repeat("a", int(documentSize)-(currentDocSize))
	}

	return hotel, nil
}

func (h *Hotel) Compare(document1 interface{}, document2 interface{}) (bool, error) {
	p1, ok := document1.(*Hotel)
	if !ok {
		return false, fmt.Errorf("unable to decode first document to person template")
	}
	p2, ok := document2.(*Hotel)
	if !ok {
		return false, fmt.Errorf("unable to decode second document to person template")
	}

	return reflect.DeepEqual(p1, p2), nil
}

func (h *Hotel) GenerateIndexes(bucketName string, scopeName string, collectionName string) ([]string, error) {
	return []string{}, errors.New("not implemented")
}

func (h *Hotel) GenerateQueries(bucketName string, scopeName string, collectionName string) ([]string, error) {
	return []string{}, errors.New("not implemented")
}

func (h *Hotel) GenerateIndexesForSdk() (map[string][]string, error) {
	return map[string][]string{}, errors.New("not implemented")
}

func (h *Hotel) GenerateSubPathAndValue(fake *faker.Faker, subDocSize int) map[string]any {
	return map[string]interface{}{
		"_1": strings.Repeat(fake.Letter(), subDocSize),
	}
}
