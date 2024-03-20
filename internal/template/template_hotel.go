package template

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/bgadrian/fastfaker/faker"
)

type Rating struct {
	RatingValue float64 `json:"rating_value,omitempty"`
	Cleanliness float64 `json:"cleanliness,omitempty"`
	Overall     float64 `json:"overall,omitempty"`
	CheckIn     float64 `json:"checkin,omitempty"`
	Rooms       float64 `json:"rooms,omitempty"`
}
type Review struct {
	Date   string `json:"date,omitempty"`
	Author string `json:"author,omitempty"`
	Rating Rating `json:"rating,omitempty"`
}

type Hotel struct {
	ID            string   `json:"_id" bson:"_id" dynamodbav:"_id"`
	Country       string   `json:"country,omitempty" dynamodbav:"country"`
	Address       string   `json:"address,omitempty" dynamodbav:"address"`
	FreeParking   bool     `json:"free_parking,omitempty" dynamodbav:"free_parking"`
	City          string   `json:"city,omitempty" dynamodbav:"city"`
	TemplateType  string   `json:"template_type" dynamodbav:"template_type"`
	URL           string   `json:"url,omitempty" dynamodbav:"url"`
	Reviews       []Review `json:"reviews,omitempty" dynamodbav:"reviews"`
	Phone         string   `json:"phone,omitempty" dynamodbav:"phone"`
	Price         float64  `json:"price,omitempty" dynamodbav:"price"`
	AvgRating     float64  `json:"avg_rating,omitempty" dynamodbav:"avg_rating"`
	FreeBreakfast bool     `json:"free_breakfast,omitempty" dynamodbav:"free_breakfast"`
	Name          string   `json:"name,omitempty" dynamodbav:"name"`
	PublicLikes   []string `json:"public_likes,omitempty" dynamodbav:"public_likes"`
	Email         string   `json:"email,omitempty" dynamodbav:"email"`
	Mutated       float64  `json:"mutated" dynamodbav:"mutated"`
	Padding       string   `json:"padding" dynamodbav:"padding"`
}

// buildReview generates the Review slice to be added into Hotel struct
/*
 * length defines the number of reviews to be added to Review slice
 * approximate size of 1 review is around 95bytes
 */
func buildReview(fake *faker.Faker, length int32) []Review {
	var r []Review
	for i := 0; i < int(length); i++ {
		r = append(r, Review{
			Date:   fake.DateStr(),
			Author: fake.Name(),
			Rating: Rating{
				RatingValue: float64(fake.Int32Range(0, 10)),
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
	var hotel *Hotel
	hotel = &Hotel{
		ID:            key,
		Country:       fake.Country(),
		Address:       fake.Address().Address,
		FreeParking:   fake.Bool(),
		City:          fake.Address().City,
		TemplateType:  "Hotel",
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
		remSize := int(documentSize) - (currentDocSize)
		numOfReviews := int(remSize/(95*2)) + 1
		rev := buildReview(fake, int32(numOfReviews))
		hotel.Reviews = rev
	}
	return hotel

}

func (h *Hotel) UpdateDocument(fieldsToChange []string, lastUpdatedDocument interface{}, documentSize int,
	fake *faker.Faker) (interface{}, error) {

	hotel, ok := lastUpdatedDocument.(*Hotel)
	if !ok {
		return nil, fmt.Errorf("unable to decode last updated document to hotel template in update doc")
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
		remSize := int(documentSize) - (currentDocSize)
		numOfReviews := int(remSize/(95*2)) + 1
		rev := buildReview(fake, int32(numOfReviews))
		hotel.Reviews = rev
	}

	return hotel, nil
}

func (h *Hotel) Compare(document1 interface{}, document2 interface{}) (bool, error) {
	p1, ok := document1.(*Hotel)
	if !ok {
		return false, fmt.Errorf("unable to decode first document to hotel template")
	}
	p2, ok := document2.(*Hotel)
	if !ok {
		return false, fmt.Errorf("unable to decode second document to hotel template")
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
func (h *Hotel) GetValues(document interface{}) (interface{}, error) {
	return document, nil
}
