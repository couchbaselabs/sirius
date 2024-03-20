package template

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/bgadrian/fastfaker/faker"
)

type Rating struct {
	RatingValue float64 `json:"rating_value,omitempty" dynamodbav:"rating_value" parquet:"name=rating_value, type=DOUBLE"`
	Cleanliness float64 `json:"cleanliness,omitempty" dynamodbav:"cleanliness" parquet:"name=cleanliness, type=DOUBLE"`
	Overall     float64 `json:"overall,omitempty" dynamodbav:"overall" parquet:"name=overall, type=DOUBLE"`
	CheckIn     float64 `json:"checkin,omitempty" dynamodbav:"checkin" parquet:"name=checkin, type=DOUBLE"`
	Rooms       float64 `json:"rooms,omitempty" dynamodbav:"rooms" parquet:"name=rooms, type=DOUBLE"`
}
type Review struct {
	Date   string `json:"date,omitempty" dynamodbav:"date" parquet:"name=date, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Author string `json:"author,omitempty" dynamodbav:"author" parquet:"name=author, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Rating Rating `json:"rating,omitempty" dynamodbav:"rating" parquet:"name=rating"`
}

type Hotel struct {
	ID            string   `json:"id" bson:"_id" dynamodbav:"id" parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Country       string   `json:"country,omitempty" dynamodbav:"country" parquet:"name=country, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Address       string   `json:"address,omitempty" dynamodbav:"address" parquet:"name=address, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	FreeParking   bool     `json:"free_parking,omitempty" dynamodbav:"free_parking" parquet:"name=free_parking, type=BOOLEAN"`
	City          string   `json:"city,omitempty" dynamodbav:"city" parquet:"name=city, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	TemplateName  string   `json:"template_name" dynamodbav:"template_name" parquet:"name=template_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	URL           string   `json:"url,omitempty" dynamodbav:"url" parquet:"name=url, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Reviews       []Review `json:"reviews,omitempty" dynamodbav:"reviews" parquet:"name=reviews, type=LIST"`
	Phone         string   `json:"phone,omitempty" dynamodbav:"phone" parquet:"name=phone, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Price         float64  `json:"price,omitempty" dynamodbav:"price" parquet:"name=price, type=DOUBLE"`
	AvgRating     float64  `json:"avg_rating,omitempty" dynamodbav:"avg_rating" parquet:"name=avg_rating, type=DOUBLE"`
	FreeBreakfast bool     `json:"free_breakfast,omitempty" dynamodbav:"free_breakfast" parquet:"name=free_breakfast, type=BOOLEAN"`
	Name          string   `json:"name,omitempty" dynamodbav:"name" parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	PublicLikes   []string `json:"public_likes,omitempty" dynamodbav:"public_likes" parquet:"name=public_likes, type=LIST"`
	Email         string   `json:"email,omitempty" dynamodbav:"email" parquet:"name=email, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Mutated       float64  `json:"mutated" dynamodbav:"mutated" parquet:"name=mutated, type=DOUBLE"`
	Padding       string   `json:"padding" dynamodbav:"padding" parquet:"name=padding, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
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
	hotel := &Hotel{
		ID:            key,
		Country:       fake.Country(),
		Address:       fake.Address().Address,
		FreeParking:   fake.Bool(),
		City:          fake.Address().City,
		TemplateName:  "Hotel",
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
	if currentDocSize < documentSize {
		remSize := documentSize - currentDocSize
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
		return nil, fmt.Errorf("in template_hotel.go UpdateDocument(), unable to decode last updated document to hotel template")
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
	if currentDocSize < documentSize {
		remSize := documentSize - currentDocSize
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
		"SubDoc": strings.Repeat(fake.Letter(), subDocSize),
	}
}

func (h *Hotel) GetValues(document interface{}) (interface{}, error) {
	return document, nil
}

// ToStringMap is used to convert the Hotel Struct into a map[string]interface{} form
// To be used while converting data in avro format.
func (h *Hotel) ToStringMap() map[string]interface{} {
	hotelMap := map[string]interface{}{
		"id":            h.ID,
		"template_name": h.TemplateName,
		"mutated":       h.Mutated,
		"padding":       h.Padding,
	}

	if h.Country != "" {
		hotelMap["country"] = h.Country
	} else {
		hotelMap["country"] = nil
	}

	if h.Address != "" {
		hotelMap["address"] = h.Address
	} else {
		hotelMap["address"] = nil
	}

	hotelMap["free_parking"] = h.FreeParking
	hotelMap["city"] = h.City

	if h.URL != "" {
		hotelMap["url"] = h.URL
	} else {
		hotelMap["url"] = nil
	}

	if len(h.Reviews) > 0 {
		reviews := make([]map[string]interface{}, 0)
		for _, review := range h.Reviews {
			reviewMap := map[string]interface{}{
				"date":   review.Date,
				"author": review.Author,
				"rating": map[string]interface{}{
					"rating_value": review.Rating.RatingValue,
					"cleanliness":  review.Rating.Cleanliness,
					"overall":      review.Rating.Overall,
					"checkin":      review.Rating.CheckIn,
					"rooms":        review.Rating.Rooms,
				},
			}
			reviews = append(reviews, reviewMap)
		}
		hotelMap["reviews"] = reviews
	} else {
		hotelMap["reviews"] = nil
	}

	hotelMap["phone"] = h.Phone
	hotelMap["price"] = h.Price
	hotelMap["avg_rating"] = h.AvgRating
	hotelMap["free_breakfast"] = h.FreeBreakfast
	hotelMap["name"] = h.Name

	if len(h.PublicLikes) > 0 {
		hotelMap["public_likes"] = h.PublicLikes
	} else {
		hotelMap["public_likes"] = nil
	}
	hotelMap["email"] = h.Email

	return hotelMap
}

// StringMapToHotel is used to convert the map[string]interface{} containing hotel doc into Hotel struct type
func StringMapToHotel(data map[string]interface{}) *Hotel {
	hotel := &Hotel{}

	for key, value := range data {
		switch key {
		case "id":
			if id, ok := value.(string); ok {
				hotel.ID = id
			}
		case "country":
			if country, ok := value.(string); ok {
				hotel.Country = country
			}
		case "address":
			if address, ok := value.(string); ok {
				hotel.Address = address
			}
		case "free_parking":
			if freeParking, ok := value.(bool); ok {
				hotel.FreeParking = freeParking
			}
		case "city":
			if city, ok := value.(string); ok {
				hotel.City = city
			}
		case "template_name":
			if templateName, ok := value.(string); ok {
				hotel.TemplateName = templateName
			}
		case "url":
			if url, ok := value.(string); ok {
				hotel.URL = url
			}
		case "reviews":
			if reviews, ok := value.([]interface{}); ok {
				for _, review := range reviews {
					if reviewMap, ok := review.(map[string]interface{}); ok {
						newReview := Review{}
						for reviewKey, reviewValue := range reviewMap {
							switch reviewKey {
							case "date":
								if date, ok := reviewValue.(string); ok {
									newReview.Date = date
								}
							case "author":
								if author, ok := reviewValue.(string); ok {
									newReview.Author = author
								}
							case "rating":
								if ratingMap, ok := reviewValue.(map[string]interface{}); ok {
									rating := Rating{}
									for ratingKey, ratingValue := range ratingMap {
										switch ratingKey {
										case "rating_value":
											if rv, ok := ratingValue.(float64); ok {
												rating.RatingValue = rv
											}
										case "cleanliness":
											if cleanliness, ok := ratingValue.(float64); ok {
												rating.Cleanliness = cleanliness
											}
										case "overall":
											if overall, ok := ratingValue.(float64); ok {
												rating.Overall = overall
											}
										case "checkin":
											if checkin, ok := ratingValue.(float64); ok {
												rating.CheckIn = checkin
											}
										case "rooms":
											if rooms, ok := ratingValue.(float64); ok {
												rating.Rooms = rooms
											}
										}
									}
									newReview.Rating = rating
								}
							}
						}
						hotel.Reviews = append(hotel.Reviews, newReview)
					}
				}
			}
		case "phone":
			if phone, ok := value.(string); ok {
				hotel.Phone = phone
			}
		case "price":
			if price, ok := value.(float64); ok {
				hotel.Price = price
			}
		case "avg_rating":
			if avgRating, ok := value.(float64); ok {
				hotel.AvgRating = avgRating
			}
		case "free_breakfast":
			if freeBreakfast, ok := value.(bool); ok {
				hotel.FreeBreakfast = freeBreakfast
			}
		case "name":
			if name, ok := value.(string); ok {
				hotel.Name = name
			}
		case "public_likes":
			if publicLikes, ok := value.([]interface{}); ok {
				for _, like := range publicLikes {
					if likeStr, ok := like.(string); ok {
						hotel.PublicLikes = append(hotel.PublicLikes, likeStr)
					}
				}
			}
		case "email":
			if email, ok := value.(string); ok {
				hotel.Email = email
			}
		case "mutated":
			if mutated, ok := value.(float64); ok {
				hotel.Mutated = mutated
			}
		case "padding":
			if padding, ok := value.(string); ok {
				hotel.Padding = padding
			}
		}
	}

	return hotel
}
