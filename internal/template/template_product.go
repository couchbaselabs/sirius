package template

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/bgadrian/fastfaker/faker"
)

type ProductRating struct {
	RatingValue  float64 `json:"rating_value,omitempty" dynamodbav:"rating_value" parquet:"name=rating_value, type=DOUBLE"`
	Performance  float64 `json:"performance,omitempty" dynamodbav:"performance" parquet:"name=performance, type=DOUBLE"`
	Utility      float64 `json:"utility,omitempty" dynamodbav:"utility" parquet:"name=utility, type=DOUBLE"`
	Pricing      float64 `json:"pricing,omitempty" dynamodbav:"pricing" parquet:"name=pricing, type=DOUBLE"`
	BuildQuality float64 `json:"build_quality,omitempty" dynamodbav:"build_quality" parquet:"name=build_quality, type=DOUBLE"`
}

type ProductReview struct {
	Date          string        `json:"date,omitempty" dynamodbav:"date" parquet:"name=date, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Author        string        `json:"author,omitempty" dynamodbav:"author" parquet:"name=author, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ProductRating ProductRating `json:"product_rating,omitempty" dynamodbav:"product_rating" parquet:"name=product_rating"`
}

type Product struct {
	ID                string            `json:"id" bson:"_id" dynamodbav:"id" parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ProductName       string            `json:"product_name,omitempty" dynamodbav:"product_name" parquet:"name=product_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ProductLink       string            `json:"product_link,omitempty" dynamodbav:"product_link" parquet:"name=product_link, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ProductFeatures   []string          `json:"product_features,omitempty" dynamodbav:"product_features" parquet:"name=product_features, type=LIST"`
	ProductSpecs      map[string]string `json:"product_specs,omitempty" dynamodbav:"product_specs" parquet:"name=product_specs, type=MAP"`
	ProductImageLinks []string          `json:"product_image_links,omitempty" dynamodbav:"product_image_links" parquet:"name=product_image_links, type=LIST"`
	ProductReviews    []ProductReview   `json:"product_reviews,omitempty" dynamodbav:"product_reviews" parquet:"name=product_reviews, type=LIST"`
	ProductCategory   []string          `json:"product_category,omitempty" dynamodbav:"product_category" parquet:"name=product_category, type=LIST"`
	Price             float64           `json:"price,omitempty" dynamodbav:"price" parquet:"name=price, type=DOUBLE"`
	AvgRating         float64           `json:"avg_rating,omitempty" dynamodbav:"avg_rating" parquet:"name=avg_rating, type=DOUBLE"`
	NumSold           int64             `json:"num_sold,omitempty" dynamodbav:"num_sold" parquet:"name=num_sold, type=INT64"`
	UploadDate        string            `json:"upload_date,omitempty" dynamodbav:"upload_date" parquet:"name=upload_date, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Weight            float64           `json:"weight,omitempty" dynamodbav:"weight" parquet:"name=weight, type=DOUBLE"`
	Quantity          int64             `json:"quantity,omitempty" dynamodbav:"quantity" parquet:"name=quantity, type=INT64"`
	SellerName        string            `json:"seller_name,omitempty" dynamodbav:"seller_name" parquet:"name=seller_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	SellerLocation    string            `json:"seller_location,omitempty" dynamodbav:"seller_location" parquet:"name=seller_location, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	SellerVerified    bool              `json:"seller_verified,omitempty" dynamodbav:"seller_verified" parquet:"name=seller_verified, type=BOOLEAN"`
	TemplateName      string            `json:"template_name" dynamodbav:"template_name" parquet:"name=template_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Mutated           float64           `json:"mutated" dynamodbav:"mutated" parquet:"name=mutated, type=DOUBLE"`
	Padding           string            `json:"padding" dynamodbav:"padding" parquet:"name=padding, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

// buildReview generates the Review slice to be added into Product struct
/*
 * length defines the number of reviews to be added to Review slice
 * approximate size of 1 review is around 95bytes
 */
func buildProductReview(fake *faker.Faker, length int32) []ProductReview {
	var prodReview []ProductReview
	for i := 0; i < int(length); i++ {
		prodReview = append(prodReview, ProductReview{
			Date:   fake.DateStr(),
			Author: fake.Name(),
			ProductRating: ProductRating{
				RatingValue:  float64(fake.Int32Range(0, 10)),
				Performance:  float64(fake.Int32Range(0, 10)),
				Utility:      float64(fake.Int32Range(1, 10)),
				Pricing:      float64(fake.Int32Range(0, 10)),
				BuildQuality: float64(fake.Int32Range(0, 10)),
			},
		})
	}
	return prodReview
}

func buildProductCategory(fake *faker.Faker, length int32) []string {
	var prodCategory []string
	for i := 0; i < int(length); i++ {
		prodCategory = append(prodCategory, fake.VehicleType())
	}
	return prodCategory
}

func buildProductImageLinks(fake *faker.Faker, length int32) []string {
	var prodImgLinks []string
	for i := 0; i < int(length); i++ {
		prodImgLinks = append(prodImgLinks, fake.URL())
	}
	return prodImgLinks
}

func buildProductFeatures(fake *faker.Faker, length int32) []string {
	var prodFeatures []string
	for i := 0; i < int(length); i++ {
		prodFeatures = append(prodFeatures, fake.Sentence(100))
	}
	return prodFeatures
}

func buildProductSpecs(fake *faker.Faker, length int32) map[string]string {
	prodSpecs := make(map[string]string)
	for i := 0; i < int(length); i++ {
		prodSpecs[fake.CarMaker()] = fake.CarModel()
	}
	return prodSpecs
}

func (p *Product) GenerateDocument(fake *faker.Faker, key string, documentSize int) interface{} {
	product := &Product{
		ID:                key,
		ProductName:       fake.Name(),
		ProductLink:       fake.URL(),
		ProductFeatures:   buildProductFeatures(fake, fake.Int32Range(1, 3)),
		ProductSpecs:      buildProductSpecs(fake, fake.Int32Range(1, 3)),
		ProductImageLinks: buildProductImageLinks(fake, fake.Int32Range(1, 3)),
		ProductReviews:    buildProductReview(fake, fake.Int32Range(1, 3)),
		ProductCategory:   buildProductCategory(fake, fake.Int32Range(1, 3)),
		Price:             fake.Price(100, 400000),
		AvgRating:         fake.Float64Range(1, 5),
		NumSold:           fake.Int64Range(0, 50000),
		UploadDate:        fake.DateStr(),
		Weight:            fake.Float64Range(0.1, 5),
		Quantity:          fake.Int64Range(0, 50000),
		SellerName:        fake.BeerName(),
		SellerLocation:    fake.Address().City + ", " + fake.Address().Country,
		SellerVerified:    fake.Bool(),
		TemplateName:      "Product",
		Mutated:           MutatedPathDefaultValue,
	}

	currentDocSize := calculateSizeOfStruct(product)
	if (currentDocSize) < documentSize {
		remSize := documentSize - currentDocSize
		numOfReviews := int(remSize/(95*2)) + 1
		prodReview := buildProductReview(fake, int32(numOfReviews))
		product.ProductReviews = prodReview
	}

	return product
}

func (p *Product) UpdateDocument(fieldsToChange []string, lastUpdatedDocument interface{}, documentSize int,
	fake *faker.Faker) (interface{}, error) {

	product, ok := lastUpdatedDocument.(*Product)
	if !ok {
		return nil, fmt.Errorf("unable to decode last updated document to product template")
	}

	checkFields := make(map[string]struct{})
	for _, s := range fieldsToChange {
		checkFields[s] = struct{}{}
	}

	if _, ok := checkFields["product_name"]; ok || len(checkFields) == 0 {
		product.ProductName = fake.Name()
	}
	if _, ok := checkFields["product_link"]; ok || len(checkFields) == 0 {
		product.ProductLink = fake.URL()
	}
	if _, ok := checkFields["product_features"]; ok || len(checkFields) == 0 {
		product.ProductFeatures = buildProductFeatures(fake, fake.Int32Range(1, 3))
	}
	if _, ok := checkFields["product_specs"]; ok || len(checkFields) == 0 {
		product.ProductSpecs = buildProductSpecs(fake, fake.Int32Range(1, 3))
	}
	if _, ok := checkFields["product_image_links"]; ok || len(checkFields) == 0 {
		product.ProductImageLinks = buildProductImageLinks(fake, fake.Int32Range(1, 3))
	}
	if _, ok := checkFields["product_reviews"]; ok || len(checkFields) == 0 {
		product.ProductReviews = buildProductReview(fake, fake.Int32Range(1, 3))
	}
	if _, ok := checkFields["product_category"]; ok || len(checkFields) == 0 {
		product.ProductCategory = buildProductCategory(fake, fake.Int32Range(1, 3))
	}
	if _, ok := checkFields["price"]; ok || len(checkFields) == 0 {
		product.Price = fake.Price(100, 400000)
	}
	if _, ok := checkFields["avg_rating"]; ok || len(checkFields) == 0 {
		product.AvgRating = fake.Float64Range(1, 5)
	}
	if _, ok := checkFields["num_sold"]; ok || len(checkFields) == 0 {
		product.NumSold = fake.Int64Range(0, 50000)
	}
	if _, ok := checkFields["upload_date"]; ok || len(checkFields) == 0 {
		//product.UploadDate = fake.Date().Format("2006-01-02")
		product.UploadDate = fake.DateStr()
	}
	if _, ok := checkFields["weight"]; ok || len(checkFields) == 0 {
		product.Weight = fake.Float64Range(0.1, 5)
	}
	if _, ok := checkFields["quantity"]; ok || len(checkFields) == 0 {
		product.Quantity = fake.Int64Range(0, 50000)
	}
	if _, ok := checkFields["seller_name"]; ok || len(checkFields) == 0 {
		product.SellerName = fake.BeerName()
	}
	if _, ok := checkFields["seller_location"]; ok || len(checkFields) == 0 {
		product.SellerLocation = fake.Address().City + ", " + fake.Address().Country
	}
	if _, ok := checkFields["seller_verified"]; ok || len(checkFields) == 0 {
		product.SellerVerified = fake.Bool()
	}

	currentDocSize := calculateSizeOfStruct(product)
	//log.Println("Size of doc before appends:", currentDocSize)
	if currentDocSize < documentSize {
		remSize := documentSize - currentDocSize
		numOfReviews := int(remSize/(95*2)) + 1
		prodReview := buildProductReview(fake, int32(numOfReviews))
		product.ProductReviews = prodReview
	}

	return product, nil
}

func (p *Product) Compare(document1 interface{}, document2 interface{}) (bool, error) {
	p1, ok := document1.(*Product)
	if !ok {
		return false, fmt.Errorf("unable to decode first document to product template")
	}
	p2, ok := document2.(*Product)
	if !ok {
		return false, fmt.Errorf("unable to decode second document to product template")
	}

	return reflect.DeepEqual(p1, p2), nil
}

func (p *Product) GenerateIndexes(bucketName string, scopeName string, collectionName string) ([]string, error) {
	// TODO
	panic("In template_product.go, to be implemented")
}

func (p *Product) GenerateQueries(bucketName string, scopeName string, collectionName string) ([]string, error) {
	// TODO
	panic("In template_product.go, to be implemented")
}

func (p *Product) GenerateIndexesForSdk() (map[string][]string, error) {
	// TODO
	panic("In template_product.go, to be implemented")
}

func (p *Product) GenerateSubPathAndValue(fake *faker.Faker, subDocSize int) map[string]any {
	return map[string]interface{}{
		"SubDoc": strings.Repeat(fake.Letter(), subDocSize),
	}
}

func (p *Product) GetValues(document interface{}) (interface{}, error) {
	return document, nil
}
