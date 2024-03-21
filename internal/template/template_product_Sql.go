package template

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/bgadrian/fastfaker/faker"
)

type ProductSql struct {
	ID             string  `json:"id" bson:"_id"`
	ProductName    string  `json:"product_name,omitempty"`
	ProductLink    string  `json:"product_link,omitempty"`
	Price          float64 `json:"price,omitempty"`
	AvgRating      float64 `json:"avg_rating,omitempty"`
	NumSold        int64   `json:"num_sold,omitempty"`
	UploadDate     string  `json:"upload_date,omitempty"`
	Weight         float64 `json:"weight,omitempty"`
	Quantity       int64   `json:"quantity,omitempty"`
	SellerName     string  `json:"seller_name,omitempty"`
	SellerLocation string  `json:"seller_location,omitempty"`
	SellerVerified bool    `json:"seller_verified,omitempty"`
	TemplateName   string  `json:"template_type"`
	Value          []interface{}
	Mutated        float64 `json:"mutated"`
	Padding        string  `json:"padding"`
}

func (p *ProductSql) GenerateDocument(fake *faker.Faker, key string, documentSize int) interface{} {
	product := &ProductSql{
		ID:             key,
		ProductName:    fake.Name(),
		ProductLink:    fake.URL(),
		Price:          fake.Price(100, 400000),
		AvgRating:      fake.Float64Range(1, 5),
		NumSold:        fake.Int64Range(0, 50000),
		UploadDate:     fake.DateStr(),
		Weight:         fake.Float64Range(0.1, 5),
		Quantity:       fake.Int64Range(0, 50000),
		SellerName:     fake.BeerName(),
		SellerLocation: fake.Address().City + ", " + fake.Address().Country,
		SellerVerified: fake.Bool(),
		TemplateName:   "Product_sql",
		Mutated:        MutatedPathDefaultValue,
	}
	product.Padding = ""
	currentDocSize := calculateSizeOfStruct(product)
	if (currentDocSize) < int(documentSize) {
		product.Padding = strings.Repeat("a", int(documentSize)-(currentDocSize))
	}
	values := []interface{}{&product.TemplateName, &product.ID, &product.ProductName, &product.ProductLink, &product.Price, &product.AvgRating, &product.NumSold,
		&product.UploadDate, &product.Weight, &product.Quantity, &product.SellerName, &product.SellerLocation, &product.SellerVerified,
		&product.Padding, &product.Mutated}
	product.Value = values
	return product
}

func (p *ProductSql) UpdateDocument(fieldsToChange []string, lastUpdatedDocument interface{}, documentSize int,
	fake *faker.Faker) (interface{}, error) {

	product, ok := lastUpdatedDocument.(*ProductSql)
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
	product.Padding = ""
	if (currentDocSize) < int(documentSize) {
		product.Padding = strings.Repeat("a", int(documentSize)-(currentDocSize))
	}
	values := []interface{}{&product.TemplateName, &product.ID, &product.ProductName, &product.ProductLink, &product.Price, &product.AvgRating, &product.NumSold,
		&product.UploadDate, &product.Weight, &product.Quantity, &product.SellerName, &product.SellerLocation, &product.SellerVerified,
		&product.Padding, &product.Mutated}
	product.Value = values
	return product, nil
}

func (p *ProductSql) Compare(document1 interface{}, document2 interface{}) (bool, error) {
	p1, ok := document1.(*ProductSql)
	if !ok {
		return false, fmt.Errorf("unable to decode first document to product template")
	}
	p2, ok := document2.(*ProductSql)
	if !ok {
		return false, fmt.Errorf("unable to decode second document to product template")
	}
	return reflect.DeepEqual(p1, p2), nil
}

func (p *ProductSql) GenerateIndexes(bucketName string, scopeName string, collectionName string) ([]string, error) {
	// TODO
	panic("In template_product, to be implemented")
}

func (p *ProductSql) GenerateQueries(bucketName string, scopeName string, collectionName string) ([]string, error) {
	// TODO
	panic("In template_product, to be implemented")
}

func (p *ProductSql) GenerateIndexesForSdk() (map[string][]string, error) {
	// TODO
	panic("In template_product, to be implemented")
}

func (p *ProductSql) GenerateSubPathAndValue(fake *faker.Faker, subDocSize int) map[string]any {
	return map[string]interface{}{
		"_1": strings.Repeat(fake.Letter(), subDocSize),
	}
}
func (p *ProductSql) GetValues(document interface{}) (interface{}, error) {
	product, ok := document.(*ProductSql)
	if !ok {
		return nil, fmt.Errorf("unable to decode last updated document to product template")
	}
	return product.Value, nil
}
