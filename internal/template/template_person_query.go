package template

import "fmt"

func (p *Person) GenerateQueries(bucketName string, scopeName string, collectionName string) ([]string, error) {
	return []string{
		fmt.Sprintf("SELECT x.* FROM `%s`.`%s`.`%s` x LIMIT 100;", bucketName, scopeName, collectionName),
		fmt.Sprintf("select meta().id, first_name, last_name, email, age, address from `%s`.`%s`.`%s` where age between 0 and 50 limit 100;", bucketName, scopeName, collectionName),
		fmt.Sprintf("select first_name, last_name, attributes, address from `%s`.`%s`.`%s` where attributes.hair.hair_type=\"wavy\" limit 100;", bucketName, scopeName, collectionName),
		fmt.Sprintf("select age, count(*) from `%s`.`%s`.`%s` where marital_status='Single' group by age order by age limit 100;", bucketName, scopeName, collectionName),
		fmt.Sprintf("select first_name, gender, address.city, hobby from `%s`.`%s`.`%s` where gender=\"feminine\" limit 100;",
			bucketName, scopeName, collectionName),
		fmt.Sprintf("select age, marital_status from `%s`.`%s`.`%s` where hobbies is not null limit 100", bucketName, scopeName, collectionName),
		fmt.Sprintf("select gender, count(*) from `%s`.`%s`.`%s` group by gender order by gender limit 100;", bucketName, scopeName, collectionName),
		fmt.Sprintf("select first_name, email, attributes from `%s`.`%s`.`%s` where attributes.weight between 100 and 150 and attributes.height between 150 and 250 limit 100;",
			bucketName, scopeName, collectionName),
		fmt.Sprintf("select `%s`.address.street, `%s`.first_name, `%s`.age, hobby from `%s`.`%s`.`%s` unnest hobbies as hobby limit 100;",
			collectionName, collectionName, collectionName, bucketName, scopeName, collectionName),
	}, nil
}

func (p *Person) GenerateIndexes(bucketName string, scopeName string, collectionName string) ([]string, error) {
	return []string{
		fmt.Sprintf("CREATE INDEX ix_name on `%s`.`%s`.`%s`(first_name) WITH {\"defer_build\": true};",
			bucketName, scopeName, collectionName),
		fmt.Sprintf("CREATE INDEX ix_email on `%s`.`%s`.`%s`(email) WITH {\"defer_build\": true};",
			bucketName, scopeName, collectionName),
		fmt.Sprintf("CREATE INDEX ix_age_over_age on `%s`.`%s`.`%s`(age) where age between 30 and 50 WITH {\"defer_build\": true};",
			bucketName, scopeName, collectionName),
		fmt.Sprintf("CREATE INDEX ix_age_over_first_name on `%s`.`%s`.`%s`(first_name) where age between 0 and 50 WITH {\"defer_build\": true};",
			bucketName, scopeName, collectionName),
		fmt.Sprintf("CREATE INDEX ix_age_marital on `%s`.`%s`.`%s`(marital,age) USING GSI WITH {\"defer_build\": true};",
			bucketName, scopeName, collectionName),
		fmt.Sprintf("CREATE INDEX ix_gender_address_city_hobby on `%s`.`%s`.`%s`(Gender,address.city, DISTINCT ARRAY hobby FOR hobby in Hobbies END) where Gender=\"feminine\" WITH {\"defer_build\": true};",
			bucketName, scopeName, collectionName),
		fmt.Sprintf("BUILD INDEX ON `%s`.`%s`.`%s`(`#primary`, `ix_name_`, `ix_email`, `ix_age_over_age`, `ix_age_over_first_name`, `ix_age_marital`, `ix_gender_address_city_hobby`)",
			bucketName, scopeName, collectionName),
	}, nil
}

func (p *Person) GenerateIndexesForSdk() (map[string][]string, error) {

	return map[string][]string{
		"ix_fN_gender_hobby_addr": []string{"first_name", "gender", "hobby", "address"},
		"ix_age_maritalStatus":    []string{"age", "marital_status"},
		"ix_height_weight":        []string{"attributes.height", "attributes.weight"},
		"ix_city_color":           []string{"address.city", "attributes.colour"},
		"ix_hobby":                []string{"hobbies"},
		"ix_hair_type_color":      []string{"attributes.hair.hair_colour", "attributes.hair.hair_type"},
	}, nil
}
