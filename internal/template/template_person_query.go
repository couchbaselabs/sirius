package template

func (p *Person) GenerateQueries() ([]string, error) {
	return []string{
		"SELECT x.* FROM `%s`.%s.%s x LIMIT 100;",
		"select meta().id, firstName, lastName, email, age, address from `%s`.%s.%s where age between 0 and 50 limit 100;",
		"select firstName, lastName, attributes, address from `%s`.%s.%s where attributes.hair.type=\"wavy\" limit 100;",
		"select age, count(*) from `%s`.%s.%s where maritalStatus='Single' group by age order by age limit 100;",
		"select firstName, gender, address.city, hobby from `%s`.%s.%s where gender=\"feminine\" limit 100;",
		"select mishra.address.street, mishra.firstName, mishra.age, hobby from `%s`.%s.%s unnest hobbies as hobby limit 100",
		"select age, maritalStatus from `%s`.%s.%s where hobbies is not null limit 100",
		"select gender, count(*) from `%s`.%s.%s group by gender order by gender limit 100;",
		"select firstName, email, attributes from `%s`.%s.%s where attributes.weight between 100 and 150 and attributes.height between 150 and 250 limit 100;",
	}, nil
}

func (p *Person) GenerateIndexes() ([]string, error) {
	return []string{
		"CREATE PRIMARY INDEX ON `%s`.%s.%s;",
		"CREATE INDEX ix_name ON `%s`.%s.%s(firstName) WITH {\"defer_build\": true};",
		"CREATE INDEX ix_email ON `%s`.%s.%s(email) WITH {\"defer_build\": true};",
		"CREATE INDEX ix_age_over_age on `%s`.%s.%s(age) where age between 30 and 50 WITH {\"defer_build\": true};",
		"CREATE INDEX ix_age_over_firstName on `%s`.%s.%s(firstName) where age between 0 and 50 WITH {\"defer_build\": true};",
		"CREATE INDEX ix_age_marital on `%s`.%s.%s(marital,age) USING GSI WITH {\"defer_build\": true};",
		"CREATE INDEX ix_gender_address_city_hobby on `%s`.%s.%s(Gender,address.city, DISTINCT ARRAY hobby FOR hobby in Hobbies END) where Gender=\"feminine\" WITH {\"defer_build\": true};",
		"BUILD INDEX ON `%s`.%s.%s(`#primary`, `ix_name`, `ix_email`, `ix_age_over_age`, `ix_age_over_firstName`, `ix_age_marital`, `ix_gender_address_city_hobby`)",
	}, nil
}
