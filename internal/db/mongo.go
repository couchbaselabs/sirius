package db

type Mongo struct {
}

func (m Mongo) Connect(connStr, username, password string, extra Extras) error {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) Create(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) Update(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) Read(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) Delete(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) Touch(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) InsertSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) UpsertSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) Increment(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) ReplaceSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) ReadSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) DeleteSubDoc(connStr, username, password, key string, keyValue []KeyValue, offset int64, extra Extras) SubDocOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) CreateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) Warmup(connStr, username, password string, extra Extras) error {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) Close(connStr string) error {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) UpdateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) ReadBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) DeleteBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	//TODO implement me
	panic("implement me")
}

func (m Mongo) TouchBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	//TODO implement me
	panic("implement me")
}
