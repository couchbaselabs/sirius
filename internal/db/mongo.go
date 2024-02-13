package db

type Mongo struct {
}

func (m *Mongo) Connect(connStr, username, password string, extra Extras) error {
	//TODO implement me
	panic("implement me")
}

func (m *Mongo) Create(connStr, username, password, key string, doc interface{}, extra Extras) OperationResult {
	//TODO implement me
	panic("implement me")
}

func (m *Mongo) Read(connStr, username, password, key string, extra Extras) OperationResult {
	//TODO implement me
	panic("implement me")
}

func (m *Mongo) Update(connStr, username, password, key string, doc interface{}, extra Extras) OperationResult {
	//TODO implement me
	panic("implement me")
}

func (m *Mongo) Delete(connStr, username, password, key string, extra Extras) OperationResult {
	//TODO implement me
	panic("implement me")
}

func (m *Mongo) Touch(connStr, username, password, key string, extra Extras) OperationResult {
	//TODO implement me
	panic("implement me")
}

func (m *Mongo) InsertSubDoc(connStr, username, password, key string, subPathValues map[string]any,
	extra Extras) OperationResult {
	//TODO implement me
	panic("implement me")
}

func (m *Mongo) UpsertSubDoc(connStr, username, password, key string, subPathValues map[string]any,
	extra Extras) OperationResult {
	//TODO implement me
	panic("implement me")
}

func (m *Mongo) ReplaceSubDoc(connStr, username, password, key string, subPathValues map[string]any,
	extra Extras) OperationResult {
	//TODO implement me
	panic("implement me")
}

func (m *Mongo) ReadSubDoc(connStr, username, password, key string, subPathValues map[string]any,
	extra Extras) OperationResult {
	//TODO implement me
	panic("implement me")
}

func (m *Mongo) DeleteSubDoc(connStr, username, password, key string, subPathValues map[string]any,
	extra Extras) OperationResult {
	//TODO implement me
	panic("implement me")
}

func (m *Mongo) IncrementMutationCount(connStr, username, password, key string, subPathValues map[string]any,
	extra Extras) OperationResult {
	//TODO implement me
	panic("implement me")
}

func (m *Mongo) Warmup(connStr, username, password string, extra Extras) error {
	//TODO implement me
	panic("implement me")
}

func (m *Mongo) Close(connStr string) error {
	//TODO implement me
	panic("implement me")
}
