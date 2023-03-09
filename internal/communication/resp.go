package communication

type Response struct {
	Token [32]byte `json:"token"`
	Seed  int64    `json:"seed"`
}
