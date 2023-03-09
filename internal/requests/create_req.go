package requests

type CreateRequest struct {
}

func (r *CreateRequest) Validate() (bool, error) {

	return true, nil
}
