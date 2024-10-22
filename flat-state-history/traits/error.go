package traits

import "errors"

var (
	ErrBlockNotFound   = errors.New("block not found")
	ErrAccountNotFound = errors.New("account not found")
	ErrStorageNotFound = errors.New("storage not found")
)
