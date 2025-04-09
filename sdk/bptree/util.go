package bptree

import "github.com/hashicorp/go-uuid"

// genUuid generates a UUID
func genUuid() string {
	aUuid, err := uuid.GenerateUUID()
	if err != nil {
		panic(err)
	}
	return aUuid
}
