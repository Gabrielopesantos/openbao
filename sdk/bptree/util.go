package bptree

import "github.com/hashicorp/go-uuid"

// genUUID generates a UUID
func genUUID() string {
	aUuid, err := uuid.GenerateUUID()
	if err != nil {
		panic(err)
	}
	return aUuid
}
