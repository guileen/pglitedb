package pkg3

import (
	"example.com/internaltest/pkg1"
)

func InternalFunction() string {
	pkg1.PublicFunction() // Using pkg1 function
	return "Hello from internal/pkg3"
}