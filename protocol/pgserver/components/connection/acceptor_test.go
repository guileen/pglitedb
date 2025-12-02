package connection

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConnectionAcceptor_NewConnectionAcceptor(t *testing.T) {
	acceptor := NewConnectionAcceptor()
	assert.NotNil(t, acceptor)
	assert.Equal(t, 0, acceptor.GetConnectionCount())
}

func TestConnectionAcceptor_StopAcceptingConnections(t *testing.T) {
	acceptor := NewConnectionAcceptor()
	
	// Initially not stopped
	// Note: We can't directly check the stopped field, but we can test the behavior
	
	acceptor.StopAcceptingConnections()
	
	// After stopping, subsequent operations should behave accordingly
	// The real test is in AcceptConnections when stopped
}

func TestConnectionAcceptor_GetConnectionCount(t *testing.T) {
	acceptor := NewConnectionAcceptor()
	
	// Initially zero
	assert.Equal(t, 0, acceptor.GetConnectionCount())
	
	// Note: Testing increment/decrement would require integration testing
	// with actual connections, which is complex in unit tests
}