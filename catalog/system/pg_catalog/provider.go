package pgcatalog

import (
	"github.com/guileen/pglitedb/catalog/system/interfaces"
)

// Provider implements the PgCatalogProvider interface
type Provider struct {
	manager interfaces.TableManager
}

// NewProvider creates a new pg_catalog provider
func NewProvider(manager interfaces.TableManager) *Provider {
	return &Provider{
		manager: manager,
	}
}