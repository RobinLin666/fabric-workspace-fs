package fabric

import (
	"context"
	"fmt"
	"net/http"
	"testing"
)

func TestEnvironmentDefinitionCanHaveNoOptionalParts(t *testing.T) {
	client, _ := api(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"definition":{"parts":[]}}`)
	}, Options{})
	def, err := client.GetDefinition(context.Background(), workspaceID, itemID, "Environment", "")
	if err != nil || len(def.Parts) != 0 {
		t.Fatal("empty optional Environment parts were rejected", def, err)
	}
	if _, err := client.GetDefinition(context.Background(), workspaceID, itemID, "Notebook", "ipynb"); err == nil {
		t.Fatal("Notebook still requires a real ipynb content part")
	}
}
