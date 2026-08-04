package gateway

import (
	"net/http"
	"time"
)

// exportN9routerEnter emits plaintext only after the caller explicitly selects
// the 9router format and acknowledges secret inclusion.
func (s *Server) exportN9routerEnter(w http.ResponseWriter, r *http.Request) {
	accounts, err := s.accounts.ListByTenant(r.Context(), adminTenant)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list accounts failed")
		return
	}
	out := make([]map[string]any, 0)
	for _, account := range accounts {
		if account.Provider != "enter-converge" {
			continue
		}
		creds, err := s.vault.Open(account)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "decrypt Enter Converge account failed")
			return
		}
		row := map[string]any{
			"id": account.ID, "provider": account.Provider, "authType": "apiKey",
			"name": account.Label, "priority": account.Priority, "isActive": !account.Disabled,
			"apiKey": creds.APIKey,
		}
		psd := map[string]any{}
		if workspace := firstString(creds.Extra["workspaceId"], creds.Extra["workspace_id"]); workspace != "" {
			psd["workspaceId"] = workspace
		}
		row["providerSpecificData"] = psd
		locks, err := s.db.Routing().ListModelCooldowns(r.Context(), account.ID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "list model cooldowns failed")
			return
		}
		for model, until := range locks {
			row["modelLock_"+model] = until.UTC().Format(time.RFC3339Nano)
		}
		out = append(out, row)
	}
	writeJSON(w, http.StatusOK, map[string]any{"providerConnections": out})
}

func firstString(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
