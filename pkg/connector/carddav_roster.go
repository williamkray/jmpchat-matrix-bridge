package connector

// rosterSnapshot returns a copy of the roster as lowercased bare JID -> roster name ("" for an entry
// without a name) and whether the first complete roster fetch of this client has finished (before that
// the set may be partial).
func (x *xmppClient) rosterSnapshot() (roster map[string]string, loaded bool) {
	x.rosterMu.RLock()
	defer x.rosterMu.RUnlock()
	roster = make(map[string]string, len(x.rosterJIDs))
	for j := range x.rosterJIDs {
		roster[j] = x.roster[j]
	}
	return roster, x.rosterLoaded
}

// rosterEntryName reports whether the bare JID (any case) is in the roster and its roster name.
func (x *xmppClient) rosterEntryName(bareJID string) (name string, in bool) {
	in, _ = x.inRoster(bareJID)
	if !in {
		return "", false
	}
	return x.lookupRosterName(bareJID), true
}
