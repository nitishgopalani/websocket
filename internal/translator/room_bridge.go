package translator

// laneForSource returns the translation lane that ingests audio from role.
func (r *room) laneForSource(role LegRole) *translationLane {
	r.mu.Lock()
	defer r.mu.Unlock()
	if role == LegA {
		return r.lane
	}
	return r.laneBToA
}

func (r *room) laneAToB() *translationLane {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lane
}

func (r *room) laneBToARef() *translationLane {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.laneBToA
}

func (r *room) loopBreakerTripped() bool {
	r.mu.Lock()
	breaker := r.breaker
	r.mu.Unlock()
	if breaker == nil {
		return false
	}
	return breaker.Tripped()
}

func (r *room) allowASRIngest(role LegRole) bool {
	r.mu.Lock()
	breaker := r.breaker
	coord := r.coord
	lane := r.lane
	laneB := r.laneBToA
	r.mu.Unlock()

	if breaker != nil && breaker.Tripped() {
		return false
	}
	if coord != nil {
		return coord.AllowASRIngest(role)
	}
	if role == LegA && lane != nil {
		return true
	}
	if role == LegB && laneB != nil {
		return true
	}
	return false
}

func (r *room) relayLaneFor(role LegRole) *translationLane {
	r.mu.Lock()
	defer r.mu.Unlock()
	if role == LegA {
		return r.lane
	}
	return r.laneBToA
}
