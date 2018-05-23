package main

type ProgressMessage struct {
	Progress float32 `json:"progress"`
}

func UpdateProgress(expectedEvents int, events chan bool) (p chan ProgressMessage) {
	p = make(chan ProgressMessage, 1000)
	var eventCount int
	updateStep := expectedEvents / 400
	nextUpdate := updateStep
	go func() {
		for range events {
			eventCount++
			if eventCount > nextUpdate {
				perc := 100.0 * float32(eventCount) / float32(expectedEvents)
				p <- ProgressMessage{perc}
				nextUpdate = ((eventCount / updateStep) + 1) * updateStep
			}
		}
	}()
	return
}
