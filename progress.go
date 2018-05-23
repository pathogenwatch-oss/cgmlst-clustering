package main

type ProgressMessage struct {
	Progress float32 `json:"progress"`
}

func UpdateProgress(expectedEvents int, events chan bool) (p chan ProgressMessage) {
	p = make(chan ProgressMessage, 1000)
	var eventCount int
	updateStep := expectedEvents / 400
	if updateStep == 0 {
		updateStep = 1
	}
	nextUpdate := updateStep
	go func() {
		if expectedEvents == 0 {
			p <- ProgressMessage{99.99}
		}
		for range events {
			if expectedEvents == 0 {
				continue
			}
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
