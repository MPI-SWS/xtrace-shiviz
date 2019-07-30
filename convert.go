package main

import (
	"encoding/json"
	"github.com/DistributedClocks/GoVector/govec/vclock"
	"fmt"
	"log"
	"os"
	"strconv"
)

// Represents an event in the trace. Currently, only a subset of the fields are parsed
// which are relevant to Shiviz/TSViz
type Event struct {
	ProcessName string `json:"ProcessName"`
	Label string `json:Label"`
	Timestamp uint64 `json:"Timestamp"`
	HRT uint64 `json:"HRT"`
	EventID string `json:"EventID"`
	Parents []string `json:"ParentEventID"`
	ThreadID int `json:"ThreadID"`
	Agent string `json:"Agent"`
}

//Struct that represents an XTrace. This corresponds to XTraceV4
type XTrace struct {
	ID string `json:"id"`
	Events []Event `json:"reports"`
}

func print_events(trace XTrace) {
	fmt.Println("Process, ThreadID, Agent, Event, Parents")
	for _, event := range trace.Events {
		fmt.Println(event.ProcessName, event.ThreadID, event.Agent, event.EventID, event.Parents)
	}
}

//Function initializes a shiviz file with the correct regular expression
func init_shiviz_file(filename string) (*os.File, error) {
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	_, err = f.WriteString("(?<host>\\S*) (?<clock>{.*})\\n(?<event>.*)\n\n")
	if err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func all_parents_seen(event Event, seen_events map[string]bool) bool {
	result := true
	for _, parent := range event.Parents {
		if _, ok := seen_events[parent]; ok {
			// This parent is already in the log. We can continue
			continue
		} else {
			// Add the current event to waiting events
			result = false
			break
		}
	}
	return result
}

func sort_events(events []Event) []Event {
	var sorted_events []Event
	seen_events := make(map[string]bool)
	var waiting_events []Event
	for _, event := range events {
		// Check if each parent has been seen before
		parents_seen := all_parents_seen(event, seen_events)
		if parents_seen {
			sorted_events = append(sorted_events, event)
			seen_events[event.EventID] = true
			// Check the waiting list
			for _, waiting_event := range waiting_events {
				// We have already marked this waiting_event as seen. #LazyRemoval
				if _, ok := seen_events[waiting_event.EventID]; ok {
					continue
				}
				if all_parents_seen(waiting_event, seen_events) {
					sorted_events = append(sorted_events, waiting_event)
					seen_events[waiting_event.EventID] = true
				}
			}
		} else {
			waiting_events = append(waiting_events, event)
		}
	}
	return sorted_events
}

func write_shiviz_file(traces []XTrace, shiviz_file string) error {
	f, err := init_shiviz_file(shiviz_file)
	if err != nil {
		return err
	}
	defer f.Close()
	trace := traces[0]
	//print_events(trace)
	event_proc_map := make(map[string]string)
	for _, event := range trace.Events {
		node_name := event.ProcessName + strconv.Itoa(event.ThreadID)
		event_proc_map[event.EventID] = node_name
	}
	clocks := make(map[string]vclock.VClock)
	max_ticks := make(map[string]uint64)
	sorted_events := sort_events(trace.Events)
	for _, event := range sorted_events {
		node_name := event.ProcessName + strconv.Itoa(event.ThreadID)
		vc := vclock.New()
		var  immediate_parent string
		for _, parent := range event.Parents {
			if node_name == event_proc_map[parent] {
				vc = clocks[parent]
				immediate_parent = parent
				break
			}
		}
		// Tick must account for loopbacks and baggage forks.
		if current_ticks, ok := vc.FindTicks(node_name); !ok {
			if max_val, ok := max_ticks[node_name]; !ok {
				vc.Tick(node_name)
			} else {
				vc.Set(node_name, max_val + 1)
			}
		} else {
			if max_val, ok := max_ticks[node_name]; !ok {
				vc.Tick(node_name)
			} else {
				if current_ticks < max_val {
					vc.Set(node_name, max_val)
				}
				vc.Tick(node_name)
			}
		}
		val, _ := vc.FindTicks(node_name)
		max_ticks[node_name] = val
		for _, parent := range event.Parents {
			if parent != immediate_parent {
				vc.Merge(clocks[parent])
			}
		}
		clocks[event.EventID] = vc
		_, err := f.WriteString(node_name + " " + vc.ReturnVCString() + "\n" + event.Label + "\n")
		if err != nil {
			return err
		}
	}
	return nil
}

//@Precondition : filename only contains 1 trace
func read_traces(filename string) ([]XTrace, error) {
	var traces []XTrace
	f, err := os.Open(filename)
	if err != nil {
		return traces, err
	}
	dec := json.NewDecoder(f)
	err = dec.Decode(&traces)
	return traces, err
}

func main() {
	if len(os.Args) != 3 {
		log.Fatal("Usage: go run convert.go <trace_filename> <shiviz_filename>")
	}

	filename := os.Args[1]
	shiviz_filename := os.Args[2]
	traces, err := read_traces(filename)
	if err != nil {
		log.Fatal(err)
	}
	err = write_shiviz_file(traces, shiviz_filename)
	if err != nil {
		log.Fatal(err)
	}
}

