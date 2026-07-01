package main

import (
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

//go:embed index.html
var indexHTML []byte

// ---- worker events ----

type Event struct {
	ID          int64           `json:"id"`
	Type        string          `json:"event_type"`
	Title       string          `json:"title"`
	Description string          `json:"description"`
	Metadata    json.RawMessage `json:"metadata,omitempty"`
	Time        time.Time       `json:"time"`
}

type Worker struct {
	Key      string    `json:"key"`
	Title    string    `json:"title"`
	PaneLoc  string    `json:"pane_loc,omitempty"`
	LastType string    `json:"last_event_type"`
	LastSeen time.Time `json:"last_seen"`
}

// Store holds events and active workers, mirrored to dataDir: events are
// appended to events.jsonl and replayed on startup (which also rebuilds the
// workers map); the read marker lives in read.txt.
type Store struct {
	mu       sync.Mutex
	events   []Event
	nextID   int64
	lastRead int64
	workers  map[string]*Worker
	subs     map[chan []byte]struct{}
	dataDir  string
}

func NewStore(dataDir string) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{
		nextID:  1,
		workers: map[string]*Worker{},
		subs:    map[chan []byte]struct{}{},
		dataDir: dataDir,
	}
	if data, err := os.ReadFile(s.eventsPath()); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if line == "" {
				continue
			}
			var e Event
			if err := json.Unmarshal([]byte(line), &e); err != nil {
				log.Printf("skipping corrupt event line: %v", err)
				continue
			}
			s.events = append(s.events, e)
			if e.ID >= s.nextID {
				s.nextID = e.ID + 1
			}
		}
		s.events = retain(s.events)
		s.rewriteEvents()
		for _, e := range s.events {
			s.updateWorkers(e)
		}
	}
	if data, err := os.ReadFile(s.readPath()); err == nil {
		fmt.Sscanf(string(data), "%d", &s.lastRead)
	}
	return s, nil
}

func (s *Store) eventsPath() string { return filepath.Join(s.dataDir, "events.jsonl") }
func (s *Store) readPath() string   { return filepath.Join(s.dataDir, "read.txt") }

// rewriteEvents compacts events.jsonl down to the currently retained events,
// dropping anything pruned on load. Written via a temp file + rename so a crash
// can't leave the log half-written.
func (s *Store) rewriteEvents() {
	var b strings.Builder
	for _, e := range s.events {
		if line, err := json.Marshal(e); err == nil {
			b.Write(line)
			b.WriteByte('\n')
		}
	}
	tmp := s.eventsPath() + ".tmp"
	if err := os.WriteFile(tmp, []byte(b.String()), 0o644); err != nil {
		log.Printf("compact events: %v", err)
		return
	}
	if err := os.Rename(tmp, s.eventsPath()); err != nil {
		log.Printf("compact events: %v", err)
	}
}

const (
	maxEvents = 100
	maxAge    = 7 * 24 * time.Hour
)

// retain drops events older than maxAge and caps the count at maxEvents. Events
// are stored in ascending time order, so stale ones are always a prefix.
func retain(events []Event) []Event {
	cutoff := time.Now().Add(-maxAge)
	i := 0
	for i < len(events) && events[i].Time.Before(cutoff) {
		i++
	}
	events = events[i:]
	if len(events) > maxEvents {
		events = events[len(events)-maxEvents:]
	}
	return events
}

func (s *Store) Add(e Event) Event {
	s.mu.Lock()
	e.ID = s.nextID
	s.nextID++
	e.Time = time.Now()
	s.events = append(s.events, e)
	s.events = retain(s.events)
	s.updateWorkers(e)
	if line, err := json.Marshal(e); err == nil {
		f, err := os.OpenFile(s.eventsPath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err == nil {
			f.Write(append(line, '\n'))
			f.Close()
		} else {
			log.Printf("persist event: %v", err)
		}
	}
	payload, _ := json.Marshal(e)
	for ch := range s.subs {
		select {
		case ch <- payload:
		default: // slow subscriber; drop rather than block
		}
	}
	s.mu.Unlock()
	return e
}

// updateWorkers must be called with the lock held.
func (s *Store) updateWorkers(e Event) {
	var meta struct {
		SessionID string `json:"session_id"`
		PaneLoc   string `json:"pane_loc"`
	}
	json.Unmarshal(e.Metadata, &meta)
	key := meta.SessionID
	if key == "" {
		key = meta.PaneLoc
	}
	if key == "" {
		return
	}
	if e.Type == "session_end" {
		delete(s.workers, key)
		return
	}
	w := s.workers[key]
	if w == nil {
		w = &Worker{Key: key}
		s.workers[key] = w
	}
	if e.Title != "" {
		w.Title = e.Title
	}
	if meta.PaneLoc != "" {
		w.PaneLoc = meta.PaneLoc
	}
	w.LastType = e.Type
	w.LastSeen = e.Time
}

// MarkRead marks all events with ID <= id as read.
func (s *Store) MarkRead(id int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastRead = id
	if err := os.WriteFile(s.readPath(), []byte(fmt.Sprintf("%d\n", id)), 0o644); err != nil {
		log.Printf("persist read marker: %v", err)
	}
}

func (s *Store) LastRead() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastRead
}

func (s *Store) Events() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Event, len(s.events))
	copy(out, s.events)
	return out
}

// ForgetWorker removes a worker regardless of its last event, for clearing
// entries orphaned by a missed session_end (e.g. pane killed without a clean
// exit). Reports whether the key was present.
func (s *Store) ForgetWorker(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.workers[key]; !ok {
		return false
	}
	delete(s.workers, key)
	return true
}

func (s *Store) Workers() []Worker {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Worker, 0, len(s.workers))
	for _, w := range s.workers {
		out = append(out, *w)
	}
	return out
}

func (s *Store) Subscribe() chan []byte {
	ch := make(chan []byte, 16)
	s.mu.Lock()
	s.subs[ch] = struct{}{}
	s.mu.Unlock()
	return ch
}

func (s *Store) Unsubscribe(ch chan []byte) {
	s.mu.Lock()
	delete(s.subs, ch)
	s.mu.Unlock()
}

// ---- main / handlers ----

type stringList []string

func (l *stringList) String() string     { return strings.Join(*l, ",") }
func (l *stringList) Set(v string) error { *l = append(*l, v); return nil }

var gotoTarget = regexp.MustCompile(`^[A-Za-z0-9._-]*@[0-9]+-%[0-9]+$`)

func main() {
	var allow stringList
	port := flag.Int("port", 8723, "port to listen on (binds 127.0.0.1)")
	gotoScript := flag.String("goto-script", "", "explicit path to goto-pane-location (overrides -scripts)")
	home, _ := os.UserHomeDir()
	// Default: scripts/ next to the compiled binary. For go run, set TASKBOARD_SCRIPTS_PATH.
	exe, _ := os.Executable()
	defaultScripts := filepath.Join(filepath.Dir(exe), "scripts")
	if u := os.Getenv("TASKBOARD_SCRIPTS_PATH"); u != "" {
		defaultScripts = u
	}
	scriptsPath := flag.String("scripts", defaultScripts, "directory containing helper scripts (goto-pane-location, etc.); overridden by TASKBOARD_SCRIPTS_PATH env var")
	dataDir := flag.String("data", filepath.Join(home, ".taskboard"), "directory for persisted events and read marker")
	flag.Var(&allow, "allow", "allowed filename to serve (repeatable, default context.txt)")
	flag.Parse()
	if len(allow) == 0 {
		allow = stringList{"context.txt"}
	}

	root := os.Getenv("TASK_QUEUE_PATH")
	if root == "" {
		log.Fatal("TASK_QUEUE_PATH is not set")
	}

	scripts, _ := filepath.Abs(*scriptsPath)
	script := *gotoScript
	if script == "" {
		script = filepath.Join(scripts, "goto-pane-location")
	}
	script, _ = filepath.Abs(script)

	store, err := NewStore(*dataDir)
	if err != nil {
		log.Fatalf("init store: %v", err)
	}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})

	mux.HandleFunc("GET /api/files", func(w http.ResponseWriter, r *http.Request) {
		var files []map[string]any
		filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if d.IsDir() {
				// Don't descend into git worktrees/repos: task files live in the
				// task dir itself, not inside the checked-out repos (which hold
				// tens of thousands of files and dominate the walk).
				if path != root {
					if _, err := os.Stat(filepath.Join(path, ".git")); err == nil {
						return filepath.SkipDir
					}
				}
				return nil
			}
			for _, name := range allow {
				if d.Name() == name {
					rel, _ := filepath.Rel(root, path)
					info, _ := d.Info()
					f := map[string]any{"path": rel}
					if info != nil {
						f["mtime"] = info.ModTime()
					}
					files = append(files, f)
				}
			}
			return nil
		})
		writeJSON(w, files)
	})

	mux.HandleFunc("GET /api/file", func(w http.ResponseWriter, r *http.Request) {
		rel := r.URL.Query().Get("path")
		full := filepath.Join(root, filepath.Clean("/"+rel))
		if !strings.HasPrefix(full, root+string(filepath.Separator)) || !allowed(allow, filepath.Base(full)) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		data, err := os.ReadFile(full)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		writeJSON(w, map[string]any{"path": rel, "content": string(data)})
	})

	mux.HandleFunc("PUT /api/file", func(w http.ResponseWriter, r *http.Request) {
		rel := r.URL.Query().Get("path")
		full := filepath.Join(root, filepath.Clean("/"+rel))
		if !strings.HasPrefix(full, root+string(filepath.Separator)) || !allowed(allow, filepath.Base(full)) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		var in struct {
			Content string `json:"content"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}
		if err := os.WriteFile(full, []byte(in.Content), 0o644); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]any{"ok": true})
	})

	mux.HandleFunc("GET /api/events", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"events": store.Events(), "last_read": store.LastRead()})
	})

	mux.HandleFunc("POST /api/read", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			ID int64 `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}
		store.MarkRead(in.ID)
		writeJSON(w, map[string]any{"ok": true, "last_read": in.ID})
	})

	mux.HandleFunc("POST /api/events", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Type        string          `json:"event_type"`
			Title       string          `json:"title"`
			Description string          `json:"description"`
			Metadata    json.RawMessage `json:"metadata"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}
		if in.Type == "" {
			http.Error(w, "event_type is required", http.StatusBadRequest)
			return
		}
		e := store.Add(Event{Type: in.Type, Title: in.Title, Description: in.Description, Metadata: in.Metadata})
		writeJSON(w, e)
	})

	mux.HandleFunc("GET /api/workers", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, store.Workers())
	})

	mux.HandleFunc("DELETE /api/workers", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Key string `json:"key"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Key == "" {
			http.Error(w, "key is required", http.StatusBadRequest)
			return
		}
		if !store.ForgetWorker(in.Key) {
			http.Error(w, "no such worker", http.StatusNotFound)
			return
		}
		writeJSON(w, map[string]any{"ok": true})
	})

	mux.HandleFunc("GET /api/stream", func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		ch := store.Subscribe()
		defer store.Unsubscribe(ch)
		// Flush headers immediately so the client's EventSource fires onopen at
		// once; otherwise no bytes are sent until the first event or ping (up to
		// 25s), which stalls the initial events/workers load behind onopen.
		fmt.Fprint(w, ": connected\n\n")
		flusher.Flush()
		// Heartbeat keeps idle connections from being dropped (sleep/wake,
		// proxies) and lets the browser detect dead ones promptly.
		ping := time.NewTicker(25 * time.Second)
		defer ping.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-ping.C:
				fmt.Fprint(w, ": ping\n\n")
				flusher.Flush()
			case msg := <-ch:
				fmt.Fprintf(w, "data: %s\n\n", msg)
				flusher.Flush()
			}
		}
	})

	mux.HandleFunc("POST /api/goto", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Target string `json:"target"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil || !gotoTarget.MatchString(in.Target) {
			http.Error(w, "target must match slug-@N-%N", http.StatusBadRequest)
			return
		}
		out, err := exec.Command(script, in.Target).CombinedOutput()
		if err != nil {
			http.Error(w, fmt.Sprintf("%v: %s", err, out), http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]any{"ok": true, "output": string(out)})
	})

	addr := fmt.Sprintf("127.0.0.1:%d", *port)
	log.Printf("taskboard on http://%s (root=%s, allow=%v, scripts=%s, goto=%s)", addr, root, allow, scripts, script)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func allowed(allow []string, name string) bool {
	for _, a := range allow {
		if a == name {
			return true
		}
	}
	return false
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
