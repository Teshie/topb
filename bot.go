// bot.go
package main

import (
	"context"
	crand "crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	mathrand "math/rand"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	"gorm.io/gorm"
)

/*
Behavior (coordinated + resilient + time-aware):
- Bots are preallocated per room from JSON (conf.json) as a MAX capacity for that room.
- A time-of-day schedule (UTC+3 / Addis Ababa) decides how many of those bots are "on duty".
- Off-duty bots just sleep and don’t connect.
- Bots on duty:
  - Join rooms with jitter.
  - Stagger board selection via per-room slot schedule.
  - After committing a board, they try to start the game (no captain).
  - During play, they track called numbers and claim with human-ish delay + miss chance.
*/

// ===== JSON-driven bot plan =====

type BotPlan struct {
	WSBase string       `json:"ws_base"` // overrides ws base for bots
	Rooms  []BotPlanRow `json:"rooms"`
}

type BotPlanRow struct {
	ID    string `json:"id"`    // room id ("10","20","50",...)
	Stake int    `json:"stake"` // informational
	Bots  int    `json:"bots"`  // max bots to run in this room
}

func loadBotPlanFromJSON(path string) (*BotPlan, error) {
	b, err := os.ReadFile(strings.TrimSpace(path))
	if err != nil {
		return nil, err
	}
	var p BotPlan
	if err := json.Unmarshal(b, &p); err != nil {
		return nil, err
	}
	// minimal sanity
	filtered := make([]BotPlanRow, 0, len(p.Rooms))
	seen := map[string]struct{}{}
	for _, r := range p.Rooms {
		r.ID = strings.TrimSpace(r.ID)
		if r.ID == "" || r.Bots <= 0 {
			continue
		}
		if _, dup := seen[r.ID]; dup {
			continue
		}
		seen[r.ID] = struct{}{}
		filtered = append(filtered, r)
	}
	p.Rooms = filtered
	return &p, nil
}

// ===== Behavior config: all constants, no env =====

type botConfig struct {
	WSBase  string
	Rooms   []string
	PerRoom int // not used when JSON plan is present

	JoinJitter time.Duration

	// Selection staggering and human-ish jitter
	SlotSpacing    time.Duration
	SelectDelayMin time.Duration
	SelectDelayMax time.Duration
	SmallJitterMax time.Duration

	// Quick retry window after a selection conflict
	RetryWithinAfterSelect time.Duration

	// Claim behavior
	ReactMin         time.Duration
	ReactMax         time.Duration
	MissChancePct    int
	BoardSelectStyle string

	// Readiness-gated start retry loop (for all bots post-commit)
	MinBoardsToStart int
	StartRetryMin    time.Duration
	StartRetryMax    time.Duration

	// Start jitter to avoid flood (decentralized starts)
	StartJitterMax time.Duration
}


func newBotConfig() botConfig {
	return botConfig{
		WSBase:  "",  // will be filled from conf.json
		Rooms:   nil, // will be filled from conf.json
		PerRoom: 0,   // unused with JSON plan

		JoinJitter: 1500 * time.Millisecond, // [750ms .. 1500ms] initial join jitter

		SlotSpacing:    180 * time.Millisecond,  // Reduced from 250ms - allows 150 bots in ~12 seconds
		SelectDelayMin: 0,
		SelectDelayMax: 0,
		SmallJitterMax: 120 * time.Millisecond, // Reduced from 200ms

		RetryWithinAfterSelect: 5 * time.Second, // Extended from 2s for more retry opportunities

		ReactMin:         300 * time.Millisecond,
		ReactMax:         1200 * time.Millisecond,
		MissChancePct:    3,
		BoardSelectStyle: "random",

		MinBoardsToStart: 2,
		StartRetryMin:    800 * time.Millisecond,
		StartRetryMax:    1600 * time.Millisecond,

		StartJitterMax: 2 * time.Second,
	}
}

func atoiDefault(s string, def int) int {
	i, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return def
	}
	return i
}

// ---------------- Daily bot schedule (UTC+3) ----------------

// etLoc: Ethiopian time, UTC+3.
var etLoc *time.Location

func init() {
	loc, err := time.LoadLocation("Africa/Addis_Ababa")
	if err != nil {
		// fallback if tzdata not present
		loc = time.FixedZone("UTC+3", 3*60*60)
	}
	etLoc = loc
}

func desiredBotCount(roomID string, now time.Time) int {
	t := now.In(etLoc)
	h := t.Hour()
	m := t.Minute()

	// minutes since midnight
	totalMin := h*60 + m

	type triple struct{ r10, r20, r50 int }
	var base triple

	switch {
	case totalMin >= 5*60 && totalMin < 6*60:
		base = triple{70, 57, 54}

	case totalMin >= 6*60 && totalMin < 12*60: // 06:00–12:00
		base = triple{173, 120, 110}

	case totalMin >= 12*60 && totalMin < 15*60: // 12:00–15:00
		base = triple{173, 120, 100}

	case totalMin >= 15*60 && totalMin < 17*60: // 15:00–17:00
		base = triple{173, 120, 100}

	case totalMin >= 17*60 && totalMin < (23*60+30): // 17:00–23:30 → evening peak of
		base = triple{173, 120, 100}

	case totalMin >= (23*60+30) && totalMin < (24*60+30): // 23:30–00:30 → decline phase
		minutesFrom2330 := totalMin - (23*60 + 30)
		totalWindow := (24*60 + 30) - (23*60 + 30) // 60 minutes

		if totalWindow <= 0 {
			base = triple{100, 70, 60}
			break
		}

		// frac goes from 1.0 at 23:30 down to 0.0 at 00:30
		frac := 1.0 - float64(minutesFrom2330)/float64(totalWindow)
		if frac < 0 {
			frac = 0
		}

		peak := triple{100, 90, 60} // your chosen late-night peak
		base = triple{
			int(frac * float64(peak.r10)),
			int(frac * float64(peak.r20)),
			int(frac * float64(peak.r50)),
		}

	default:
		// 00:30–05:00 → full rest
		base = triple{70, 57, 54}
	}

	switch roomID {
	case "10":
		return base.r10
	case "20":
		return base.r20
	case "50":
		return base.r50
	default:
		return 0
	}
}

// botOnDuty decides if a bot with given slotIdx (0-based) should be active now
// for this room, based on desiredBotCount with random variance.
// NOTE: conf.json Bots MUST be >= max desiredBots for that room.
func botOnDuty(roomID string, slotIdx int, now time.Time) bool {
	target := desiredBotCount(roomID, now)
	// Apply random variance: reduce by 0 to 20 bots, changes every minute
	variance := getRoomVariance(roomID, now)
	target = target - variance
	if target < 0 {
		target = 0
	}
	return slotIdx < target
}

// Per-room random variance that changes every minute
var (
	varianceMu    sync.Mutex
	roomVariance  = map[string]int{}
	varianceTime  = map[string]int64{} // minute timestamp when variance was set
)

// getRoomVariance returns a random variance (0-20) for the room, refreshed every minute
func getRoomVariance(roomID string, now time.Time) int {
	currentMinute := now.Unix() / 60

	varianceMu.Lock()
	defer varianceMu.Unlock()

	lastMinute, exists := varianceTime[roomID]
	if !exists || lastMinute != currentMinute {
		// Generate new random variance: 0 to 20 bots
		roomVariance[roomID] = mathrand.Intn(21) // 0-20
		varianceTime[roomID] = currentMinute
	}

	return roomVariance[roomID]
}

// ---------------- Room-level schedule coordination (per-round) ----------------

type schedState struct {
	Base       time.Time // base time for current pre-play round
	LastStatus string
}

var (
	schedMu     sync.Mutex
	roomSched   = map[string]*schedState{} // roomID -> schedule
	prePlaySet  = map[string]bool{}
	prePlayList = map[string]time.Time{}
)

func isPrePlayStatus(s string) bool {
	switch strings.ToLower(s) {
	case "ready", "pending", "about_to_start":
		return true
	default:
		return false
	}
}

func updateRoomScheduleOnState(roomID, status string) (base time.Time, newlySet bool) {
	now := time.Now()
	schedMu.Lock()
	defer schedMu.Unlock()

	ss, ok := roomSched[roomID]
	if !ok {
		ss = &schedState{}
		roomSched[roomID] = ss
	}

	wasPre := isPrePlayStatus(ss.LastStatus)
	isPre := isPrePlayStatus(status)

	if isPre && !wasPre {
		ss.Base = now.Add(800 * time.Millisecond)
		newlySet = true
		prePlaySet[roomID] = true
		prePlayList[roomID] = now
	}
	if isPre && ss.Base.IsZero() {
		ss.Base = now.Add(800 * time.Millisecond)
		newlySet = true
		prePlaySet[roomID] = true
		prePlayList[roomID] = now
	}

	if !isPre {
		delete(prePlaySet, roomID)
		delete(prePlayList, roomID)
	}

	ss.LastStatus = status
	return ss.Base, newlySet
}

// ---------------- Entrypoint ----------------

func startBots(ctx context.Context) {
	// Always enabled (per your request)
	cfg := newBotConfig()

	// Load JSON plan (path fixed: ./conf.json)
	planPath := "conf.json"
	plan, err := loadBotPlanFromJSON(planPath)
	if err != nil || plan == nil || len(plan.Rooms) == 0 {
		log.Printf("[bots] cannot load plan from %s (%v); bots NOT started", planPath, err)
		return
	}

	cfg.WSBase = strings.TrimSpace(plan.WSBase)
	if cfg.WSBase == "" {
		log.Printf("[bots] WS base is empty in %s; bots NOT started", planPath)
		return
	}

	var rooms []string
	perRoomByID := map[string]int{}
	for _, r := range plan.Rooms {
		rooms = append(rooms, r.ID)
		perRoomByID[r.ID] = r.Bots
	}

	// Compute total bots requested (max capacity from JSON)
	total := 0
	for _, id := range rooms {
		total += perRoomByID[id]
	}
	if total <= 0 {
		log.Println("[bots] total requested bots is 0; check conf.json")
		return
	}

	// Ensure enough bot users exist
	if err := ensureBotUsers(total); err != nil {
		log.Printf("[bots] %v", err)
	}

	// Fetch bot users
	var bots []User
	if err := db.Where("is_bot = ?", true).Limit(total).Find(&bots).Error; err != nil {
		log.Printf("[bots] query error: %v", err)
		return
	}
	if len(bots) == 0 {
		log.Println("[bots] no users with is_bot=true")
		return
	}

	log.Printf("[bots] launching up to %d bots across %d rooms (time-based duty schedule, decentralized start)", total, len(rooms))

	// Round-robin assign across plan
	bi := 0
	for _, roomID := range rooms {
		perRoom := perRoomByID[roomID]
		for i := 0; i < perRoom && bi < len(bots); i, bi = i+1, bi+1 {
			u := bots[bi]

			// Randomized initial join: [JoinJitter/2 .. JoinJitter]
			min := cfg.JoinJitter / 2
			max := cfg.JoinJitter
			if max < time.Millisecond {
				max = 1500 * time.Millisecond
			}
			if min < 0 {
				min = 0
			}
			delay := randBetween(min, max)

			slotIdx := i // 0..(perRoom-1)

			go func(b User, rid string, idx int, d time.Duration) {
				select {
				case <-ctx.Done():
					return
				case <-time.After(d):
					runBotForever(ctx, cfg, rid, b.TelegramID, idx)
				}
			}(u, roomID, slotIdx, delay)
		}
	}
}

func runBotForever(ctx context.Context, cfg botConfig, roomID string, tid int64, slotIdx int) {
	// per-goroutine randomness
	var seedBytes [8]byte
	_, _ = crand.Read(seedBytes[:])
	mathrand.Seed(time.Now().UnixNano() ^ int64(seedBytes[0])<<56)

	for {
		// Time-based duty check: if this slot is off-duty for this room now, sleep.
		if !botOnDuty(roomID, slotIdx, time.Now()) {
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Minute):
				continue
			}
		}

		if err := botSession(ctx, cfg, roomID, tid, slotIdx); err != nil {
			log.Printf("[bot tid=%d room=%s] session end: %v", tid, roomID, err)
		}
		// backoff before reconnect
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(1500+mathrand.Intn(2500)) * time.Millisecond):
		}
	}
}

func makeJWT(tid int64) (string, error) {
	// Only env we still use
	secret := strings.TrimSpace(os.Getenv("JWT_SECRET"))
	if secret == "" {
		return "", fmt.Errorf("JWT_SECRET not set")
	}
	claims := jwt.MapClaims{
		"tid": tid,
		"exp": time.Now().Add(24 * time.Hour).Unix(),
		"iat": time.Now().Unix(),
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(secret))
}

// ---------------- Bot session ----------------

func botSession(ctx context.Context, cfg botConfig, roomID string, tid int64, slotIdx int) error {
	j, err := makeJWT(tid)
	if err != nil {
		return err
	}

	wsURL := strings.TrimRight(cfg.WSBase, "/") + "/ws/room/" + url.PathEscape(roomID) + "?token=" + url.QueryEscape(j)
	dialer := websocket.Dialer{HandshakeTimeout: 6 * time.Second}
	conn, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial %s: %w", wsURL, err)
	}
	defer conn.Close()

	type roundState struct {
		status  string     // ready|pending|about_to_start|playing|claimed
		startAt *time.Time // parsed from room.start_time (UTC)

		myBoard *int
		called  []int

		// selection lifecycle
		boardCommitted bool
		pendingChoice  *int
		selectTimer    *time.Timer
		selectAt       time.Time
		roundBase      time.Time

		// readiness-gated start retry loop (decentralized: all bots post-commit)
		startRetryTimer  *time.Timer
		startRetryActive bool
		committedCount   int

		// Track latest room state for fresh board selection
		latestRoom       map[string]any
		selectRetryTimer *time.Timer
		selectRetryCount int
	}

	st := &roundState{
		status:           "",
		called:           []int{},
		roundBase:        time.Time{},
		selectAt:         time.Time{},
		selectTimer:      nil,
		startRetryTimer:  nil,
		startRetryActive: false,
		committedCount:   0,
		latestRoom:       nil,
		selectRetryTimer: nil,
		selectRetryCount: 0,
	}

	send := func(v any) error {
		b, _ := json.Marshal(v)
		_ = conn.SetWriteDeadline(time.Now().Add(4 * time.Second))
		return conn.WriteMessage(websocket.TextMessage, b)
	}
	resetSelect := func() {
		if st.selectTimer != nil {
			st.selectTimer.Stop()
			st.selectTimer = nil
		}
		if st.selectRetryTimer != nil {
			st.selectRetryTimer.Stop()
			st.selectRetryTimer = nil
		}
		st.pendingChoice = nil
		st.selectAt = time.Time{}
		st.selectRetryCount = 0
	}
	cancelStartRetry := func() {
		if st.startRetryTimer != nil {
			st.startRetryTimer.Stop()
			st.startRetryTimer = nil
		}
		st.startRetryActive = false
	}
	resetPerRoundFlags := func() {
		st.boardCommitted = false
		resetSelect()
		st.myBoard = nil
		st.startAt = nil
		cancelStartRetry()
	}

	// helpers
	commitCount := func(room map[string]any) int {
		if room == nil {
			return 0
		}
		if arr, ok := room["selected_board_numbers"].([]any); ok {
			return len(arr)
		}
		return 0
	}
	shouldAttemptStart := func(st *roundState, cfg botConfig) bool {
		if st.status != "ready" && st.status != "pending" && st.status != "about_to_start" {
			return false
		}
		if st.startAt != nil { // start_time already present
			return false
		}
		if st.myBoard == nil || !st.boardCommitted {
			return false
		}
		if st.committedCount < cfg.MinBoardsToStart {
			return false
		}
		return true
	}
	startRetryOnce := func() {
		// Jitter before first send to stagger decentralized attempts
		if cfg.StartJitterMax > 0 {
			jitter := randBetween(0, cfg.StartJitterMax)
			time.Sleep(jitter)
		}
		_ = send(map[string]any{"action": "start"})
	}
	scheduleStartRetry := func() {
		if st.startRetryActive {
			return
		}
		st.startRetryActive = true
		var loop func()
		loop = func() {
			// stop if conditions are no longer true
			if st.startAt != nil || !(st.status == "ready" || st.status == "pending" || st.status == "about_to_start") {
				cancelStartRetry()
				return
			}
			startRetryOnce()
			d := randBetween(cfg.StartRetryMin, cfg.StartRetryMax)
			st.startRetryTimer = time.AfterFunc(d, loop)
		}
		// fire first attempt (with jitter) immediately
		loop()
	}

	// Declare scheduleSelectRetry first for recursive calls
	var scheduleSelectRetry func()

	pickAndSendBoard := func() {
		// Use the latest room state, not stale captured data
		taken := map[int]bool{}
		if st.latestRoom != nil {
			if arr, ok := st.latestRoom["selected_board_numbers"].([]any); ok {
				for _, v := range arr {
					if f, ok := v.(float64); ok {
						taken[int(f)] = true
					}
				}
			}
		}
		choice := pickBoard(taken, cfg.BoardSelectStyle)
		if choice > 0 {
			st.pendingChoice = &choice
			_ = send(map[string]any{"action": "set_board", "board_number": choice})
		}
	}

	// Schedule automatic retry for board selection if not confirmed
	scheduleSelectRetry = func() {
		if st.selectRetryTimer != nil {
			return // already scheduled
		}
		st.selectRetryTimer = time.AfterFunc(500*time.Millisecond, func() {
			st.selectRetryTimer = nil
			// Only retry if still pre-play, not committed, no board confirmed, and still on duty
			if isPrePlayStatus(st.status) && !st.boardCommitted && st.myBoard == nil && botOnDuty(roomID, slotIdx, time.Now()) {
				st.selectRetryCount++
				if st.selectRetryCount <= 10 { // Max 10 retries
					pickAndSendBoard()
					scheduleSelectRetry() // Schedule next retry
				}
			}
		})
	}

	// read loop
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		var msg map[string]any
		if json.Unmarshal(data, &msg) != nil {
			continue
		}

		switch strings.ToLower(fmt.Sprint(msg["type"])) {
		case "room_state":
			room, _ := msg["room"].(map[string]any)
			oldStatus := st.status

			// Store the latest room state for fresh board selection
			st.latestRoom = room

			// update status & startAt
			if room != nil {
				if s, ok := room["status"].(string); ok {
					st.status = strings.ToLower(strings.TrimSpace(s))
				}
				st.startAt = nil
				if ts, ok := room["start_time"].(string); ok && strings.TrimSpace(ts) != "" {
					if t, err := time.Parse(time.RFC3339, ts); err == nil {
						st.startAt = &t
					}
				}
			}

			// your board echoed by server
			if v, ok := msg["your_board_number"]; ok && v != nil {
				switch t := v.(type) {
				case float64:
					b := int(t)
					st.myBoard = &b
				case int:
					b := t
					st.myBoard = &b
				default:
					st.myBoard = nil
				}
			} else {
				st.myBoard = nil
			}

			// round transition: live/claimed -> pre-play => clear per-round flags
			if (oldStatus == "playing" || oldStatus == "claimed") && isPrePlayStatus(st.status) {
				resetPerRoundFlags()
			}
			// live/claimed → no pending selection or start retry
			if st.status == "playing" || st.status == "claimed" {
				resetSelect()
				cancelStartRetry()
			}

			// commit detection (our pending choice got committed)
			if st.pendingChoice != nil && st.myBoard != nil && *st.myBoard == *st.pendingChoice {
				st.boardCommitted = true
				resetSelect()
			}

			// selection staggering (respect on-duty schedule)
			if isPrePlayStatus(st.status) && !st.boardCommitted && st.myBoard == nil {
				// If this bot is not on duty at this time for this room, skip selection this round
				if !botOnDuty(roomID, slotIdx, time.Now()) {
					updateRoomScheduleOnState(roomID, st.status)
				} else {
					base, _ := updateRoomScheduleOnState(roomID, st.status)
					// refresh base if changed significantly
					if st.roundBase.IsZero() || base.After(st.roundBase.Add(1*time.Second)) || base.Before(st.roundBase.Add(-1*time.Second)) {
						st.roundBase = base

						slotTime := base.Add(time.Duration(slotIdx) * cfg.SlotSpacing)
						if cfg.SmallJitterMax > 0 {
							slotTime = slotTime.Add(randBetween(0, cfg.SmallJitterMax))
						}
						if cfg.SelectDelayMax > 0 {
							slotTime = slotTime.Add(randBetween(cfg.SelectDelayMin, cfg.SelectDelayMax))
						}
						st.selectAt = slotTime

						delay := time.Until(slotTime)
						if delay < 0 {
							delay = 0
						}
						resetSelect()
						st.selectTimer = time.AfterFunc(delay, func() {
							// Only select if still pre-play, not committed, no board, and still on duty
							if isPrePlayStatus(st.status) && !st.boardCommitted && st.myBoard == nil && botOnDuty(roomID, slotIdx, time.Now()) {
								pickAndSendBoard()
								scheduleSelectRetry() // Auto-retry if selection fails
							}
						})
					}
				}
			} else {
				updateRoomScheduleOnState(roomID, st.status)
			}

			// update committed count
			st.committedCount = commitCount(room)

			// Decentralized start retry: Trigger if personally ready (post-commit) + low threshold
			if shouldAttemptStart(st, cfg) {
				scheduleStartRetry()
			} else {
				cancelStartRetry()
			}

		case "called_numbers":
			// Update called numbers
			st.called = st.called[:0]
			if arr, ok := msg["called_numbers"].([]any); ok {
				for _, v := range arr {
					if f, ok := v.(float64); ok {
						st.called = append(st.called, int(f))
					}
				}
			}
			// If live and we have a committed board, consider claiming
			if st.status == "playing" && st.myBoard != nil {
				if botHasBingo(*st.myBoard, st.called) {
					// pretend we just noticed it (human-like delay)
					delay := randBetween(cfg.ReactMin, cfg.ReactMax)
					time.AfterFunc(delay, func() {
						if cfg.MissChancePct > 0 && mathrand.Intn(100) < cfg.MissChancePct {
							log.Printf("[bot %d] deliberately missing bingo", tid)
							return
						}
						log.Printf("[bot %d room=%s] claiming bingo (forced check)", tid, roomID)
						_ = send(map[string]any{
							"action":    "claim",
							"request_id": uuid4(),
						})
					})
				}
			}

		case "winners":
			// round ended; per-round flags are reset when we re-enter pre-play via room_state

		case "error":
			// selection conflict? schedule retry with fresh room data
			if isPrePlayStatus(st.status) && st.myBoard == nil && !st.boardCommitted {
				if time.Since(st.selectAt) <= cfg.RetryWithinAfterSelect {
					// Use the new retry mechanism with fresh data
					scheduleSelectRetry()
				}
			}
		}
	}
}

func pickBoard(taken map[int]bool, style string) int {
	max := maxBoardNumber()
	if max <= 0 {
		max = 100
	}
	switch strings.ToLower(style) {
	case "random":
		for tries := 0; tries < 64; tries++ {
			cand := 1 + mathrand.Intn(max)
			if !taken[cand] {
				return cand
			}
		}
		fallthrough
	default: // "first_free"
		for i := 1; i <= max; i++ {
			if !taken[i] {
				return i
			}
		}
	}
	return 0
}

// botHasBingo uses the same server-side board + checker (helpers must exist in your project).
func botHasBingo(boardNum int, called []int) bool {
	b, ok := getBoard(boardNum)
	if !ok {
		return false
	}
	okWin, _ := checkBingo(b, called)
	return okWin
}

func randBetween(min, max time.Duration) time.Duration {
	if max <= min {
		return min
	}
	diff := max - min
	n := mathrand.Int63n(int64(diff))
	return min + time.Duration(n)
}

func uuid4() string {
	var b [16]byte
	_, _ = crand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// Ensure at least N bot users exist before launching.
func ensureBotUsers(minCount int) error {
	var n int64
	if err := db.Model(&User{}).Where("is_bot = ?", true).Count(&n).Error; err != nil && err != gorm.ErrRecordNotFound {
		return err
	}
	if int(n) >= minCount {
		return nil
	}
	return fmt.Errorf("only %d bot users present; seed more before enabling bots", n)
}
