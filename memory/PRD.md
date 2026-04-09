# Dire Bingo Backend PRD

## Original Problem Statement
Fix slow SQL queries (500ms-6000ms) in Go backend with PostgreSQL:
- UPDATE on `rooms` table taking 5-6 seconds
- INSERT/UPSERT on `room_players` taking 500ms+
- SELECT on `room_players` taking 600ms+

## Architecture
- **Language**: Go (Gin framework)
- **Database**: PostgreSQL via GORM
- **Features**: WebSocket-based real-time Bingo game rooms

## User Personas
- Telegram users playing Bingo via mini app
- Admins managing rooms and payouts

## Core Requirements (Static)
- Real-time game state synchronization
- Low-latency database operations
- Concurrent multi-room support

## Implementation Log

### 2026-01-XX - SQL Performance Optimization
**Changes Made:**
1. Connection pool configuration (50 max open, 25 idle, 5-min lifetime)
2. Enabled `PrepareStmt` for query caching
3. Added individual indexes on `room_players.room_id` and `room_players.telegram_id`
4. Added composite index on `room_players(room_id, telegram_id)`
5. Added debouncing (100ms) to `saveRoomStateToDB()` to reduce write frequency

**Files Modified:**
- `/app/main.go` - initDB(), createPerformanceIndexes()
- `/app/ws.go` - RoomPlayer struct, saveRoomStateToDB()

## Prioritized Backlog
- P0: ✅ Fix slow queries (DONE)
- P1: Add Redis caching for room states
- P2: Add query monitoring/alerting
- P2: Review lock contention patterns

## Next Tasks
1. Deploy and validate query improvements
2. Run `ANALYZE` on affected tables
3. Monitor pg_stat_activity for lock issues
