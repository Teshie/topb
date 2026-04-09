package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/golang-jwt/jwt/v5"
	"github.com/joho/godotenv"

	"github.com/gin-gonic/gin"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

/*
ENV (recommended):
  PG_DSN="host=localhost user=postgres password=postgres dbname=users port=5432 sslmode=disable"
  JWT_SECRET="supersecret"
  PORT="8004"
  BOT_USERNAME="YourBotWithoutAt"
  REFERRAL_REWARD_BIRR="10"

# NEW toggles:
  REFERRAL_INVITEE_BIRR="5"                 # bonus to the joiner; 0 to disable
  REFERRAL_ALLOW_DUPLICATE="true"           # TEST MODE: credit even if already credited before
  REFERRAL_CREDIT_EXISTING_USERS="true"     # allow credit when existing user arrives with a start param
  REFERRAL_NOTIFY="true"                    # send referral notifications
*/

var db *gorm.DB

/* ==========================
   Auth / Context
========================== */

type ctxKey string

const ctxTID ctxKey = "tid"

func authJWT() gin.HandlerFunc {
	return func(c *gin.Context) {
		h := c.GetHeader("Authorization")
		if !strings.HasPrefix(strings.ToLower(h), "bearer ") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing bearer token"})
			return
		}
		tokenStr := strings.TrimSpace(h[len("Bearer "):])

		secret := strings.TrimSpace(os.Getenv("JWT_SECRET"))
		if secret == "" {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "server misconfigured"})
			return
		}

		tok, err := jwt.Parse(tokenStr, func(t *jwt.Token) (interface{}, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method")
			}
			return []byte(secret), nil
		})
		if err != nil || !tok.Valid {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
			return
		}

		claims, ok := tok.Claims.(jwt.MapClaims)
		if !ok {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid claims"})
			return
		}

		v, ok := claims["tid"]
		if !ok {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "no tid claim"})
			return
		}

		var tid int64
		switch t := v.(type) {
		case float64:
			tid = int64(t)
		case int64:
			tid = t
		case string:
			if p, e := strconv.ParseInt(t, 10, 64); e == nil {
				tid = p
			}
		}
		if tid == 0 {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "bad tid"})
			return
		}

		c.Set(string(ctxTID), tid)
		c.Next()
	}
}

/* ==========================
   Models used here
   (Room + RoomPlayer are in ws.go)
========================== */
// POST /admin/rooms/:room_id/reset
func resetRoomHandler(c *gin.Context) {
	roomID := strings.TrimSpace(c.Param("room_id"))
	if roomID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "room_id required"})
		return
	}

	// Reset in DB
	if err := db.Transaction(func(tx *gorm.DB) error {
		// Set room back to pending/idle, clear start_time & possible_win
		if err := tx.Model(&Room{}).
			Where("room_id = ?", roomID).
			Updates(map[string]interface{}{
				"status":       "pending",
				"start_time":   gorm.Expr("NULL"),
				"possible_win": 0,
			}).Error; err != nil {
			return err
		}

		// Remove all players & selections for that room
		if err := tx.Where("room_id = ?", roomID).Delete(&RoomPlayer{}).Error; err != nil {
			return err
		}
		return nil
	}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Refresh in-memory hub from DB so all clients get the reset state
	if rooms, err := loadAllRoomsFromDB(); err == nil {
		globalRoomsHub.replaceAll(rooms)
		globalRoomsHub.broadcastRooms()
	} else {
		log.Printf("resetRoomHandler: loadAllRoomsFromDB error: %v", err)
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok", "room_id": roomID})
}

type User struct {
	ID                uint      `json:"id" gorm:"primaryKey"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
	TelegramID        int64     `json:"telegram_id" gorm:"uniqueIndex;not null"`
	Username          string    `json:"username"`
	FirstName         string    `json:"first_name"`
	LastName          string    `json:"last_name"`
	Name              string    `json:"name"`
	Email             string    `json:"email"`
	Phone             *string   `json:"phone" gorm:"uniqueIndex;type:varchar(32)"`
	PhoneVerifiedAt   *time.Time
	BalanceCents      int64      `json:"-" gorm:"not null;default:0"`
	MainBalanceCents  int64      `json:"-" gorm:"not null;default:0"`
	IsBot             bool       `gorm:"not null;default:false"`
	IsAdmin           bool       `json:"is_admin" gorm:"column:is_admin"`
	BotStrategy       string     `gorm:"type:varchar(64);not null;default:'default'"`
	BotReactionMsMin  int        `gorm:"not null;default:300"`
	BotReactionMsMax  int        `gorm:"not null;default:1200"`
	BotMissChancePct  int        `gorm:"not null;default:0"`
	PhoneBonusAt      *time.Time `json:"-" gorm:""`
	HasDeposit        bool       `json:"has_deposit" gorm:"not null;default:false"`
	ReferredBy        *int64     `json:"referred_by" gorm:"index"`
	InviterTelegramID *int64     `json:"inviter_telegram_id" gorm:"index"` // 👈 NEW FIELD

}

type TxnType string

const (
	TxnDeposit  TxnType = "deposit"
	TxnWithdraw TxnType = "withdraw"
)

// Referral model (unique per invitee in normal mode)
type Referral struct {
	ID          uint      `json:"id" gorm:"primaryKey"`
	CreatedAt   time.Time `json:"created_at"`
	InviterTID  int64     `json:"inviter_tid" gorm:"index"`
	InviteeTID  int64     `json:"invitee_tid" gorm:"uniqueIndex:uniq_invitee"`
	AmountCents int64     `json:"amount_cents" gorm:"not null;default:0"`
}
type ReferralPayout struct {
	ID          uint `gorm:"primaryKey"`
	CreatedAt   time.Time
	InviterTID  int64 `gorm:"index"`
	InviteeTID  int64 `gorm:"index"`
	DepositTxID uint  `gorm:"uniqueIndex:uniq_deposit"` // unique per deposit
	AmountCents int64
}

type Transaction struct {
	ID           uint      `json:"id" gorm:"primaryKey"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
	TelegramID   int64     `json:"telegram_id" gorm:"index"`
	Type         string    `json:"type" gorm:"type:varchar(20)"`
	Method       string    `json:"method" gorm:"type:varchar(30)"`
	Status       string    `json:"status" gorm:"type:varchar(20)"`
	Note         string    `json:"note" gorm:"type:text"`
	Amount       string    `json:"amount" gorm:"type:numeric(18,2)"`
	Bonus        string    `json:"bonus" gorm:"type:numeric(18,2)"`
	Total        string    `json:"total" gorm:"type:numeric(18,2)"`
	Reference    *string   `json:"reference" gorm:"type:varchar(64)"`
	TxID         *string   `json:"txid" gorm:"type:varchar(64)"`
	Account      string    `json:"account" gorm:"type:varchar(64)"`
	SenderName   string    `json:"sender_name" gorm:"type:varchar(128)"`
	ReceiverName string    `json:"receiver_name" gorm:"type:varchar(128)"`
	ReceiptURL   string    `json:"receipt_url" gorm:"type:text"`
	PaymentDate  string    `json:"payment_date" gorm:"type:varchar(64)"`
}

/* ==========================
   DTOs & helpers
========================== */

type UserDTO struct {
	ID                uint      `json:"id"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
	TelegramID        int64     `json:"telegram_id"`
	Username          string    `json:"username"`
	FirstName         string    `json:"first_name"`
	LastName          string    `json:"last_name"`
	Name              string    `json:"name"`
	Email             string    `json:"email"`
	Phone             string    `json:"phone"`
	HasPhone          bool      `json:"has_phone"`
	BalanceBirr       string    `json:"balance_birr"`
	MainBalanceBirr   string    `json:"main_balance_birr"`
	IsAdmin           bool      `json:"is_admin"`
	HasDeposit        bool      `json:"has_deposit" `
	IsBot             bool      `json:"is_bot" `
	InviterTelegramID *int64    `json:"inviter_telegram_id"` // 👈 NEW

}
type BonusDepositorsReq struct {
	Amount string `json:"amount" binding:"required"` // e.g. "100" or "100.00"
	Tag    string `json:"tag"`                       // optional idempotency tag, e.g. "sep2025"
	Note   string `json:"note"`                      // optional
}

type BonusDepositorsResp struct {
	Credited     int     `json:"credited"`
	Skipped      int     `json:"skipped"`
	Already      int     `json:"already"`
	CreditedTIDs []int64 `json:"credited_tids,omitempty"` // NEW
}

// helper: parse truthy envs
func boolFromEnv(key string, def bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if v == "" {
		return def
	}
	switch v {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return def
	}
}

// helper: send HTML DM and return (retryAfterSec, permanent, err)

// POST /admin/bonuses/depositors  (JWT admin)
func adminBonusDepositors(c *gin.Context) {
	raw, _ := c.Get(string(ctxTID))
	callerTID, _ := raw.(int64)
	if !isUserAdminByTID(callerTID) {
		c.JSON(http.StatusForbidden, gin.H{"error": "admin only"})
		return
	}

	var req BonusDepositorsReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	amtCents, err := fromBirrToCents(req.Amount)
	if err != nil || amtCents <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid amount"})
		return
	}

	tag := strings.TrimSpace(req.Tag)
	if tag == "" {
		tag = time.Now().Format("20060102")
	}
	refFor := func(tid int64) *string {
		return ptrOrNil(fmt.Sprintf("depositor_bonus:%s:%d", tag, tid))
	}

	var tids []int64
	if err := db.Model(&Transaction{}).
		Where("type = ? AND status = 'success'", "deposit").
		Distinct().Pluck("telegram_id", &tids).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	resp := BonusDepositorsResp{}
	amountStr := toBirrString(amtCents)
	note := strings.TrimSpace(req.Note)
	if note == "" {
		note = fmt.Sprintf("Promo bonus (%s) for having deposited", tag)
	}

	creditedTIDs := make([]int64, 0, len(tids))

	if err := db.Transaction(func(tx *gorm.DB) error {
		for _, tid := range tids {
			var u User
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Where("telegram_id = ?", tid).First(&u).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					resp.Skipped++
					continue
				}
				return err
			}

			t := Transaction{
				Reference:  refFor(tid),
				TelegramID: tid,
				Type:       "promo_bonus",
				Method:     "+",
				Status:     "success",
				Note:       note,
				Amount:     amountStr,
				Bonus:      "0.00",
				Total:      amountStr,
				Account:    "balance", // Play balance
			}
			if err := tx.Create(&t).Error; err != nil {
				low := strings.ToLower(err.Error())
				if strings.Contains(low, "duplicate") || strings.Contains(low, "23505") || strings.Contains(low, "unique") {
					resp.Already++
					continue
				}
				resp.Skipped++
				continue
			}

			u.BalanceCents += amtCents
			if err := tx.Save(&u).Error; err != nil {
				resp.Skipped++
				continue
			}

			creditedTIDs = append(creditedTIDs, tid)
			resp.Credited++
		}
		return nil
	}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// hand the list back to the bot
	resp.CreditedTIDs = creditedTIDs

	// IMPORTANT: remove the goroutine DM here to avoid double-sending.
	// The bot will send the DMs using resp.CreditedTIDs.

	c.JSON(http.StatusOK, resp)
}

func toUserDTO(u *User) *UserDTO {
	phone := ""
	if u.Phone != nil {
		phone = *u.Phone
	}
	return &UserDTO{
		ID:                u.ID,
		CreatedAt:         u.CreatedAt,
		UpdatedAt:         u.UpdatedAt,
		TelegramID:        u.TelegramID,
		Username:          u.Username,
		FirstName:         u.FirstName,
		LastName:          u.LastName,
		Name:              u.Name,
		Email:             u.Email,
		Phone:             phone,
		HasPhone:          hasPhone(u),
		BalanceBirr:       toBirrString(u.BalanceCents),
		MainBalanceBirr:   toBirrString(u.MainBalanceCents),
		IsAdmin:           u.IsAdmin,    // ← add this line
		HasDeposit:        u.HasDeposit, // <-- add this line
		IsBot:             u.IsBot,      // <-- add this line
		InviterTelegramID: u.InviterTelegramID,
	}
}

func phoneVerifyBonusCents() int64 {
	v := strings.TrimSpace(os.Getenv("PHONE_VERIFY_BONUS_BIRR"))
	if v == "" {
		return 0 // was: 10 * 100
	}
	n, _ := strconv.Atoi(v)
	if n <= 0 {
		return 0
	}
	return int64(n) * 100
}

func grantPhoneVerifyBonusTX(tx *gorm.DB, u *User) error {
	// Defense-in-depth: never bonus bots, never more than once
	if u.IsBot || u.PhoneBonusAt != nil {
		return nil
	}
	amt := phoneVerifyBonusCents()
	if amt <= 0 {
		return nil
	}

	u.BalanceCents += amt
	now := time.Now()
	u.PhoneBonusAt = &now
	if err := tx.Save(u).Error; err != nil {
		return err
	}

	amtStr := toBirrString(amt)
	t := Transaction{
		TelegramID: u.TelegramID,
		Type:       "phone_verify_bonus",
		Method:     "+",
		Status:     "success",
		Note:       "Phone verification bonus",
		Amount:     amtStr,
		Bonus:      "0.00",
		Total:      amtStr,
		Account:    "balance", // change to "main" if you credit MainBalanceCents instead
		// Optional extra guard if you also add the unique index below:
		// Reference: ptrOrNil(fmt.Sprintf("phone_bonus:%d", u.TelegramID)),
	}
	return tx.Create(&t).Error
}
// PUT /users/:telegram_id/inviter
func updateUserInviter(c *gin.Context) {
	tid, ok := parseTelegramIDParam(c)
	if !ok {
		return
	}

	var req struct {
		InviterTelegramID int64 `json:"inviter_telegram_id" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Prevent self-invite
	if tid == req.InviterTelegramID {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cannot set self as inviter"})
		return
	}

	if err := db.Transaction(func(tx *gorm.DB) error {
		var user User
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("telegram_id = ?", tid).First(&user).Error; err != nil {
			return err
		}

		// Only set inviter if not already set (don't override existing inviter)
		if user.InviterTelegramID != nil {
			return fmt.Errorf("inviter already set")
		}

		// Verify that the inviter exists
		var inviter User
		if err := tx.Where("telegram_id = ?", req.InviterTelegramID).First(&inviter).Error; err != nil {
			return fmt.Errorf("inviter not found")
		}

		user.InviterTelegramID = &req.InviterTelegramID
		return tx.Save(&user).Error
	}); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
func toBirrString(cents int64) string {
	sign := ""
	if cents < 0 {
		sign = "-"
		cents = -cents
	}
	whole := cents / 100
	frac := cents % 100
	return sign + strconv.FormatInt(whole, 10) + "." + fmt2(frac, 2)
}

func fmt2(n int64, width int) string {
	s := strconv.FormatInt(n, 10)
	for len(s) < width {
		s = "0" + s
	}
	return s
}
func strptr(s string) *string {
	v := strings.TrimSpace(s)
	if v == "" {
		return nil
	}
	return &v
}
func hasPhone(u *User) bool { return u.Phone != nil && strings.TrimSpace(*u.Phone) != "" }

// ===== ENV helpers (NEW) =====
func getenvBool(key string, def bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	switch v {
	case "1", "true", "t", "yes", "y", "on":
		return true
	case "0", "false", "f", "no", "n", "off":
		return false
	}
	return def
}

func fromBirrToCents(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("amount required")
	}
	s = strings.ReplaceAll(s, ",", "")
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}
	return int64(math.Round(f * 100)), nil
}

func pickName(name, first, last string) string {
	name = strings.TrimSpace(name)
	if name != "" {
		return name
	}
	first = strings.TrimSpace(first)
	last = strings.TrimSpace(last)
	return strings.TrimSpace(strings.Join([]string{first, last}, " "))
}

// keep only digits
func onlyDigits(s string) string {
	re := regexp.MustCompile(`\D+`)
	return re.ReplaceAllString(s, "")
}

// last 9 digits (used for matching)
func last9Digits(s string) string {
	d := onlyDigits(s)
	if len(d) > 9 {
		return d[len(d)-9:]
	}
	return d
}
func getUserByPhone(c *gin.Context) {
	raw := strings.TrimSpace(c.Param("phone"))
	if raw == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "phone required"})
		return
	}

	l9 := last9Digits(raw)
	if len(l9) != 9 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid phone"})
		return
	}

	// Match by last-9 of digits-only phone (works for +251/09/9 forms)
	var u User
	err := db.
		Where("RIGHT(regexp_replace(phone, '\\D', '', 'g'), 9) = ?", l9).
		First(&u).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		c.JSON(http.StatusNotFound, gin.H{"error": "User not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, toUserDTO(&u))
}

func parseTelegramIDParam(c *gin.Context) (int64, bool) {
	tidStr := c.Param("telegram_id")
	tid, err := strconv.ParseInt(tidStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid telegram_id"})
		return 0, false
	}
	return tid, true
}

/* ==========================
   Referral helpers
========================== */

func referralRewardCents() int64 {
	v := strings.TrimSpace(os.Getenv("REFERRAL_REWARD_BIRR"))
	if v == "" {
		return 10 * 100 // default 10 birr
	}
	if n, err := strconv.Atoi(v); err == nil && n > 0 {
		return int64(n) * 100
	}
	return 10 * 100
}
func inviteeRewardCents() int64 { // NEW
	n := getenvInt("REFERRAL_INVITEE_BIRR", 0)
	if n < 0 {
		n = 0
	}
	return int64(n) * 100
}

// t.me link: https://t.me/<BOT_USERNAME>?start=ref_<tid>
func referralLinkFor(inviterTID int64) (string, error) {
	bot := strings.TrimSpace(os.Getenv("BOT_USERNAME"))
	if bot == "" {
		return "", errors.New("BOT_USERNAME not set")
	}
	return fmt.Sprintf("https://t.me/%s?start=ref_%d", bot, inviterTID), nil
}

// SAFER parser (NEW: replaces older version)
func parseStartParamRefTID(startParam string) (int64, bool) {
	s := strings.ToLower(strings.TrimSpace(startParam))
	if !strings.HasPrefix(s, "ref_") {
		return 0, false
	}
	id := strings.TrimPrefix(s, "ref_")
	tid, err := strconv.ParseInt(id, 10, 64)
	if err != nil || tid <= 0 {
		return 0, false
	}
	return tid, true
}

// Legacy: credit once per invitee (kept for reference; unused in new flexible flow)
// Returns the credited cents for inviter/invitee and whether a credit actually occurred.
// It DOES NOT send notifications. Caller should notify AFTER the transaction commits.
func creditReferralFlexibleTXNoNotify(tx *gorm.DB, inviterTID, inviteeTID int64, allowDuplicate bool) (int64, int64, bool, error) {
	if inviterTID == 0 || inviteeTID == 0 || inviterTID == inviteeTID {
		return 0, 0, false, nil
	}

	// Lock inviter
	var inviter User
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("telegram_id = ?", inviterTID).First(&inviter).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, 0, false, nil // inviter unknown → skip
		}
		return 0, 0, false, err
	}

	// Ensure invitee
	var invitee User
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("telegram_id = ?", inviteeTID).First(&invitee).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			invitee = User{TelegramID: inviteeTID}
			if e := tx.Create(&invitee).Error; e != nil {
				return 0, 0, false, e
			}
		} else {
			return 0, 0, false, err
		}
	}

	inviterAmt := referralRewardCents()
	inviteeAmt := inviteeRewardCents()

	// Enforce uniqueness unless allowDuplicate
	if !allowDuplicate {
		r := Referral{
			InviterTID:  inviter.TelegramID,
			InviteeTID:  invitee.TelegramID,
			AmountCents: inviterAmt,
		}
		if err := tx.Create(&r).Error; err != nil {
			low := strings.ToLower(err.Error())
			if strings.Contains(low, "duplicate") || strings.Contains(low, "unique") || strings.Contains(low, "23505") {
				return 0, 0, false, nil // already credited → no-op
			}
			return 0, 0, false, err
		}
	}

	// Credit inviter
	inviter.BalanceCents += inviterAmt
	if err := tx.Save(&inviter).Error; err != nil {
		return 0, 0, false, err
	}
	amtStr := toBirrString(inviterAmt)
	if err := tx.Create(&Transaction{
		TelegramID: inviter.TelegramID,
		Type:       "referral_bonus",
		Method:     "+",
		Status:     "success",
		Note:       fmt.Sprintf("Referral bonus for inviting tg:%d", invitee.TelegramID),
		Amount:     amtStr,
		Bonus:      "0.00",
		Total:      amtStr,
	}).Error; err != nil {
		return 0, 0, false, err
	}

	// Optional invitee bonus
	if inviteeAmt > 0 {
		invitee.BalanceCents += inviteeAmt
		if err := tx.Save(&invitee).Error; err != nil {
			return 0, 0, false, err
		}
		joinStr := toBirrString(inviteeAmt)
		if err := tx.Create(&Transaction{
			TelegramID: invitee.TelegramID,
			Type:       "referral_join_bonus",
			Method:     "+",
			Status:     "success",
			Note:       fmt.Sprintf("Join bonus via inviter tg:%d", inviter.TelegramID),
			Amount:     joinStr,
			Bonus:      "0.00",
			Total:      joinStr,
		}).Error; err != nil {
			return 0, 0, false, err
		}
	}

	return inviterAmt, inviteeAmt, true, nil
}

// Notifications (NEW - replace logs with real Telegram senders)
// --- Telegram referral notifications (server-side DM for both parties) ---
// ---- Admin broadcast (skip bots) ----
type postBroadcastReq struct {
	Message string `json:"message"` // HTML allowed
}
type postBroadcastResp struct {
	Status  string `json:"status"`
	Sent    int    `json:"sent"`
	Failed  int    `json:"failed"`
	Skipped int    `json:"skipped"` // bots & invalid chats
}

// quick admin checker
func isUserAdminByTID(tid int64) bool {
	var u User
	if err := db.Select("is_admin").Where("telegram_id = ?", tid).First(&u).Error; err != nil {
		return false
	}
	return u.IsAdmin
}

// core broadcaster: skips bots
func broadcastAnnouncementHTML(html string) (sent, failed, skipped int) {
	if strings.TrimSpace(html) == "" {
		return 0, 0, 0
	}
	botToken := strings.TrimSpace(os.Getenv("BOT_TOKEN"))
	if botToken == "" {
		log.Println("broadcastAnnouncementHTML: BOT_TOKEN missing")
		return 0, 0, 0
	}

	type rec struct {
		TelegramID int64
		IsBot      bool
	}
	var recips []rec
	if err := db.Model(&User{}).
		Select("telegram_id, is_bot").
		Where("telegram_id IS NOT NULL AND telegram_id <> 0").
		Find(&recips).Error; err != nil {
		log.Printf("broadcast query error: %v", err)
		return 0, 0, 0
	}

	ctx := context.Background()
	for _, r := range recips {
		if r.IsBot {
			skipped++
			continue
		}
		if err := sendTelegramDM(ctx, botToken, r.TelegramID, html); err != nil {
			// Treat 400/403 as failed but keep going (sendTelegramDM already logs / squelches some)
			failed++
			continue
		}
		sent++
	}
	return
}

// sendTelegramDM posts a plain text/HTML message to a Telegram user.
// It safely no-ops on common delivery errors (e.g., 403 when the user hasn't started the bot).
func sendTelegramDM(ctx context.Context, botToken string, chatID int64, text string) error {
	if botToken == "" || chatID == 0 || strings.TrimSpace(text) == "" {
		return nil
	}
	apiBase := strings.TrimSpace(os.Getenv("TELEGRAM_API_BASE"))
	if apiBase == "" {
		apiBase = "https://api.telegram.org"
	}
	urlStr := fmt.Sprintf("%s/bot%s/sendMessage", apiBase, botToken)

	form := url.Values{}
	form.Set("chat_id", strconv.FormatInt(chatID, 10))
	form.Set("text", text)
	form.Set("parse_mode", "HTML")
	form.Set("disable_web_page_preview", "true")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, urlStr, strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusBadRequest {
		slurp, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		log.Printf("notify DM skipped chat_id=%d http=%d body=%s", chatID, resp.StatusCode, string(slurp))
		return nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		slurp, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("telegram sendMessage %d: %s", resp.StatusCode, string(slurp))
	}
	return nil
}

// notifyReferral sends DMs to both inviter and invitee (if amounts > 0).
// Controlled by REFERRAL_NOTIFY (default true). Requires BOT_TOKEN in env.
func notifyReferral(inviterTID, inviteeTID int64, inviterCents, inviteeCents int64) {
	if !getenvBool("REFERRAL_NOTIFY", true) {
		return
	}
	botToken := strings.TrimSpace(os.Getenv("BOT_TOKEN"))
	if botToken == "" {
		log.Printf("notifyReferral: BOT_TOKEN missing; skipping DMs")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()

	// Inviter message
	if inviterTID != 0 && inviterCents > 0 {
		msg := fmt.Sprintf("🎉 <b>You got money!</b>\nA friend joined with your link.\nCredited: <b>%s ብር</b>.", toBirrString(inviterCents))
		if err := sendTelegramDM(ctx, botToken, inviterTID, msg); err != nil {
			log.Printf("notifyReferral inviter send error: %v", err)
		}
	}

	// Invitee message
	if inviteeTID != 0 && inviteeCents > 0 {
		msg := fmt.Sprintf("👋 <b>Welcome!</b>\nYou joined via a referral.\nBonus received: <b>%s ብር</b>.", toBirrString(inviteeCents))
		if err := sendTelegramDM(ctx, botToken, inviteeTID, msg); err != nil {
			log.Printf("notifyReferral invitee send error: %v", err)
		}
	}
}

// Flexible crediting (NEW)
func creditReferralFlexibleTX(tx *gorm.DB, inviterTID, inviteeTID int64, allowDuplicate bool) error {
	if inviterTID == 0 || inviteeTID == 0 || inviterTID == inviteeTID {
		return nil
	}

	// Lock inviter row
	var inviter User
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("telegram_id = ?", inviterTID).First(&inviter).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// Option: silently skip if inviter unknown
			return nil
		}
		return err
	}

	// Ensure invitee exists (lock it)
	var invitee User
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("telegram_id = ?", inviteeTID).First(&invitee).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			invitee = User{TelegramID: inviteeTID}
			if e := tx.Create(&invitee).Error; e != nil {
				return e
			}
		} else {
			return err
		}
	}

	inviterAmt := referralRewardCents()
	inviteeAmt := inviteeRewardCents()

	// Normal mode: enforce uniqueness using Referral table
	if !allowDuplicate {
		r := Referral{
			InviterTID:  inviterTID,
			InviteeTID:  inviteeTID,
			AmountCents: inviterAmt,
		}
		if err := tx.Create(&r).Error; err != nil {
			low := strings.ToLower(err.Error())
			if strings.Contains(low, "duplicate") || strings.Contains(low, "unique") || strings.Contains(low, "23505") {
				// already credited for this invitee → idempotent
				return nil
			}
			return err
		}
	}

	// Credit inviter
	inviter.BalanceCents += inviterAmt
	if err := tx.Save(&inviter).Error; err != nil {
		return err
	}
	amtStr := toBirrString(inviterAmt)
	if err := tx.Create(&Transaction{
		TelegramID: inviter.TelegramID,
		Type:       "referral_bonus",
		Method:     "+",
		Status:     "success",
		Note:       fmt.Sprintf("Referral bonus for inviting tg:%d", inviteeTID),
		Amount:     amtStr,
		Bonus:      "0.00",
		Total:      amtStr,
	}).Error; err != nil {
		return err
	}

	// Optional: credit invitee
	if inviteeAmt > 0 {
		invitee.BalanceCents += inviteeAmt
		if err := tx.Save(&invitee).Error; err != nil {
			return err
		}
		joinStr := toBirrString(inviteeAmt)
		if err := tx.Create(&Transaction{
			TelegramID: invitee.TelegramID,
			Type:       "referral_join_bonus",
			Method:     "+",
			Status:     "success",
			Note:       fmt.Sprintf("Join bonus via inviter tg:%d", inviterTID),
			Amount:     joinStr,
			Bonus:      "0.00",
			Total:      joinStr,
		}).Error; err != nil {
			return err
		}
	}

	notifyReferral(inviter.TelegramID, invitee.TelegramID, inviterAmt, inviteeAmt)
	return nil
}

/* ==========================
   DB init / migrate
========================== */

func mustLoadEnv() {
	if err := godotenv.Load(); err != nil {
		log.Println("no .env file found; relying on environment")
	}
}

// ensureUniqueTxReference cleans duplicate/blank references, optionally backfills
// a stable reference for the *first* phone bonus per user, then creates the index.
func ensureUniqueTxReference() error {
	// 1) Normalize blanks to NULL ('' is not NULL and can violate unique index)
	if err := db.Exec(`
		UPDATE transactions
		SET reference = NULL
		WHERE reference IS NOT NULL AND btrim(reference) = ''
	`).Error; err != nil {
		return err
	}

	// 2) De-dup any identical non-NULL references: keep the lowest id, NULL the rest
	if err := db.Exec(`
		WITH d AS (
		  SELECT reference, MIN(id) AS keep_id
		  FROM transactions
		  WHERE reference IS NOT NULL
		  GROUP BY reference
		  HAVING COUNT(*) > 1
		)
		UPDATE transactions t
		SET reference = NULL
		FROM d
		WHERE t.reference = d.reference AND t.id <> d.keep_id
	`).Error; err != nil {
		return err
	}

	// 3) Optional: ensure a deterministic reference for the *first* phone bonus per user
	//    (safe because we haven't created the unique index yet)
	if err := db.Exec(`
		WITH first_bonus AS (
		  SELECT MIN(id) AS keep_id
		  FROM transactions
		  WHERE type = 'phone_verify_bonus'
		  GROUP BY telegram_id
		)
		UPDATE transactions t
		SET reference = CONCAT('phone_bonus:', t.telegram_id)
		FROM first_bonus fb
		WHERE t.id = fb.keep_id AND t.reference IS NULL
	`).Error; err != nil {
		return err
	}

	// 4) Finally, create the partial unique index (non-NULL only)
	//    (Use non-concurrent here; if your table is huge, run CONCURRENTLY via an external migration)
	if err := db.Exec(`
		CREATE UNIQUE INDEX IF NOT EXISTS uniq_tx_reference
		ON transactions(reference) WHERE reference IS NOT NULL
	`).Error; err != nil {
		return err
	}
	return nil
}

func initDB() {
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		dsn = "host=localhost user=postgres password=postgres dbname=users port=5432 sslmode=disable"
	}
	var err error
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger:      logger.Default.LogMode(logger.Warn),
		PrepareStmt: true, // Cache prepared statements for better performance
	})
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}

	// Configure connection pool for better concurrency
	sqlDB, err := db.DB()
	if err != nil {
		log.Fatal("Failed to get underlying sql.DB:", err)
	}
	sqlDB.SetMaxOpenConns(50)                 // Increase max open connections
	sqlDB.SetMaxIdleConns(25)                 // Keep connections ready
	sqlDB.SetConnMaxLifetime(5 * time.Minute) // Recycle connections periodically

	// Start bots (non-blocking)
	go startBots(context.Background())

	if err := db.AutoMigrate(&User{}, &Transaction{}, &Room{}, &RoomPlayer{}, &Referral{}, &ReferralPayout{}); err != nil {
		log.Fatal("AutoMigrate failed:", err)
	}

	// NEW: clean duplicates & create unique index for transactions.reference
	if err := ensureUniqueTxReference(); err != nil {
		log.Fatal("ensureUniqueTxReference failed:", err)
	}

	// Create additional indexes for slow queries
	if err := createPerformanceIndexes(); err != nil {
		log.Printf("Warning: createPerformanceIndexes failed: %v", err)
	}
}

// createPerformanceIndexes adds indexes to optimize the slow queries
func createPerformanceIndexes() error {
	indexes := []string{
		// Index for room_players lookups by room_id alone
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_room_players_room_id ON room_players(room_id)`,
		// Index for room_players lookups by telegram_id alone  
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_room_players_telegram_id ON room_players(telegram_id)`,
		// Compound index for the most common query pattern
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_room_players_room_tid ON room_players(room_id, telegram_id)`,
		// Index for users by telegram_id (if not already covered by unique constraint)
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_users_telegram_id ON users(telegram_id)`,
	}

	for _, idx := range indexes {
		// Use Exec with error handling for CONCURRENTLY (can't run in transaction)
		if err := db.Exec(idx).Error; err != nil {
			// Ignore "already exists" errors
			errStr := strings.ToLower(err.Error())
			if !strings.Contains(errStr, "already exists") && !strings.Contains(errStr, "duplicate") {
				log.Printf("Index creation warning: %v", err)
			}
		}
	}
	return nil
}

/* ==========================
   Requests payloads
========================== */

type CreateUserReq struct {
	TelegramID int64  `json:"telegram_id" binding:"required"`
	Username   string `json:"username"`
	FirstName  string `json:"first_name"`
	LastName   string `json:"last_name"`
	Name       string `json:"name"`
	Email      string `json:"email"`
}

type UpdateUserReq struct {
	Username  *string `json:"username"`
	FirstName *string `json:"first_name"`
	LastName  *string `json:"last_name"`
	Name      *string `json:"name"`
	Email     *string `json:"email"`
}

type WalletChangeReq struct {
	Amount string `json:"amount" binding:"required"`
	Note   string `json:"note"`
}

type TelegramSyncReq struct {
	TelegramID int64  `json:"telegram_id" binding:"required"`
	Username   string `json:"username"`
	FirstName  string `json:"first_name"`
	LastName   string `json:"last_name"`
	Name       string `json:"name"`
	Email      string `json:"email"`
	Phone      string `json:"phone"`       // optional
	StartParam string `json:"start_param"` // e.g. "ref_397753549"
}

type TransferReq struct {
	FromTelegramID int64  `json:"from_telegram_id" binding:"required"`
	ToUsername     string `json:"to_username"` // optional
	ToPhone        string `json:"to_phone"`    // optional, e.g. +251926262782 or 0926262782
	Amount         string `json:"amount" binding:"required"`
	Target         string `json:"target" binding:"omitempty,oneof=main balance"` // default "balance"
}

type UpdateTxnStatusReq struct {
	Status string `json:"status" binding:"required"`
}

type createTxnReq struct {
	Reference    string `json:"reference"`
	TelegramID   int64  `json:"telegram_id"`
	Type         string `json:"type"`   // deposit | withdraw | referral_bonus
	Amount       string `json:"amount"` // "500.00"
	Bonus        string `json:"bonus"`
	Total        string `json:"total"`
	Method       string `json:"method"`
	Account      string `json:"account"`
	Status       string `json:"status"` // success | pending | failed
	TxID         string `json:"txid"`
	Note         string `json:"note"`
	SenderName   string `json:"sender_name"`
	ReceiverName string `json:"receiver_name"`
	ReceiptURL   string `json:"receipt_url"`
	PaymentDate  string `json:"payment_date"`
}

/* ==========================
   Users / Auth handlers
========================== */
// ===== referral-notice debouncer (in-memory) =====

func findUserByUsernameOrPhone(tx *gorm.DB, uname, phone string) (*User, error) {
	uname = strings.TrimSpace(strings.TrimPrefix(uname, "@"))
	phone = strings.TrimSpace(phone)

	var u User
	var err error

	switch {
	case uname != "" && phone == "":
		err = tx.Where("LOWER(username) = ?", strings.ToLower(uname)).First(&u).Error
	case phone != "" && uname == "":
		p, e := normalizePhoneE164(phone)
		if e != nil {
			return nil, errors.New("invalid phone")
		}
		err = tx.Where("phone = ?", p).First(&u).Error
	case uname != "" && phone != "":
		// prefer phone if both sent
		p, e := normalizePhoneE164(phone)
		if e != nil {
			return nil, errors.New("invalid phone")
		}
		err = tx.Where("phone = ?", p).First(&u).Error
	default:
		return nil, errors.New("to_username or to_phone required")
	}

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.New("recipient not found")
		}
		return nil, err
	}
	return &u, nil
}

func getMe(c *gin.Context) {
	raw, _ := c.Get(string(ctxTID))
	tid, _ := raw.(int64)

	var u User
	if err := db.Where("telegram_id = ?", tid).First(&u).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "User not found"})
		return
	}
	c.JSON(http.StatusOK, toUserDTO(&u))
}

func getUsers(c *gin.Context) {
	var users []User
	db.Order("id desc").Find(&users)
	out := make([]*UserDTO, 0, len(users))
	for i := range users {
		out = append(out, toUserDTO(&users[i]))
	}
	c.JSON(http.StatusOK, out)
}

func createUser(c *gin.Context) {
	var req CreateUserReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	req.Username = strings.TrimPrefix(req.Username, "@")
	u := &User{
		TelegramID:       req.TelegramID,
		Username:         req.Username,
		FirstName:        req.FirstName,
		LastName:         req.LastName,
		Name:             pickName(req.Name, req.FirstName, req.LastName),
		Email:            req.Email,
		BalanceCents:     0,
		MainBalanceCents: 0,
	}
	if err := db.Create(u).Error; err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, toUserDTO(u))
}

var (
	adminEveryDeposit    = getenvBool("REFERRAL_ADMIN_EVERY_DEPOSIT", true)
	nonAdminFirstDeposit = getenvBool("REFERRAL_NONADMIN_FIRST_DEPOSIT", true)
)

func referralDepositRewardCents(depositAmountCents int64) int64 {
	flatBirr := getenvInt("REFERRAL_DEPOSIT_FLAT_BIRR", 0) // default: disabled
	ratePct := getenvInt("REFERRAL_DEPOSIT_RATE_PCT", 50)  // default: 50%

	if flatBirr > 0 {
		return int64(flatBirr) * 100
	}
	if ratePct > 0 {
		return (depositAmountCents * int64(ratePct)) / 100
	}
	return 0
}

// linkReferralOnJoinTX creates the Referral row if missing and notifies inviter once.
func linkReferralOnJoinTX(tx *gorm.DB, inviterTID, inviteeTID int64) error {
	if inviterTID == 0 || inviteeTID == 0 || inviterTID == inviteeTID {
		return nil
	}

	// Ensure both users exist (and lock inviter row minimally)
	var inviter User
	if err := tx.Select("telegram_id, is_bot").Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("telegram_id = ?", inviterTID).First(&inviter).Error; err != nil {
		return err
	}
	var invitee User
	if err := tx.Where("telegram_id = ?", inviteeTID).First(&invitee).Error; err != nil {
		// if brand-new user was just created above this call, you’ll already have them
		return err
	}

	// Idempotent: create referral only if missing
	var existing Referral
	if err := tx.Where("invitee_tid = ?", inviteeTID).First(&existing).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			if err := tx.Create(&Referral{
				InviterTID:  inviterTID,
				InviteeTID:  inviteeTID,
				AmountCents: 0,
			}).Error; err != nil {
				low := strings.ToLower(err.Error())
				if strings.Contains(low, "duplicate") || strings.Contains(low, "23505") {
					// race/no-op
				} else {
					return err
				}
			}
		} else {
			return err
		}
	}

	// Notify inviter ONCE per inviter:invitee pair (use your debouncer)
	if shouldNotifyExistingRefOnce(inviterTID, inviteeTID) {
		go func() {
			if !getenvBool("REFERRAL_NOTIFY", true) {
				return
			}
			botToken := strings.TrimSpace(os.Getenv("BOT_TOKEN"))
			if botToken == "" {
				return
			}
			msg := "👋 Someone joined with your invite link.\n" +
				"💡 When they deposit, you’ll earn <b>50%</b> of their deposit balance."
			_ = sendTelegramDM(context.Background(), botToken, inviterTID, msg)
		}()
	}

	return nil
}

// awardReferralOnDepositTX is idempotent per deposit_tx_id via unique index.
// - If inviter is admin: pays on every successful deposit
// - If inviter not admin: pays only once (first successful deposit) per invitee
func awardReferralOnDepositTX(tx *gorm.DB, depositTx Transaction) error {
	if strings.ToLower(depositTx.Type) != "deposit" || strings.ToLower(depositTx.Status) != "success" {
		return nil
	}
	inviteeTID := depositTx.TelegramID

	// Find inviter for this invitee
	var rel Referral
	if err := tx.Where("invitee_tid = ?", inviteeTID).First(&rel).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil // no inviter → nothing to do
		}
		return err
	}
	inviterTID := rel.InviterTID
	if inviterTID == 0 || inviterTID == inviteeTID {
		return nil
	}

	// Load inviter to check admin
	var inviter User
	if err := tx.Select("telegram_id, is_admin, balance_cents").
		Where("telegram_id = ?", inviterTID).
		First(&inviter).Error; err != nil {
		return err
	}

	// Determine reward
	depositAmountCents, err := fromBirrToCents(depositTx.Amount) // depositTx.Amount like "500.00"
	if err != nil || depositAmountCents <= 0 {
		return nil
	}
	reward := referralDepositRewardCents(depositAmountCents)
	if reward <= 0 {
		return nil
	}

	// Non-admin path: pay only once (first successful deposit)
	if !inviter.IsAdmin {
		if !nonAdminFirstDeposit {
			return nil
		}
		// Has any prior payout been made for this invitee?
		var cnt int64
		if err := tx.Model(&ReferralPayout{}).
			Where("invitee_tid = ?", inviteeTID).
			Count(&cnt).Error; err != nil {
			return err
		}
		if cnt > 0 {
			return nil // already paid once
		}
	} else {
		// Admin path: guard with toggle
		if !adminEveryDeposit {
			return nil
		}
	}

	// Idempotency per deposit: if a payout exists for this deposit_tx_id → no-op
	var existing ReferralPayout
	if err := tx.Where("deposit_tx_id = ?", depositTx.ID).First(&existing).Error; err == nil {
		return nil
	}

	// Credit inviter balance
	inviter.BalanceCents += reward
	if err := tx.Save(&inviter).Error; err != nil {
		return err
	}

	// Write payout record
	if err := tx.Create(&ReferralPayout{
		InviterTID:  inviterTID,
		InviteeTID:  inviteeTID,
		DepositTxID: depositTx.ID,
		AmountCents: reward,
	}).Error; err != nil {
		// unique(deposit_tx_id) handles races
		low := strings.ToLower(err.Error())
		if strings.Contains(low, "duplicate") || strings.Contains(low, "23505") {
			return nil
		}
		return err
	}
	go func() {
		botToken := strings.TrimSpace(os.Getenv("BOT_TOKEN"))
		if botToken == "" {
			return
		}
		msg := fmt.Sprintf("🪙 Your customer deposited money. You earned %s ብር. Added to your Play balance.",
			toBirrString(reward))
		_ = sendTelegramDM(context.Background(), botToken, inviterTID, msg)
	}()

	// Log a Transaction row for inviter (auditable)
	amtStr := toBirrString(reward)
	if err := tx.Create(&Transaction{
		TelegramID: inviterTID,
		Type:       "referral_deposit_bonus",
		Method:     "+",
		Status:     "success",
		Note:       fmt.Sprintf("Bonus from invitee tg:%d deposit tx:%d", inviteeTID, depositTx.ID),
		Amount:     amtStr,
		Bonus:      "0.00",
		Total:      amtStr,
		Account:    "balance",
	}).Error; err != nil {
		return err
	}

	return nil
}

func getUser(c *gin.Context) {
	var user User
	if err := db.First(&user, c.Param("id")).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "User not found"})
		return
	}
	c.JSON(http.StatusOK, toUserDTO(&user))
}

func updateUser(c *gin.Context) {
	var user User
	id := c.Param("id")
	if err := db.First(&user, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "User not found"})
		return
	}
	var req UpdateUserReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.Username != nil {
		user.Username = strings.TrimPrefix(*req.Username, "@")
	}
	if req.FirstName != nil {
		user.FirstName = *req.FirstName
	}
	if req.LastName != nil {
		user.LastName = *req.LastName
	}
	if req.Name != nil {
		user.Name = *req.Name
	} else if user.Name == "" {
		user.Name = pickName("", user.FirstName, user.LastName)
	}
	if req.Email != nil {
		user.Email = *req.Email
	}
	if err := db.Save(&user).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, toUserDTO(&user))
}

func deleteUser(c *gin.Context) {
	if err := db.Delete(&User{}, c.Param("id")).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusNoContent)
}

func getUserByTelegramID(c *gin.Context) {
	tidStr := c.Param("telegram_id")
	tid, err := strconv.ParseInt(tidStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid telegram_id"})
		return
	}
	var user User
	if err := db.Where("telegram_id = ?", tid).First(&user).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "User not found"})
		return
	}
	c.JSON(http.StatusOK, toUserDTO(&user))
}

func getUserByUsername(c *gin.Context) {
	uname := strings.TrimSpace(c.Param("username"))
	uname = strings.TrimPrefix(uname, "@")
	if uname == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "username required"})
		return
	}
	var u User
	if err := db.Where("LOWER(username) = ?", strings.ToLower(uname)).First(&u).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "User not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, toUserDTO(&u))
}

/* ==========================
   Telegram sync / token
========================== */

type SharePhoneReq struct {
	TelegramID int64  `json:"telegram_id" binding:"required"`
	Phone      string `json:"phone" binding:"required"`
	StartParam string `json:"start_param"` // optional referral start param
}

func normalizePhoneE164(raw string) (string, error) {
	s := strings.TrimSpace(raw)
	re := regexp.MustCompile(`[^0-9\+]+`)
	s = re.ReplaceAllString(s, "")
	if s == "" {
		return "", errors.New("empty phone")
	}
	if !strings.HasPrefix(s, "+") {
		if strings.HasPrefix(s, "0") {
			s = "+251" + strings.TrimLeft(s, "0")
		} else {
			s = "+" + s
		}
	}
	if len(s) < 8 || len(s) > 20 {
		return "", errors.New("invalid phone length")
	}
	return s, nil
}

var (
	recentRefCredit = struct {
		m map[string]time.Time
		sync.Mutex
	}{m: make(map[string]time.Time)}
	refCreditTTL = 10 * time.Minute
)
var (
	recentRefNotice = struct {
		m map[string]time.Time
		sync.Mutex
	}{m: make(map[string]time.Time)}
	refNoticeTTL = 24 * time.Hour
)

func shouldNotifyExistingRefOnce(inviterTID, inviteeTID int64) bool {
	key := fmt.Sprintf("%d:%d", inviterTID, inviteeTID)
	now := time.Now()

	recentRefNotice.Lock()
	defer recentRefNotice.Unlock()

	for k, until := range recentRefNotice.m {
		if now.After(until) {
			delete(recentRefNotice.m, k)
		}
	}
	if until, ok := recentRefNotice.m[key]; ok && now.Before(until) {
		return false
	}
	recentRefNotice.m[key] = now.Add(refNoticeTTL)
	return true
}

func shouldCreditNowDebounced(inviter, invitee int64) bool {
	key := fmt.Sprintf("%d:%d", inviter, invitee)
	now := time.Now()

	recentRefCredit.Lock()
	defer recentRefCredit.Unlock()

	// cleanup expired
	for k, until := range recentRefCredit.m {
		if now.After(until) {
			delete(recentRefCredit.m, k)
		}
	}

	if until, ok := recentRefCredit.m[key]; ok && now.Before(until) {
		return false
	}
	recentRefCredit.m[key] = now.Add(refCreditTTL)
	return true
}
func upsertFromTelegram(c *gin.Context) {
    var req TelegramSyncReq
    if err := c.ShouldBindJSON(&req); err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
        return
    }

    // Parse inviter from start param if provided
    var inviterTID int64
    if req.StartParam != "" {
        if tid, ok := parseStartParamRefTID(req.StartParam); ok {
            inviterTID = tid
            log.Printf("[REFERRAL] Parsed inviter_tid=%d from start_param=%q", inviterTID, req.StartParam)
        }
    }

    var resp *UserDTO

    if err := db.Transaction(func(tx *gorm.DB) error {
        var user User
        err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
            Where("telegram_id = ?", req.TelegramID).
            First(&user).Error

        if errors.Is(err, gorm.ErrRecordNotFound) {
            // NEW USER - create with inviter if provided
            user = User{
                TelegramID: req.TelegramID,
                Username:   req.Username,
                FirstName:  req.FirstName,
                LastName:   req.LastName,
                Name:       pickName(req.Name, req.FirstName, req.LastName),
                Email:      req.Email,
            }
            
            // Set inviter for new users only if provided and valid
            if inviterTID > 0 && inviterTID != req.TelegramID {
                // Verify inviter exists
                var inviter User
                if err := tx.Where("telegram_id = ?", inviterTID).First(&inviter).Error; err == nil {
                    user.InviterTelegramID = &inviterTID
                    log.Printf("[REFERRAL] Set inviter_tid=%d for new user %d", inviterTID, req.TelegramID)
                } else {
                    log.Printf("[REFERRAL] Inviter %d not found for new user %d", inviterTID, req.TelegramID)
                }
            }
            
            if err := tx.Create(&user).Error; err != nil {
                return err
            }
        } else if err != nil {
            return err
        } else {
            // EXISTING USER - update basic info but don't change inviter
            // Only update inviter if it's not already set
            if user.InviterTelegramID == nil && inviterTID > 0 && inviterTID != req.TelegramID {
                // Verify inviter exists
                var inviter User
                if err := tx.Where("telegram_id = ?", inviterTID).First(&inviter).Error; err == nil {
                    user.InviterTelegramID = &inviterTID
                    log.Printf("[REFERRAL] Set inviter_tid=%d for existing user %d", inviterTID, req.TelegramID)
                } else {
                    log.Printf("[REFERRAL] Inviter %d not found for existing user %d", inviterTID, req.TelegramID)
                }
            }
            
            // Update basic profile fields
            if req.Username != "" {
                user.Username = req.Username
            }
            if req.FirstName != "" {
                user.FirstName = req.FirstName
            }
            if req.LastName != "" {
                user.LastName = req.LastName
            }
            if req.Name != "" {
                user.Name = req.Name
            } else if strings.TrimSpace(user.Name) == "" {
                user.Name = pickName("", user.FirstName, user.LastName)
            }
            if req.Email != "" {
                user.Email = req.Email
            }
            
            if err := tx.Save(&user).Error; err != nil {
                return err
            }
        }

        resp = toUserDTO(&user)
        return nil
    }); err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
        return
    }

    c.JSON(http.StatusOK, resp)
}
type mintTokenReq struct {
	TelegramID int64 `json:"telegram_id" binding:"required"`
}
type mintTokenResp struct {
	Token string `json:"token"`
}

func generateJWT(telegramID int64) (string, error) {
	secret := strings.TrimSpace(os.Getenv("JWT_SECRET"))
	if secret == "" {
		return "", errors.New("JWT_SECRET not set")
	}
	claims := jwt.MapClaims{
		"sub": fmt.Sprintf("tg:%d", telegramID),
		"tid": telegramID,
		"iat": time.Now().Unix(),
		"exp": time.Now().Add(24 * time.Hour).Unix(),
		"typ": "phone_link",
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(secret))
}

func mintToken(c *gin.Context) {
	var req mintTokenReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	var u User
	if err := db.Where("telegram_id = ?", req.TelegramID).First(&u).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "user not found"})
		return
	}
	if !hasPhone(&u) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "phone not linked"})
		return
	}
	tok, err := generateJWT(req.TelegramID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, mintTokenResp{Token: tok})
}

// notify when an existing user tries to use a referral link (no credit)
// Notify when an existing user tries to use a referral link (no credit).
// Notify when an existing user tries to use a referral link (no credit case)
func notifyReferralExistingAttempt(inviterTID, inviteeTID int64) {
	if !getenvBool("REFERRAL_NOTIFY", true) {
		return
	}
	botToken := strings.TrimSpace(os.Getenv("BOT_TOKEN"))
	if botToken == "" {
		log.Printf("notifyReferralExistingAttempt: BOT_TOKEN missing; skipping DMs")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()

	// Invitee
	if inviteeTID != 0 {
		msg := "ℹ️ You already joined. Referral links only work for brand-new users."
		if err := sendTelegramDM(ctx, botToken, inviteeTID, msg); err != nil {
			log.Printf("notifyReferralExistingAttempt invitee send error: %v", err)
		}
	}
	// // Optional: Inviter heads-up
	// if inviterTID != 0 {
	// 	msg := "👀 Someone opened your link, but they were already registered — no credit granted."
	// 	if err := sendTelegramDM(ctx, botToken, inviterTID, msg); err != nil {
	// 		log.Printf("notifyReferralExistingAttempt inviter send error: %v", err)
	// 	}
	// }
}

type phoneBonusReq struct {
	TelegramID int64 `json:"telegram_id" binding:"required"`
}

func phoneVerifyBonus(c *gin.Context) {
	var req phoneBonusReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	ref := fmt.Sprintf("phone_bonus:%d", req.TelegramID)
	amtCents := int64(10 * 100)

	if err := db.Transaction(func(tx *gorm.DB) error {
		// unique transaction first
		t := Transaction{
			Reference:  &ref,
			TelegramID: req.TelegramID,
			Type:       "phone_verify_bonus",
			Method:     "+",
			Status:     "success",
			Amount:     "10.00", Bonus: "0.00", Total: "10.00",
			Account: "",
		}
		if err := tx.Create(&t).Error; err != nil {
			low := strings.ToLower(err.Error())
			if strings.Contains(low, "duplicate") || strings.Contains(low, "23505") || strings.Contains(low, "unique") {
				return nil // already awarded → noop
			}
			return err
		}

		// credit once
		var u User
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("telegram_id = ?", req.TelegramID).First(&u).Error; err != nil {
			return err
		}
		u.BalanceCents += amtCents
		return tx.Save(&u).Error
	}); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"status": "ok"})
}
func sharePhoneAndIssueToken(c *gin.Context) {
	var req SharePhoneReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	phone, err := normalizePhoneE164(req.Phone)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	var token string
	if err := db.Transaction(func(tx *gorm.DB) error {
		var u User
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("telegram_id = ?", req.TelegramID).
			First(&u).Error

		switch {
		// ===== NEW USER =====
		case errors.Is(err, gorm.ErrRecordNotFound):
			u = User{
				TelegramID: req.TelegramID,
				Phone:      strptr(phone),
			}
			now := time.Now()
			u.PhoneVerifiedAt = &now
			if e := tx.Create(&u).Error; e != nil {
				return e
			}

			// New user never had a phone → grant once
			if e := grantPhoneVerifyBonusTX(tx, &u); e != nil {
				return e
			}

		// ===== ERROR =====
		case err != nil:
			return err

		// ===== EXISTING USER =====
		default:
			// Did they already have a phone BEFORE this request?
			hadPhoneBefore := hasPhone(&u) || u.PhoneVerifiedAt != nil

			// Ensure phone is unique
			var cnt int64
			if e := tx.Model(&User{}).
				Where("phone = ? AND id <> ?", phone, u.ID).
				Count(&cnt).Error; e != nil {
				return e
			}
			if cnt > 0 {
				return fmt.Errorf("duplicate key value violates unique constraint \"idx_users_phone\"")
			}

			// Apply/replace phone and mark verified
			u.Phone = strptr(phone)
			now := time.Now()
			u.PhoneVerifiedAt = &now
			if e := tx.Save(&u).Error; e != nil {
				return e
			}

			// Only grant bonus if they DID NOT have a phone before
			if !hadPhoneBefore {
				if e := grantPhoneVerifyBonusTX(tx, &u); e != nil {
					return e
				}
			}
		}

		// Mint JWT
		t, e := generateJWT(req.TelegramID)
		if e != nil {
			return e
		}
		token = t
		return nil
	}); err != nil {
		low := strings.ToLower(err.Error())
		if strings.Contains(low, "duplicate key") || strings.Contains(low, "unique") || strings.Contains(low, "idx_users_phone") {
			c.JSON(http.StatusConflict, gin.H{"error": "phone already used"})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok", "token": token})
}

/* ==========================
   Wallet handlers
========================== */

func ptrOrNil(s string) *string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	v := s
	return &v
}

func getWallet(c *gin.Context) {
	tid, ok := parseTelegramIDParam(c)
	if !ok {
		return
	}
	var user User
	if err := db.Where("telegram_id = ?", tid).First(&user).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "User not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"telegram_id":       user.TelegramID,
		"balance_birr":      toBirrString(user.BalanceCents),
		"main_balance_birr": toBirrString(user.MainBalanceCents),
	})
}

// awardReferralBonusOnDeposit gives 10 birr to referrer on every deposit made by their referral
func awardReferralBonusOnDeposit(tx *gorm.DB, depositor User, depositAmountCents int64, depositTxID uint) error {
	// Check if this user was referred by someone
	if depositor.ReferredBy == nil {
		return nil // No referrer, no bonus
	}

	referrerTID := *depositor.ReferredBy
	if referrerTID == 0 || referrerTID == depositor.TelegramID {
		return nil // Invalid referrer
	}

	// Get the referrer
	var referrer User
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("telegram_id = ?", referrerTID).First(&referrer).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil // Referrer doesn't exist anymore
		}
		return err
	}

	// Fixed bonus amount: 10 birr = 1000 cents
	referralBonusCents := int64(10 * 100)

	// Check if we've already paid this bonus for this specific deposit (idempotency)
	var existingPayout ReferralPayout
	if err := tx.Where("deposit_tx_id = ?", depositTxID).First(&existingPayout).Error; err == nil {
		return nil // Already paid for this deposit
	}

	// Credit the referrer
	referrer.BalanceCents += referralBonusCents
	if err := tx.Save(&referrer).Error; err != nil {
		return err
	}

	// Record the payout
	payout := ReferralPayout{
		InviterTID:  referrerTID,
		InviteeTID:  depositor.TelegramID,
		DepositTxID: depositTxID,
		AmountCents: referralBonusCents,
	}
	if err := tx.Create(&payout).Error; err != nil {
		return err
	}

	// Record transaction for the referrer
	bonusStr := toBirrString(referralBonusCents)
	referrerTx := Transaction{
		TelegramID: referrerTID,
		Type:       "referral_bonus",
		Method:     "+",
		Status:     "success",
		Note:       fmt.Sprintf("Referral bonus - user %d deposited %s", depositor.TelegramID, toBirrString(depositAmountCents)),
		Amount:     bonusStr,
		Bonus:      "0.00",
		Total:      bonusStr,
		Account:    "balance",
	}
	if err := tx.Create(&referrerTx).Error; err != nil {
		return err
	}

	log.Printf("[REFERRAL] Awarded %s to referrer %d for deposit by user %d",
		bonusStr, referrerTID, depositor.TelegramID)

	// Send notification to referrer
	go notifyReferralDepositBonus(referrerTID, depositor.TelegramID, bonusStr, toBirrString(depositAmountCents))

	return nil
}

// notifyReferralDepositBonus sends a DM to the referrer when they earn a bonus
func notifyReferralDepositBonus(referrerTID, depositorTID int64, bonusAmount, depositAmount string) {
	if !getenvBool("REFERRAL_NOTIFY", true) {
		return
	}

	botToken := strings.TrimSpace(os.Getenv("BOT_TOKEN"))
	if botToken == "" {
		log.Printf("notifyReferralDepositBonus: BOT_TOKEN missing; skipping DM")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()

	message := fmt.Sprintf(
		"💰 <b>Referral Deposit Bonus!</b>\n\n"+
			"Your referral just made a deposit of <b>%s ብር</b>.\n"+
			"You earned: <b>%s ብር</b>\n\n"+
			"Keep inviting to earn more on every deposit! 🎯",
		depositAmount, bonusAmount,
	)

	if err := sendTelegramDM(ctx, botToken, referrerTID, message); err != nil {
		log.Printf("Failed to notify referrer %d about deposit bonus: %v", referrerTID, err)
	}
}
func creditWallet(c *gin.Context) {
	tid, ok := parseTelegramIDParam(c)
	if !ok {
		return
	}

	var req WalletChangeReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	amountCents, err := fromBirrToCents(req.Amount)
	if err != nil || amountCents <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid amount"})
		return
	}

	if err := db.Transaction(func(tx *gorm.DB) error {
		var u User
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("telegram_id = ?", tid).First(&u).Error; err != nil {
			return err
		}

		u.BalanceCents += amountCents

		if err := tx.Save(&u).Error; err != nil {
			return err
		}

		amountStr := toBirrString(amountCents)
		zero := "0.00"

		t := Transaction{
			TelegramID: u.TelegramID,
			Type:       string(TxnDeposit),
			Method:     "+",       // deposit
			Status:     "success", // created as success here
			Note:       req.Note,
			Amount:     amountStr,
			Bonus:      zero,
			Total:      amountStr,
			Account:    "",
			Reference:  nil,
			TxID:       nil,
		}
		if err := tx.Create(&t).Error; err != nil {
			return err
		}

		return nil
	}); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	getWallet(c)
}
func debitWallet(c *gin.Context) {
	tid, ok := parseTelegramIDParam(c)
	if !ok {
		return
	}

	var req WalletChangeReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	amountCents, err := fromBirrToCents(req.Amount)
	if err != nil || amountCents <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid amount"})
		return
	}

	if err := db.Transaction(func(tx *gorm.DB) error {
		var u User
		if err := tx.
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("telegram_id = ?", tid).
			First(&u).Error; err != nil {
			return err
		}

		// 1) Require sufficient MAIN balance
		if u.MainBalanceCents < amountCents {
			return errors.New("insufficient main balance")
		}

		// 2) Debit MAIN balance (do NOT touch play balance)
		u.MainBalanceCents -= amountCents

		if err := tx.Save(&u).Error; err != nil {
			return err
		}

		amountStr := toBirrString(amountCents)
		zero := "0.00"

		// Record withdrawal from MAIN (usually pending until processed)
		t := Transaction{
			TelegramID: u.TelegramID,
			Type:       string(TxnWithdraw),
			Method:     "-",
			Status:     "pending",
			Note:       strings.TrimSpace(req.Note),
			Amount:     amountStr,
			Bonus:      zero,
			Total:      amountStr,
			Account:    "main", // helpful to mark source
			Reference:  nil,
			TxID:       nil,
		}
		return tx.Create(&t).Error
	}); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	getWallet(c)
}

func transfer(c *gin.Context) {
	var req TransferReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	amountCents, err := fromBirrToCents(req.Amount)
	if err != nil || amountCents <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid amount"})
		return
	}
	target := strings.ToLower(strings.TrimSpace(req.Target))
	if target == "" {
		target = "balance"
	}

	// Do the work with a context so it can’t hang silently
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	var fromAfter, toAfter User

	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Lock sender
		var from User
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("telegram_id = ?", req.FromTelegramID).
			First(&from).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return errors.New("sender not found")
			}
			return err
		}

		// Resolve & lock recipient (by username OR phone)
		to, err := findUserByUsernameOrPhone(tx, req.ToUsername, req.ToPhone)
		if err != nil {
			return err
		}
		if from.TelegramID == to.TelegramID {
			return errors.New("cannot transfer to self")
		}
		// Lock recipient row
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ?", to.ID).First(to).Error; err != nil {
			return err
		}

		// Avoid deadlocks by updating in a stable order (lowest ID first)
		a, b := &from, to
		first, second := a, b
		if b.ID < a.ID {
			first, second = b, a
		}

		// Apply transfer
		switch target {
		case "main":
			if from.MainBalanceCents < amountCents {
				return errors.New("insufficient main balance")
			}
			from.MainBalanceCents -= amountCents
			to.MainBalanceCents += amountCents
		default:
			if from.BalanceCents < amountCents {
				return errors.New("insufficient balance")
			}
			from.BalanceCents -= amountCents
			to.BalanceCents += amountCents
		}

		// Save in stable order
		if err := tx.Save(first).Error; err != nil {
			return err
		}
		if err := tx.Save(second).Error; err != nil {
			return err
		}

		fromAfter = from
		toAfter = *to
		return nil
	})
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":      "ok",
		"target":      target,
		"amount_birr": req.Amount,
		"from": gin.H{
			"telegram_id":       fromAfter.TelegramID,
			"balance_birr":      toBirrString(fromAfter.BalanceCents),
			"main_balance_birr": toBirrString(fromAfter.MainBalanceCents),
		},
		"to": gin.H{
			"telegram_id": toAfter.TelegramID,
			"username":    toAfter.Username,
			"phone": func() string {
				if toAfter.Phone != nil {
					return *toAfter.Phone
				}
				return ""
			}(),
			"balance_birr":      toBirrString(toAfter.BalanceCents),
			"main_balance_birr": toBirrString(toAfter.MainBalanceCents),
		},
	})
}

/* ==========================
   Transactions
========================== */

func createTransaction(c *gin.Context) {
	// Admin-only guard (route should also use authJWT)
	// raw, _ := c.Get(string(ctxTID))
	// callerTID, _ := raw.(int64)
	// if !isUserAdminByTID(callerTID) {
	// 	c.JSON(http.StatusForbidden, gin.H{"error": "admin only"})
	// 	return
	// }

	const (
		maxLenReference   = 64
		maxLenTxID        = 64
		maxLenAccount     = 64
		maxLenPaymentDate = 64
		maxLenMethod      = 30
		maxLenStatus      = 20
		maxLenName        = 128
	)

	truncateUTF8 := func(s string, max int) string {
		r := []rune(strings.TrimSpace(s))
		if len(r) > max {
			return string(r[:max])
		}
		return string(r)
	}

	var req createTxnReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	method := truncateUTF8(strings.ToLower(req.Method), maxLenMethod)
	status := truncateUTF8(strings.ToLower(req.Status), maxLenStatus)
	refClamped := truncateUTF8(req.Reference, maxLenReference)
	txidClamped := truncateUTF8(req.TxID, maxLenTxID)
	account := truncateUTF8(req.Account, maxLenAccount)
	paymentDate := truncateUTF8(req.PaymentDate, maxLenPaymentDate)
	senderName := truncateUTF8(req.SenderName, maxLenName)
	receiverName := truncateUTF8(req.ReceiverName, maxLenName)

	tx := Transaction{
		Reference:    ptrOrNil(refClamped),
		TelegramID:   req.TelegramID,
		Type:         strings.ToLower(strings.TrimSpace(req.Type)),
		Method:       method,
		Status:       status,
		Note:         req.Note,
		Amount:       strings.TrimSpace(req.Amount),
		Bonus:        strings.TrimSpace(req.Bonus),
		Total:        strings.TrimSpace(req.Total),
		TxID:         ptrOrNil(txidClamped),
		Account:      account,
		SenderName:   senderName,
		ReceiverName: receiverName,
		ReceiptURL:   strings.TrimSpace(req.ReceiptURL),
		PaymentDate:  paymentDate,
	}

	if err := db.Create(&tx).Error; err != nil {
		low := strings.ToLower(err.Error())
		if strings.Contains(low, "duplicate key") ||
			strings.Contains(low, "23505") ||
			strings.Contains(low, "unique constraint") ||
			strings.Contains(low, "idx_transactions_tx_id") ||
			strings.Contains(low, "uniq_tx_reference") {
			c.JSON(http.StatusConflict, gin.H{"error": "duplicate", "field": "txid_or_reference"})
			return
		}
		if strings.Contains(low, "value too long") || strings.Contains(low, "22001") {
			c.JSON(http.StatusBadRequest, gin.H{"error": "value too long for column"})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, tx)
}

func listTransactions(c *gin.Context) {
	tidStr := c.Param("telegram_id")
	tid, err := strconv.ParseInt(tidStr, 10, 64)
	if err != nil || tid == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid telegram_id"})
		return
	}
	limit := 1000
	if l := strings.TrimSpace(c.Query("limit")); l != "" {
		if v, e := strconv.Atoi(l); e == nil && v > 0 && v <= 50 {
			limit = v
		}
	}
	var txns []Transaction
	if err := db.Where("telegram_id = ?", tid).
		Order("id DESC").
		Limit(limit).
		Find(&txns).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, txns)
}

func updateTransactionStatusByReference(c *gin.Context) {
	ref := strings.TrimSpace(c.Param("reference"))
	if ref == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "reference required"})
		return
	}

	var req UpdateTxnStatusReq
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.Status) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "status required"})
		return
	}
	newStatus := strings.ToLower(strings.TrimSpace(req.Status))

	// Run the status change + potential referral award atomically
	if err := db.Transaction(func(tx *gorm.DB) error {
		// Lock the transaction row by reference
		var t Transaction
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("reference = ?", ref).First(&t).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("transaction with reference not found")
			}
			return err
		}

		// Update status
		if err := tx.Model(&t).Update("status", newStatus).Error; err != nil {
			return err
		}
		t.Status = newStatus // keep local copy in sync

		// If this is now a successful deposit, attempt referral award (idempotent)
		if strings.ToLower(t.Type) == "deposit" && newStatus == "success" {
			if err := awardReferralOnDepositTX(tx, t); err != nil {
				return err
			}
			if err := tx.Model(&User{}).
				Where("telegram_id = ?", t.TelegramID).
				Update("has_deposit", true).Error; err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

/* ==========================
   Rooms REST mirror (DB)
   NOTE: Room + RoomPlayer are defined in ws.go
========================== */

func getRoomStateFromDB(c *gin.Context) {
	roomID := strings.TrimSpace(c.Param("room_id"))
	if roomID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "room_id required"})
		return
	}

	var r Room
	if err := db.Where("room_id = ?", roomID).First(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	var players []RoomPlayer
	if err := db.Where("room_id = ?", roomID).Find(&players).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	selected := make([]int, 0, len(players))
	for _, p := range players {
		if p.BoardNumber != nil {
			selected = append(selected, *p.BoardNumber)
		}
	}

	var startStr *string
	if r.StartTime != nil {
		s := r.StartTime.UTC().Format(time.RFC3339)
		startStr = &s
	}

	// Convert cents -> ETB for frontend (which expects ETB values)
	stakeETB := r.StakeAmount / 100
	possibleWinETB := r.PossibleWin / 100

	c.JSON(http.StatusOK, gin.H{
		"room_id":                r.RoomID,
		"stake_amount":           stakeETB,
		"status":                 r.Status,
		"start_time":             startStr, // RFC3339 or null
		"possible_win":           possibleWinETB,
		"number_of_players":      len(players),
		"selected_board_numbers": selected,
	})
}

/* ==========================
   Referral link endpoint
========================== */

func referralLink(c *gin.Context) {
	tid, ok := parseTelegramIDParam(c)
	if !ok {
		return
	}
	link, err := referralLinkFor(tid)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"link": link})
}

/* ==========================
   Router / main
========================== */

// ===== Rooms bootstrap helpers =====

func autoMigrateAll() error {
	return db.AutoMigrate(&User{}, &Transaction{}, &Room{}, &RoomPlayer{}, &Referral{})
}

func seedDefaultRoomsTX(tx *gorm.DB) error {
	defaults := []int64{10, 20, 50, 100}
	for _, stake := range defaults {
		r := Room{
			RoomID:      fmt.Sprintf("%d", stake),
			StakeAmount: stake,
			Status:      "pending", // or "idle"
			StartTime:   nil,
			PossibleWin: 0,
		}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&r).Error; err != nil {
			return err
		}
	}
	return nil
}

// load DB rooms into the in-memory hub; create defaults if none
func bootRoomsIntoHub() error {
	return db.Transaction(func(tx *gorm.DB) error {
		var rows []Room
		if err := tx.Order("stake_amount ASC").Find(&rows).Error; err != nil {
			return err
		}
		if len(rows) == 0 {
			if err := seedDefaultRoomsTX(tx); err != nil {
				return err
			}
			if err := tx.Order("stake_amount ASC").Find(&rows).Error; err != nil {
				return err
			}
		}

		// load all players once
		var rps []RoomPlayer
		if err := tx.Find(&rps).Error; err != nil {
			return err
		}
		playerMap := map[string][]RoomPlayer{}
		for _, p := range rps {
			playerMap[p.RoomID] = append(playerMap[p.RoomID], p)
		}

		lives := make([]*roomLive, 0, len(rows))
		for _, r := range rows {
			rl := newRoomLive(r.RoomID, r.StakeAmount)

			// DIRECT field assignment (matches your ws.go struct):
			rl.Status = r.Status
			if r.StartTime != nil {
				t := r.StartTime.UTC()
				rl.StartTime = &t
			} else {
				rl.StartTime = nil
			}

			// rehydrate players & selected boards
			for _, p := range playerMap[r.RoomID] {
				rl.players[p.TelegramID] = struct{}{}
				if p.BoardNumber != nil {
					rl.selected[*p.BoardNumber] = p.TelegramID
				}
			}

			lives = append(lives, rl)
		}

		// replace in-memory hub state
		globalRoomsHub.replaceAll(lives)
		return nil
	})
}
func adminClearRooms(c *gin.Context) {
	if err := db.Transaction(func(tx *gorm.DB) error {
		// remove players first (FK safety)
		if err := tx.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(&RoomPlayer{}).Error; err != nil {
			return err
		}
		// remove rooms
		if err := tx.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(&Room{}).Error; err != nil {
			return err
		}
		return nil
	}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// refresh in-memory hub to empty
	globalRoomsHub.replaceAll(nil)
	globalRoomsHub.broadcastRooms()

	c.JSON(http.StatusOK, gin.H{"status": "ok", "cleared": true})
}
// GET /transactions - List all transactions (no limits)
func listAllTransactions(c *gin.Context) {
	// Optional: Add admin authentication
	// raw, _ := c.Get(string(ctxTID))
	// callerTID, _ := raw.(int64)
	// if !isUserAdminByTID(callerTID) {
	// 	c.JSON(http.StatusForbidden, gin.H{"error": "admin only"})
	// 	return
	// }

	var txns []Transaction
	if err := db.
		Where("type IN ?", []string{"deposit", "withdraw"}).
		Order("id DESC").
		Find(&txns).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, txns)
}
// ==========================
// Report API using PENDING deposits (clean amounts)
// ==========================

type DailyReport struct {
	Date            string        `json:"date"`
	SuccessDeposit  float64       `json:"success_deposit"`  // Using PENDING deposit amounts (clean, no bonus)
	PendingWithdraw float64       `json:"pending_withdraw"` // Only genuine pending withdrawals (not transfers)
	NetBalance      float64       `json:"net_balance"`      // Deposit - Withdraw
	Deposits        []Transaction `json:"deposits"`         // Array of PENDING deposit transactions
	Withdrawals     []Transaction `json:"withdrawals"`      // Array of genuine pending withdrawal transactions
}

type WeeklyReport struct {
	WeekStart     string        `json:"week_start"`
	WeekEnd       string        `json:"week_end"`
	TotalDeposit  float64       `json:"total_deposit"`  // Sum of PENDING deposit amounts (clean, no bonus)
	TotalWithdraw float64       `json:"total_withdraw"` // Sum of genuine pending withdrawals
	NetBalance    float64       `json:"net_balance"`    // TotalDeposit - TotalWithdraw
	DailyReports  []DailyReport `json:"daily_reports"`  // Daily breakdown with filtered transactions
}

// Helper function to check if transaction is a genuine deposit (NOT bonus/transfer)
// Now we use PENDING deposits instead of SUCCESS
func isGenuineDeposit(t Transaction) bool {
	// Must be deposit type
	if strings.ToLower(t.Type) != "deposit" {
		return false
	}

	// Use PENDING status (clean amounts) instead of SUCCESS
	if strings.ToLower(t.Status) != "pending" {
		return false
	}

	// Check note for excluded patterns
	note := strings.ToLower(strings.TrimSpace(t.Note))

	// Exclude transfer transactions
	if strings.Contains(note, "transfer from") || strings.Contains(note, "transfer to") {
		return false
	}

	// Exclude referral bonus transactions
	if strings.Contains(note, "referral bonus") ||
		strings.Contains(note, "referral_") ||
		strings.Contains(note, "invitee") ||
		strings.Contains(note, "inviter") {
		return false
	}

	// Exclude phone verification bonus
	if strings.Contains(note, "phone verification") ||
		strings.Contains(note, "phone_verify") {
		return false
	}

	// Exclude promo bonuses
	if strings.Contains(note, "promo bonus") ||
		strings.Contains(note, "promo_bonus") ||
		strings.Contains(note, "promotional") {
		return false
	}

	// Exclude join bonus
	if strings.Contains(note, "join bonus") || strings.Contains(note, "welcome bonus") {
		return false
	}

	// Check transaction type field specifically
	txType := strings.ToLower(strings.TrimSpace(t.Type))
	if txType == "referral_bonus" ||
		txType == "phone_verify_bonus" ||
		txType == "promo_bonus" ||
		txType == "referral_join_bonus" ||
		txType == "referral_deposit_bonus" {
		return false
	}

	return true
}

// Helper function to check if transaction is a genuine withdrawal (NOT internal transfer)
func isGenuineWithdrawal(t Transaction) bool {
	// Must be withdraw type
	if strings.ToLower(t.Type) != "withdraw" {
		return false
	}

	// Must be pending status
	if strings.ToLower(t.Status) != "pending" {
		return false
	}

	// Check note for patterns - exclude internal transfers
	note := strings.ToLower(strings.TrimSpace(t.Note))

	// Exclude transfer transactions (internal transfers between users)
	if strings.Contains(note, "transfer to") || strings.Contains(note, "transfer from") {
		return false
	}

	// Check for genuine withdrawal patterns
	// Keep withdrawals that mention payment methods or are simple "withdraw" notes
	if strings.Contains(note, "withdraw telebirr") ||
		strings.Contains(note, "withdraw cbe") ||
		strings.Contains(note, "withdraw ebirr") ||
		strings.Contains(note, "withdraw e-birr") ||
		strings.Contains(note, "withdraw") ||
		note == "" ||
		strings.Contains(note, "payment") ||
		strings.Contains(note, "payout") {
		return true
	}

	// If note contains a payment method name, it's likely genuine
	if strings.Contains(note, "telebirr") ||
		strings.Contains(note, "cbe") ||
		strings.Contains(note, "ebirr") ||
		strings.Contains(note, "e-birr") ||
		strings.Contains(note, "bank") ||
		strings.Contains(note, "bank transfer") {
		return true
	}

	// Default: if we're not sure, include it (safer than excluding)
	return true
}

// GET /report/daily?date=YYYY-MM-DD
func getDailyReport(c *gin.Context) {
	dateStr := c.Query("date")
	if dateStr == "" {
		// Default to today in ET
		now := time.Now().UTC()
		etNow := now.Add(3 * time.Hour) // UTC+3
		dateStr = etNow.Format("2006-01-02")
	}

	// Parse date and get range for Ethiopian day (00:00-23:59 ET)
	date, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid date format"})
		return
	}

	// Convert to UTC for DB query (ET 00:00 = UTC 21:00 previous day)
	startUTC := date.Add(-3 * time.Hour)
	endUTC := startUTC.Add(24 * time.Hour)

	// Get PENDING deposits for this day (clean amounts, no bonus included)
	var pendingDeposits []Transaction
	err = db.Where("type = ?", "deposit").
		Where("status = ?", "pending"). // Use PENDING deposits for clean amounts
		Where("created_at >= ? AND created_at < ?", startUTC, endUTC).
		Order("created_at DESC").
		Find(&pendingDeposits).Error

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Filter to get only genuine pending deposits (exclude bonuses/transfers)
	var genuineDeposits []Transaction
	for _, deposit := range pendingDeposits {
		if isGenuineDeposit(deposit) {
			genuineDeposits = append(genuineDeposits, deposit)
		}
	}

	// Get PENDING withdrawals for this day
	var pendingWithdrawals []Transaction
	err = db.Where("type = ?", "withdraw").
		Where("status = ?", "pending").
		Where("created_at >= ? AND created_at < ?", startUTC, endUTC).
		Order("created_at DESC").
		Find(&pendingWithdrawals).Error

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Filter to get only genuine withdrawals (exclude internal transfers)
	var genuineWithdrawals []Transaction
	for _, withdrawal := range pendingWithdrawals {
		if isGenuineWithdrawal(withdrawal) {
			genuineWithdrawals = append(genuineWithdrawals, withdrawal)
		}
	}

	// Calculate totals from PENDING deposits (clean amounts)
	var depositTotal, withdrawTotal float64
	var totalBonusAmount float64 // Bonus amount that will be added when deposit succeeds
	
	for _, d := range genuineDeposits {
		// Use the Amount field from pending deposits (clean amount without bonus)
		amount, _ := strconv.ParseFloat(d.Amount, 64)
		depositTotal += amount
		
		// Calculate bonus amount for reference
		bonus, _ := strconv.ParseFloat(d.Bonus, 64)
		totalBonusAmount += bonus
	}

	for _, w := range genuineWithdrawals {
		amount, _ := strconv.ParseFloat(w.Amount, 64)
		withdrawTotal += amount
	}

	netBalance := depositTotal - withdrawTotal

	report := DailyReport{
		Date:            dateStr,
		SuccessDeposit:  depositTotal,  // Clean amount from pending deposits
		PendingWithdraw: withdrawTotal,
		NetBalance:      netBalance,
		Deposits:        genuineDeposits,    // PENDING deposits with clean amounts
		Withdrawals:     genuineWithdrawals, // PENDING withdrawals
	}

	c.JSON(http.StatusOK, gin.H{
		"report": report,
		"metadata": gin.H{
			"deposit_count":        len(genuineDeposits),
			"withdraw_count":       len(genuineWithdrawals),
			"total_bonus_to_add":   totalBonusAmount, // Bonus that will be added when deposits succeed
			"total_with_bonus":     depositTotal + totalBonusAmount, // What users will receive
			"note": "Using PENDING deposits for clean amounts (no bonus included in Amount field)",
		},
	})
}

// GET /report/weekly?week_start=YYYY-MM-DD
func getWeeklyReport(c *gin.Context) {
	// Get week start date or default to current week's Monday
	weekStartStr := c.Query("week_start")
	var weekStart time.Time
	var err error

	if weekStartStr != "" {
		weekStart, err = time.Parse("2006-01-02", weekStartStr)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid week_start format"})
			return
		}
	} else {
		// Default to Monday of current week (Ethiopian time UTC+3)
		now := time.Now().UTC()
		etNow := now.Add(3 * time.Hour) // UTC+3

		// Get Monday (week starts Monday)
		weekday := etNow.Weekday()
		daysSinceMonday := 0
		if weekday == time.Sunday {
			daysSinceMonday = 6
		} else {
			daysSinceMonday = int(weekday) - 1
		}

		weekStart = etNow.AddDate(0, 0, -daysSinceMonday)
		weekStart = time.Date(weekStart.Year(), weekStart.Month(), weekStart.Day(), 0, 0, 0, 0, time.UTC)
	}

	weekEnd := weekStart.AddDate(0, 0, 6)

	// Convert to UTC for queries
	startUTC := weekStart.Add(-3 * time.Hour)
	endUTC := weekEnd.Add(-3 * time.Hour).Add(24 * time.Hour)

	// Query for PENDING deposits in the week (clean amounts)
	var pendingDeposits []Transaction
	err = db.Where("type = ?", "deposit").
		Where("status = ?", "pending"). // Use PENDING for clean amounts
		Where("created_at >= ? AND created_at < ?", startUTC, endUTC).
		// Exclude bonus/transfer deposits by note patterns
		Where("(note IS NULL OR "+
			"(note NOT ILIKE '%transfer from%' AND "+
			"note NOT ILIKE '%transfer to%' AND "+
			"note NOT ILIKE '%referral bonus%' AND "+
			"note NOT ILIKE '%phone verification%' AND "+
			"note NOT ILIKE '%phone_verify%' AND "+
			"note NOT ILIKE '%promo bonus%' AND "+
			"note NOT ILIKE '%join bonus%' AND "+
			"note NOT ILIKE '%welcome bonus%'))").
		// Also exclude by transaction type
		Where("type NOT IN (?, ?, ?, ?, ?)",
			"referral_bonus",
			"phone_verify_bonus",
			"promo_bonus",
			"referral_join_bonus",
			"referral_deposit_bonus").
		Find(&pendingDeposits).Error

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Calculate REAL deposit total from PENDING deposits (clean amounts)
	var realDepositTotal float64
	var depositCount int
	var totalBonusAmount float64
	
	for _, deposit := range pendingDeposits {
		if isGenuineDeposit(deposit) {
			// Use Amount field from pending deposit (clean amount)
			amount, _ := strconv.ParseFloat(deposit.Amount, 64)
			realDepositTotal += amount
			depositCount++
			
			// Calculate bonus that will be added
			bonus, _ := strconv.ParseFloat(deposit.Bonus, 64)
			totalBonusAmount += bonus
		}
	}

	// Query for PENDING withdrawals in the week (exclude internal transfers)
	var withdrawResult struct {
		Total float64
		Count int
	}

	err = db.Model(&Transaction{}).
		Select("COALESCE(SUM(CAST(amount AS numeric)), 0) as total, COUNT(*) as count").
		Where("type = ?", "withdraw").
		Where("status = ?", "pending").
		Where("created_at >= ? AND created_at < ?", startUTC, endUTC).
		// Exclude transfer withdrawals but keep payment method withdrawals
		Where("(note IS NULL OR " +
			"(note NOT ILIKE '%transfer to%' AND note NOT ILIKE '%transfer from%'))").
		Scan(&withdrawResult).Error

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Calculate net balance with REAL deposit amounts (clean from pending deposits)
	netBalance := realDepositTotal - withdrawResult.Total

	report := WeeklyReport{
		WeekStart:     weekStart.Format("2006-01-02"),
		WeekEnd:       weekEnd.Format("2006-01-02"),
		TotalDeposit:  realDepositTotal,  // Clean amount from pending deposits
		TotalWithdraw: withdrawResult.Total,
		NetBalance:    netBalance,
	}

	c.JSON(http.StatusOK, gin.H{
		"report": report,
		"metadata": gin.H{
			"deposit_count":      depositCount,
			"withdraw_count":     withdrawResult.Count,
			"total_bonus_to_add": totalBonusAmount,
			"total_with_bonus":   realDepositTotal + totalBonusAmount,
			"note": "Using PENDING deposits for accurate reporting (Amount field is clean, Bonus field shows bonus to be added)",
		},
	})
}

// GET /report/weekly-detailed?week_start=YYYY-MM-DD
// Returns daily breakdown for the week with filtered transactions
func getWeeklyDetailedReport(c *gin.Context) {
	// Get week start date
	weekStartStr := c.Query("week_start")
	var weekStart time.Time
	var err error

	if weekStartStr != "" {
		weekStart, err = time.Parse("2006-01-02", weekStartStr)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid week_start format"})
			return
		}
	} else {
		// Default to Monday of current week
		now := time.Now().UTC()
		etNow := now.Add(3 * time.Hour)

		weekday := etNow.Weekday()
		daysSinceMonday := 0
		if weekday == time.Sunday {
			daysSinceMonday = 6
		} else {
			daysSinceMonday = int(weekday) - 1
		}

		weekStart = etNow.AddDate(0, 0, -daysSinceMonday)
		weekStart = time.Date(weekStart.Year(), weekStart.Month(), weekStart.Day(), 0, 0, 0, 0, time.UTC)
	}

	weekEnd := weekStart.AddDate(0, 0, 6)

	// Generate all dates in the week
	dates := make([]string, 7)
	current := weekStart
	for i := 0; i < 7; i++ {
		dates[i] = current.Format("2006-01-02")
		current = current.AddDate(0, 0, 1)
	}

	// Get daily reports with filtered transactions for each day
	dailyReports := make([]DailyReport, 0, 7)
	var totalRealDeposit, totalWithdraw float64
	var totalBonusAmount float64

	for _, dateStr := range dates {
		date, _ := time.Parse("2006-01-02", dateStr)
		startUTC := date.Add(-3 * time.Hour)
		endUTC := startUTC.Add(24 * time.Hour)

		// Get PENDING deposits for this day (clean amounts)
		var pendingDeposits []Transaction
		db.Where("type = ?", "deposit").
			Where("status = ?", "pending"). // Use PENDING for clean amounts
			Where("created_at >= ? AND created_at < ?", startUTC, endUTC).
			Order("created_at DESC").
			Find(&pendingDeposits)

		// Filter to get only genuine pending deposits (exclude bonuses/transfers)
		var genuineDeposits []Transaction
		for _, deposit := range pendingDeposits {
			if isGenuineDeposit(deposit) {
				genuineDeposits = append(genuineDeposits, deposit)
			}
		}

		// Get PENDING withdrawals for this day
		var pendingWithdrawals []Transaction
		db.Where("type = ?", "withdraw").
			Where("status = ?", "pending").
			Where("created_at >= ? AND created_at < ?", startUTC, endUTC).
			Order("created_at DESC").
			Find(&pendingWithdrawals)

		// Filter to get only genuine withdrawals (exclude internal transfers)
		var genuineWithdrawals []Transaction
		for _, withdrawal := range pendingWithdrawals {
			if isGenuineWithdrawal(withdrawal) {
				genuineWithdrawals = append(genuineWithdrawals, withdrawal)
			}
		}

		// Calculate REAL deposit totals from PENDING deposits (clean amounts)
		var dayRealDepositTotal, dayWithdrawTotal float64
		var dayBonusAmount float64
		
		for _, d := range genuineDeposits {
			// Use Amount field from pending deposit (clean amount)
			amount, _ := strconv.ParseFloat(d.Amount, 64)
			dayRealDepositTotal += amount
			
			// Calculate bonus that will be added
			bonus, _ := strconv.ParseFloat(d.Bonus, 64)
			dayBonusAmount += bonus
		}

		for _, w := range genuineWithdrawals {
			amount, _ := strconv.ParseFloat(w.Amount, 64)
			dayWithdrawTotal += amount
		}

		netBalance := dayRealDepositTotal - dayWithdrawTotal

		dailyReport := DailyReport{
			Date:            dateStr,
			SuccessDeposit:  dayRealDepositTotal,  // Clean amount from pending deposits
			PendingWithdraw: dayWithdrawTotal,
			NetBalance:      netBalance,
			Deposits:        genuineDeposits,    // PENDING deposits with clean amounts
			Withdrawals:     genuineWithdrawals, // PENDING withdrawals
		}

		dailyReports = append(dailyReports, dailyReport)
		totalRealDeposit += dayRealDepositTotal
		totalWithdraw += dayWithdrawTotal
		totalBonusAmount += dayBonusAmount
	}

	weeklyReport := WeeklyReport{
		WeekStart:     weekStart.Format("2006-01-02"),
		WeekEnd:       weekEnd.Format("2006-01-02"),
		TotalDeposit:  totalRealDeposit,  // Clean amount from pending deposits
		TotalWithdraw: totalWithdraw,
		NetBalance:    totalRealDeposit - totalWithdraw,
		DailyReports:  dailyReports,
	}

	c.JSON(http.StatusOK, gin.H{
		"report": weeklyReport,
		"metadata": gin.H{
			"total_bonus_to_add": totalBonusAmount,
			"total_with_bonus":   totalRealDeposit + totalBonusAmount,
			"note": "All calculations use PENDING transactions. Deposits show clean amounts (no bonus), withdrawals show requested amounts.",
		},
	})
}
func main() {
	mustLoadEnv()
	initDB()

	// start the bot manager (optional)
	// StartBotManager()

	// 1) migrate everything up front
	if err := autoMigrateAll(); err != nil {
		log.Fatal("AutoMigrate failed:", err)
	}

	// 2) start the hub
	globalRoomsHub = newRoomsHub()
	go globalRoomsHub.run()

	// 3) load/seed rooms into hub from DB
	if err := bootRoomsIntoHub(); err != nil {
		log.Fatal("bootRoomsIntoHub failed:", err)
	}


	// preload rooms from DB so /ws/rooms has something to send right away
	if rooms, err := loadAllRoomsFromDB(); err == nil {
		globalRoomsHub.replaceAll(rooms)
		globalRoomsHub.broadcastRooms()
	} else {
		log.Println("loadAllRoomsFromDB:", err)
	}

	r := gin.Default()

	// RegisterBroadcastRoutes(r)

	r.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Authorization", "Accept", "X-Requested-With", "ngrok-skip-browser-warning"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: false,
		MaxAge:           12 * time.Hour,
	}))

	// WS endpoints
	r.GET("/ws/rooms", func(c *gin.Context) { wsRoomsHandler(c) })
	r.GET("/ws/room/:room_id", func(c *gin.Context) { wsRoomHandler(c) })
	// Admin room reset (add auth if you want it locked down)
	r.POST("/admin/rooms/:room_id/reset", resetRoomHandler)

	// Optional REST mirrors:
	r.GET("/rooms", func(c *gin.Context) { c.JSON(200, listRoomStates()) })
	r.GET("/rooms/:room_id", getRoomStateFromDB)
	r.POST("/admin/rooms/clear", adminClearRooms) // clears all rooms & players
	// in main() after r := gin.Default()
	r.POST("/admin/bonuses/depositors", authJWT(), adminBonusDepositors)

	// Users + Auth
	r.GET("/users", getUsers)
	r.POST("/users", createUser)
	r.GET("/users/:id", getUser)
	r.PUT("/users/:id", updateUser)
	r.DELETE("/users/:id", deleteUser)
	r.GET("/me", authJWT(), getMe)
	r.GET("/users/by-phone/:phone", getUserByPhone)

	// Telegram sync & tokens
	r.GET("/users/by-telegram/:telegram_id", getUserByTelegramID)
	r.GET("/users/by-username/:username", getUserByUsername)
	r.POST("/telegram/users/sync", upsertFromTelegram)
	r.POST("/telegrams/users/sync", upsertFromTelegram) // legacy alias
	r.POST("/telegram/users/share-phone", sharePhoneAndIssueToken)
	r.POST("/telegram/users/mint-token", mintToken)

	// Referral links
	r.GET("/referral/link/:telegram_id", referralLink)

	// Wallet
	r.GET("/wallet/:telegram_id", getWallet)
	r.POST("/wallet/:telegram_id/credit", creditWallet)
	r.POST("/wallet/:telegram_id/debit", debitWallet)
	r.POST("/wallet/transfer", transfer)
	r.POST("/wallet/transfers", transfer) // alias

	// Transactions
	r.GET("/transactions/:telegram_id", listTransactions)
	r.POST("/transactions/:reference/status", updateTransactionStatusByReference)
	r.POST("/transactions", createTransaction) // NOW auth + admin check inside
	r.GET("/transactions", listAllTransactions) // 👈 ADD THIS LINE

	// Add report routes
	r.GET("/report/daily", getDailyReport)
	r.GET("/report/weekly", getWeeklyReport)
	r.GET("/report/weekly-detailed", getWeeklyDetailedReport)

	// Health
	r.GET("/healthz", func(c *gin.Context) { c.String(200, "ok") })

	port := os.Getenv("PORT")
	if port == "" {
		port = "8004"
	}
	if err := r.Run(":" + port); err != nil {
		log.Fatal(err)
	}

	// Optional: seed 4 rooms if missing
	seedRoomsIfEmpty()
}

func seedRoomsIfEmpty() {
	var cnt int64
	if err := db.Model(&Room{}).Count(&cnt).Error; err != nil {
		log.Println("count rooms:", err)
		return
	}
	if cnt > 0 {
		return
	}

	stakes := []int64{10, 20, 50, 100}
	for _, s := range stakes {
		r := Room{
			RoomID:      fmt.Sprintf("stake-%d", s),
			StakeAmount: s,
			Status:      "Ready",
			PossibleWin: 0,
		}
		if err := db.Create(&r).Error; err != nil {
			log.Println("seed room err:", err)
		}
	}
}
