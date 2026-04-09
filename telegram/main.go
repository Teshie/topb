package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/PuerkitoBio/goquery"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/joho/godotenv"
	pdf "github.com/ledongthuc/pdf"
)

/* ===================== Types & DTOs ===================== */

// In the userDTO struct, add InviterTelegramID field
type userDTO struct {
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
	IsBot             bool      `json:"is_bot"`
	HasDeposit        bool      `json:"has_deposit"`
	InviterTelegramID *int64    `json:"inviter_telegram_id"` // 👈 ADD THIS LINE
}

type walletResp struct {
	TelegramID      int64  `json:"telegram_id"`
	BalanceBirr     string `json:"balance_birr"`      // Play
	MainBalanceBirr string `json:"main_balance_birr"` // Main
}

type TelebirrSMS struct {
	Invoice string
	Amount  string
	Payer   string
	Payee   string
	Link    string
	Date    string
}

type TelebirrReceipt struct {
	InvoiceNo         string
	PaymentDate       string
	SettledAmount     string
	PayerName         string
	CreditedPartyName string
	Link              string
	Source            string // "receipt" or "sms"

	Debug map[string]string `json:"debug,omitempty"`
}

type CBEReceipt struct {
	TxID              string
	PaymentDate       string
	TransferredAmount string
	PayerName         string
	ReceiverName      string
	PayerAccount      string
	ReceiverAccount   string
	Reason            string
	Commission        string
	VAT               string
	TotalDebited      string
	Link              string
	Source            string // "cbe"
}

type sharePhoneResp struct {
	Status string `json:"status"`
	Token  string `json:"token"`
}

// per-chat state
type depositSession struct {
	// Deposit
	AwaitingAmount bool
	AmountETB      int
	Reference      string

	// Transfer
	TransferMode             bool
	AwaitingTransferUsername bool
	AwaitingTransferAmount   bool
	TransferToUsername       string
	TransferTarget           string
	TransferToID             int64

	// Withdraw
	WithdrawMode         bool
	AwaitWithdrawAmount  bool
	AwaitWithdrawMethod  bool
	AwaitWithdrawAccount bool
	WithdrawAmountBirr   string
	WithdrawMethod       string
	WithdrawAccount      string
	WithdrawRef          string

	// Referral
	StartParam      string // e.g. "ref_397753549"
	SelectedMethod  string // "telebirr" | "ebirr" | "cbe" | ""
	TransferToLabel string // e.g. "@user" or "09xxxxxxx"

	// Bonus
	BonusMode        bool
	AwaitBonusAmount bool
	BonusAmountBirr  string
	BonusTag         string

	PostMode                bool
	AwaitPostText           bool
	ResumeDepositAfterPhone bool // 👈 ADD THIS LINE

	NotifyMode      bool
	AwaitNotifyText bool
}

/* ===================== Config ===================== */
var (
	APIBase               = getenvDefault("API_BASE", "https://henb.teshie.dev")
	AdminSupergroup int64 = -1002877017597

	PayeeName  = "Henok"
	PayeePhone = "0915418674"

	TelebirrAgentAcct  = "0915418674"
	NigedBankAgentAcct = "1000517969005"
	EBIRRAgentAcct     = "0915418674"
	AbyssiniaAgentAcct = "147857373"

	SupportHandle1 = "@"
	SupportHandle2 = "@"

	MinAmountETB = 10
	MaxAmountETB = 500000

	// Allow-listed receipt hosts
	ReceiptAllowedHost = "transactioninfo.ethiotelecom.et"
	CBEAllowedHost     = "apps.cbe.com.et"
	CBENewAllowedHost  = "mbreciept.cbe.com.et"
	BOAAllowedHost     = "cs.bankofabyssinia.com"
	EBirrAllowedHost   = "transactioninfo.ebirr.com" // NEW

	HTTPTimeout = 30 * time.Second
	UserAgent   = "Mozilla/5.0 (compatible; AddisBot/1.0; +https://example.com)"

	NotifyChatID int64 = -1002877017597

	// Require credited party to be this exact person (tokenized match)
	AllowedTelebirrReceiverName = getenvDefault("ALLOWED_RECEIVER_TELEBIRR", "Henok Belay Mendefro")
	AllowedCBEBirrReceiverName  = getenvDefault("ALLOWED_RECEIVER_CBE", "HENOK BELAY MANDEFRO")
	AllowedBOAReceiverName      = getenvDefault("ALLOWED_RECEIVER_BOA", "HENOK BELAY MANDEFRO")
	AllowedEBirrReceiverName    = getenvDefault("ALLOWED_RECEIVER_EBIRR", "Henok Belay Mandafro")
	CBEMobileAPIBase            = getenvDefault("CBE_MOBILE_API_BASE", "https://mb.cbe.com.et")
	CBEMobileAppID              = getenvDefault("CBE_MOBILE_APP_ID", "d1292e42-7400-49de-a2d3-9731caa4c819")
	CBEMobileAppVersion         = getenvDefault("CBE_MOBILE_APP_VERSION", "0a01980b-9859-1369-8198-59f403820000")
	// NEW: referral bonus amount used for the notifier message
	ReferralBonusBirr = getenvDefault("REFERRAL_BONUS_BIRR", "5")
	// In the config section, add this line
	PhoneVerifyBonusBirr = getenvDefault("PHONE_VERIFY_BONUS_BIRR", "8.00")
)

/* ===================== Local idempotency (TTL cache) ===================== */

var (
	seenMu  sync.Mutex
	seenTx  = map[string]time.Time{}
	seenRef = map[string]time.Time{}
	seenTTL = 48 * time.Hour
)

// noise words often appended to legal/business names; extend as needed
var receiverStopwords = map[string]struct{}{
	"plc": {}, "ltd": {}, "sc": {}, "s.c.": {}, "share": {}, "company": {}, "co": {},
	"bank": {}, "eth": {}, "ethiopia": {}, "et": {}, "&": {}, "and": {},
}

func methodFromReceiptURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	h := strings.ToLower(u.Hostname())
	switch h {
	case strings.ToLower(EBirrAllowedHost):
		return "ebirr"
	case strings.ToLower(ReceiptAllowedHost):
		return "telebirr"
	case strings.ToLower(CBEAllowedHost), strings.ToLower(CBENewAllowedHost):
		return "cbe"
	default:
		return ""
	}
}

func isAllowedCBEHost(hostname string) bool {
	h := strings.ToLower(strings.TrimSpace(hostname))
	return h == strings.ToLower(CBEAllowedHost) || h == strings.ToLower(CBENewAllowedHost)
}
func jsonDebug(v any) string {
	b, _ := json.MarshalIndent(v, "", "  ")
	return string(b)
}
func extractDate(input string) string {
	patterns := []string{
		`\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2}\s*[+-]\d{4}`,         // 2026/02/20 11:46:59 +0300
		`\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2}`,                     // 2026/02/20 11:46:59
		`\d{2}-\d{2}-\d{4}\s+\d{2}:\d{2}:\d{2}`,                     // 20-02-2026 11:20:27
		`\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}`,                     // 20/02/2026 17:16:23
		`\d{1,2}/\d{1,2}/\d{4}[,\s]+\d{1,2}:\d{2}(?::\d{2})?\s*(?:AM|PM)`, // 2/20/2026, 6:00:00 PM
	}

	for _, p := range patterns {
		re := regexp.MustCompile(p)
		match := re.FindString(input)
		if match != "" {
			// Remove any trailing text after AM/PM
			reTrim := regexp.MustCompile(`(AM|PM)`)
			loc := reTrim.FindStringIndex(match)
			if loc != nil {
				return strings.TrimSpace(match[:loc[1]])
			}
			return strings.TrimSpace(match)
		}
	}
	return ""
}

func parseFlexibleDate(dateStr string) (time.Time, error) {
	// Normalize multiple spaces to single space
	dateStr = regexp.MustCompile(`\s+`).ReplaceAllString(strings.TrimSpace(dateStr), " ")
	
	// Handle comma after date if present
	dateStr = strings.ReplaceAll(dateStr, ",", " ")

	layouts := []string{
		"2006/01/02 15:04:05 -0700",
		"2006/01/02 15:04:05",
		"02-01-2006 15:04:05",
		"02/01/2006 15:04:05",
		"1/2/2006 15:04:05 PM",  // Added for format with seconds and AM/PM
		"1/2/2006 3:04:05 PM",    // This should match "2/20/2026 6:00:00 PM"
		"1/2/2006 3:04 PM",
		"1/2/2006 15:04:05",      // 24-hour format without AM/PM
	}

	loc := time.Now().Location()

	for _, layout := range layouts {
		// Try parsing as-is
		t, err := time.Parse(layout, dateStr)
		if err == nil {
			return t, nil
		}
		
		// Try with location
		t, err = time.ParseInLocation(layout, dateStr, loc)
		if err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported date format: %s", dateStr)
}
func isWithinLastDays(input string, days int) (bool, error) {
	if strings.TrimSpace(input) == "" {
		return false, fmt.Errorf("empty input")
	}

	// Remove timezone abbreviations (EAT, EST, etc.) but keep numeric offset
	cleaned := regexp.MustCompile(`\s+[A-Z]{2,4}(\s|$)`).ReplaceAllString(input, " ")
	cleaned = strings.TrimSpace(cleaned)

	dateStr := extractDate(cleaned)
	if dateStr == "" {
		return false, fmt.Errorf("no valid date found in: %s", input)
	}

	parsedTime, err := parseFlexibleDate(dateStr)
	if err != nil {
		return false, err
	}

	cutoff := time.Now().AddDate(0, 0, -days)
	return !parsedTime.Before(cutoff), nil
}
// Add this function in the utils section
// func isWithinLastDays(dateStr string, days int) (bool, error) {
// 	if dateStr == "" {
// 		return false, fmt.Errorf("empty date")
// 	}

// 	// Remove extra timezone names, keep only offset
// 	if strings.Contains(dateStr, "EAT") {
// 		dateStr = strings.ReplaceAll(dateStr, "EAT", "")
// 		dateStr = strings.TrimSpace(dateStr)
// 	}

// 	formats := []string{
// 		"2006/01/02 15:04:05 -0700",     // 2026/02/20 11:46:59 +0300
// 		"02-01-2006 15:04:05",           // 20-02-2026 11:20:27
// 		"1/2/2006, 03:04:05 PM",         // 2/20/2026, 11:52:00 AM
// 		"02/01/2006 15:04:05",           // DD/MM/YYYY HH:MM:SS
// 		"02-01-2006",                     // DD-MM-YYYY
// 		"2006-01-02",                     // YYYY-MM-DD
// 	}

// 	var parsedTime time.Time
// 	var err error

// 	for _, format := range formats {
// 		parsedTime, err = time.Parse(format, dateStr)
// 		if err == nil {
// 			break
// 		}
// 	}

// 	if err != nil {
// 		return false, fmt.Errorf("could not parse date: %s", dateStr)
// 	}

// 	// Cutoff: N days ago
// 	cutoff := time.Now().AddDate(0, 0, -days)

// 	return parsedTime.After(cutoff) || parsedTime.Equal(cutoff), nil
// }
// normalizePersonName already lowercases & trims; this one extracts tokens.
func nameTokens(s string) []string {
	s = strings.ToLower(strings.TrimSpace(s))
	// Build tokens: keep letters/digits, split on anything else
	var tok []string
	var b strings.Builder
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		} else {
			if b.Len() > 0 {
				w := b.String()
				if _, drop := receiverStopwords[w]; !drop && w != "" {
					tok = append(tok, w)
				}
				b.Reset()
			}
		}
	}
	if b.Len() > 0 {
		w := b.String()
		if _, drop := receiverStopwords[w]; !drop && w != "" {
			tok = append(tok, w)
		}
	}
	return tok
}
func isAllowedEBirrURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	return strings.EqualFold(u.Hostname(), EBirrAllowedHost)
}

var (
	reEBirrLink = regexp.MustCompile(`(?i)\bhttps?://transactioninfo\.ebirr\.com/\S+`)
)

func tokenSet(ss []string) map[string]struct{} {
	m := make(map[string]struct{}, len(ss))
	for _, s := range ss {
		m[s] = struct{}{}
	}
	return m
}

// containsAllWords returns true if every token in want appears in have (order-insensitive).
func containsAllWords(have, want map[string]struct{}) bool {
	if len(want) == 0 {
		return false
	}
	for w := range want {
		if _, ok := have[w]; !ok {
			return false
		}
	}
	return true
}

func normalizeKey(s string) string {
	s = strings.TrimSpace(s)
	return strings.ToUpper(s)
}

// ===== Helpers: acceptable EBirr receiver variants =====
func ebirrReceiverVariants() []string {
	base := strings.TrimSpace(AllowedEBirrReceiverName)

	// Build a small allowlist:
	// 1) whatever comes from env (could be either spelling)
	// 2) the other common spelling (Mandefro <-> Mandafro)
	// 3) surname-only fallback (both spellings)
	alts := map[string]struct{}{}

	add := func(s string) {
		s = strings.ToLower(strings.TrimSpace(s))
		if s != "" {
			alts[s] = struct{}{}
		}
	}

	add(base)
	// normalized full-name variants (both spellings)
	add(strings.ReplaceAll(strings.ToLower(base), "mandefro", "mandafro"))
	add(strings.ReplaceAll(strings.ToLower(base), "mandafro", "mandefro"))
	add("henok belay mandefro")
	add("henok belay mandafro")
	// surname-only fallbacks (some slips show only the surname)
	add("mandefro")
	add("mandafro")

	// back to slice
	out := make([]string, 0, len(alts))
	for s := range alts {
		out = append(out, s)
	}
	return out
}

// For friendly error text (proper casing)
func ebirrReceiverVariantsDisplay() []string {
	// Keep a clean, human readable set
	seen := map[string]struct{}{}
	add := func(s string) []string {
		s = strings.TrimSpace(s)
		if s == "" {
			return nil
		}
		if _, ok := seen[s]; ok {
			return nil
		}
		seen[s] = struct{}{}
		return []string{s}
	}
	out := []string{}
	base := AllowedEBirrReceiverName
	out = append(out, add(base)...)
	// the two full-name spellings
	if !strings.EqualFold(base, "Henok Belay Mandefro") {
		out = append(out, "Henok Belay Mandefro")
	}
	if !strings.EqualFold(base, "Henok Belay Mandafro") {
		out = append(out, "Henok Belay Mandafro")
	}
	// surname-only options
	out = append(out, "Mandefro", "Mandafro")
	return out
}

// canonical expected names by method
func apiTransferByIDs(fromTelegramID, toTelegramID int64, amountBirr, target string) error {
	payload := map[string]any{
		"from_telegram_id": fromTelegramID,
		"to_telegram_id":   toTelegramID,
		"amount":           amountBirr,
	}
	t := strings.ToLower(strings.TrimSpace(target))
	if t == "main" || t == "balance" {
		payload["target"] = t
	}
	if err := httpPostJSON(APIBase, "/wallet/transfer", payload, nil); err == nil {
		return nil
	}
	// Fallback (force MAIN on the debit side)
	if derr := apiDebitMain(fromTelegramID, amountBirr, fmt.Sprintf("transfer to %d", toTelegramID)); derr != nil {
		return derr
	}
	if cerr := apiCredit(toTelegramID, amountBirr, fmt.Sprintf("transfer from %d", fromTelegramID)); cerr != nil {
		_ = apiCredit(fromTelegramID, amountBirr, "refund: transfer credit failed")
		return cerr
	}
	return nil
}

// choose username path if present, otherwise ID path
func apiTransferFlexible(fromTelegramID int64, toUsername string, toTelegramID int64, amountBirr, target string) error {
	if strings.TrimSpace(toUsername) != "" {
		return apiTransfer(fromTelegramID, toUsername, amountBirr, target)
	}
	return apiTransferByIDs(fromTelegramID, toTelegramID, amountBirr, target)
}

// in your bot helpers (same place you have apiGetUserByTelegramID, etc.)
type userLite struct {
	TelegramID int64 `json:"telegram_id"`
	HasDeposit bool  `json:"has_deposit"`
}

func apiListAllUsers() ([]userDTO, error) {
	url := strings.TrimRight(APIBase, "/") + "/users"
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("GET /users failed: %s", string(b))
	}
	var out []userDTO
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

// ===== Update your expectedReceiverByMethod() to use the variants for EBirr =====
func expectedReceiverByMethod(method string) []string {
	switch strings.ToLower(strings.TrimSpace(method)) {
	case "telebirr":
		return []string{AllowedTelebirrReceiverName}
	case "cbe", "cbe-birr", "cbe_birr", "cbe birr":
		return []string{AllowedCBEBirrReceiverName}
	case "ebirr", "e-birr", "e_birr", "e birr":
		// accept any of the known EBirr variants
		return ebirrReceiverVariants()
	default:
		// conservative fallback: allow match to any known expected name
		return append(
			[]string{AllowedTelebirrReceiverName, AllowedCBEBirrReceiverName},
			ebirrReceiverVariants()...,
		)
	}
}

// isAllowedReceiverFor applies the subset-of-words rule per method.
func isAllowedReceiverFor(method, name string) bool {
	have := tokenSet(nameTokens(name))
	if len(have) == 0 {
		return false
	}
	for _, exp := range expectedReceiverByMethod(method) {
		want := tokenSet(nameTokens(exp))
		if containsAllWords(have, want) {
			return true
		}
	}
	return false
}

func alreadyProcessed(txid, ref string) bool {
	now := time.Now()
	purgeBefore := now.Add(-seenTTL)

	seenMu.Lock()
	for k, t := range seenTx {
		if t.Before(purgeBefore) {
			delete(seenTx, k)
		}
	}
	for k, t := range seenRef {
		if t.Before(purgeBefore) {
			delete(seenRef, k)
		}
	}
	txid = normalizeKey(txid)
	ref = normalizeKey(ref)
	if txid != "" {
		if t, ok := seenTx[txid]; ok && now.Sub(t) <= seenTTL {
			seenMu.Unlock()
			return true
		}
	}
	if ref != "" {
		if t, ok := seenRef[ref]; ok && now.Sub(t) <= seenTTL {
			seenMu.Unlock()
			return true
		}
	}
	seenMu.Unlock()
	return false
}

func markProcessed(txid, ref string) {
	now := time.Now()
	seenMu.Lock()
	if txid = normalizeKey(txid); txid != "" {
		seenTx[txid] = now
	}
	if ref = normalizeKey(ref); ref != "" {
		seenRef[ref] = now
	}
	seenMu.Unlock()
}

/* ===================== HTTP helpers ===================== */
var (
	CBEMaxRetries   = 3
	CBEBackoffStart = 600 * time.Millisecond
	CBEHTTPTimeout  = 25 * time.Second
)

var (
	APIHTTPTimeout  = 25 * time.Second // per-attempt timeout for your own API
	APIMaxRetries   = 3
	APIBackoffStart = 500 * time.Millisecond
)

func newHTTPClient(timeout time.Duration) *http.Client {
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 20 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		IdleConnTimeout:       30 * time.Second,
		MaxIdleConns:          100,
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}
	return &http.Client{Timeout: timeout, Transport: tr}
}

// build a tuned HTTP client; we keep this scoped so it doesn't change other calls

func isRetryableStatus(code int) bool { return code == 429 || code >= 500 }
func newCBEHTTPClient(timeout time.Duration, insecure bool) *http.Client {
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   15 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:   15 * time.Second,
		ExpectContinueTimeout: 2 * time.Second,
		ForceAttemptHTTP2:     true,
		TLSClientConfig: &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: insecure,
		},
	}
	return &http.Client{Transport: tr, Timeout: timeout}
}

func isUnknownAuthorityErr(err error) bool {
	if err == nil {
		return false
	}
	var uae x509.UnknownAuthorityError
	if errors.As(err, &uae) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "x509: certificate signed by unknown authority") ||
		strings.Contains(msg, "unknown ca") ||
		strings.Contains(msg, "certificate verify failed")
}

// *** GUARANTEE insecure retry for CBE host (no env required) ***
func cbeAllowInsecureForHost(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	h := strings.ToLower(u.Host)
	return h == "apps.cbe.com.et" || h == "apps.cbe.com.et:100" || h == "mbreciept.cbe.com.et"
}

// Optional: still allow a global switch too (CBE_ALLOW_INSECURE=1)
func cbeAllowInsecureEnv() bool { return os.Getenv("CBE_ALLOW_INSECURE") == "1" }

// Structure compatible with your TelebirrReceipt usage
// (re-uses the same DTO so downstream code stays identical)

// Grab text from the nearest block that clearly looks like a section header.
// We search for an element that contains one of the keywords, then take the text of
// either that node's parent table, or the node plus a few siblings as fallback.
func sectionText(doc *goquery.Document, keys ...string) string {
	lowerKeys := make([]string, 0, len(keys))
	for _, k := range keys {
		k = strings.ToLower(strings.TrimSpace(k))
		if k != "" {
			lowerKeys = append(lowerKeys, k)
		}
	}
	var best string
	doc.Find("h1,h2,h3,th,td,div,span,p").EachWithBreak(func(_ int, el *goquery.Selection) bool {
		t := strings.ToLower(cleanVal(el.Text()))
		hit := false
		for _, k := range lowerKeys {
			if strings.Contains(t, k) {
				hit = true
				break
			}
		}
		if !hit {
			return true
		}

		// Prefer a surrounding table row/section if available.
		if par := el.ParentsFiltered("table,tbody,thead,tfoot"); par.Length() > 0 {
			best = cleanVal(par.Text())
			return false
		}

		// Fallback: take el + immediate siblings (often used in these receipts)
		sb := &strings.Builder{}
		sb.WriteString(cleanVal(el.Text()))
		// up to a handful of siblings
		nxt := el
		for i := 0; i < 8; i++ {
			nxt = nxt.Next()
			if nxt.Length() == 0 {
				break
			}
			sb.WriteRune('\n')
			sb.WriteString(cleanVal(nxt.Text()))
		}
		best = sb.String()
		return false
	})
	return best
}

// Find "Receiver name: <value>" in a given blob; stop before the next label.
func findNameInBlob(blob string, primaryLabelRE, stopAfterRE *regexp.Regexp) string {
	if blob == "" {
		return ""
	}
	// Non-greedy up to a stopper (Receiver Account / Transaction Status / next label)
	// Example matches both: "Receiver name henok belay mandefro  Receiver Account ..."
	if m := primaryLabelRE.FindStringSubmatch(blob); len(m) >= 2 {
		val := cleanVal(m[1])
		if stopAfterRE != nil {
			// Trim at the first stopper if present
			loc := stopAfterRE.FindStringIndex(val)
			if loc != nil && loc[0] > 0 {
				val = strings.TrimSpace(val[:loc[0]])
			}
		}
		return val
	}
	return ""
}

// ====================== EBirr helpers (safe to paste above the parser) ======================

// returns lowercased/trimmed
func low(s string) string { return strings.ToLower(strings.TrimSpace(s)) }

// Map a header to the value directly *under* that header in the next non-empty row.
// This fixes tables where a header row contains "Receipt No | Payment date | Settled Amount"
// and the next row contains the values aligned by column.
func valueUnderHeader(doc *goquery.Document, headerKeys ...string) string {
	if len(headerKeys) == 0 {
		return ""
	}
	keys := make([]string, 0, len(headerKeys))
	for _, k := range headerKeys {
		k = low(k)
		if k != "" {
			keys = append(keys, k)
		}
	}
	var out string

	// scan tables
	doc.Find("table").EachWithBreak(func(_ int, table *goquery.Selection) bool {
		// find a row that looks like a header row containing any of the keys
		table.Find("tr").EachWithBreak(func(_ int, tr *goquery.Selection) bool {
			heads := tr.ChildrenFiltered("th,td")
			if heads.Length() == 0 {
				return true
			}

			// locate the header index for our key
			hit := -1
			for j := 0; j < heads.Length(); j++ {
				h := low(cleanVal(heads.Eq(j).Text()))
				for _, k := range keys {
					if k != "" && strings.Contains(h, k) {
						hit = j
						break
					}
				}
				if hit >= 0 {
					break
				}
			}
			if hit < 0 {
				return true
			}

			// take the next non-empty row and the cell at the same column index
			nxt := tr.Next()
			for nxt.Length() > 0 && low(nxt.Text()) == "" {
				nxt = nxt.Next()
			}
			if nxt.Length() == 0 {
				return true
			}

			cols := nxt.ChildrenFiltered("td,th")
			if hit >= 0 && hit < cols.Length() {
				out = cleanVal(cols.Eq(hit).Text())
				return false
			}
			return true
		})
		return out == ""
	})
	return out
}

// Strict amount parser for EBirr.
// 1) value under "Settled Amount" header
// 2) value under "Total Amount Paid" header
// 3) top-right "AMOUNT: ETB 4,900" summary
// 4) guarded labeled fallback inside details table
func findEBirrAmountStrict(doc *goquery.Document) string {
	// 1) "Settled Amount"
	if v := valueUnderHeader(doc, "Settled Amount", "የተመረጠ መጠን", "የክፍያ መጠን"); v != "" {
		if m := regexp.MustCompile(`(?i)(?:etb|birr)\s*([0-9][0-9,]*\.?\d{0,2})`).FindStringSubmatch(v); len(m) >= 2 {
			return strings.ReplaceAll(m[1], ",", "")
		}
		if m := regexp.MustCompile(`\b([0-9][0-9,]*\.?\d{0,2})\b`).FindStringSubmatch(v); len(m) >= 2 {
			return strings.ReplaceAll(m[1], ",", "")
		}
	}

	// 2) "Total Amount Paid"
	if v := valueUnderHeader(doc, "Total Amount Paid", "ጠቅላላ መጠን"); v != "" {
		if m := regexp.MustCompile(`(?i)(?:etb|birr)\s*([0-9][0-9,]*\.?\d{0,2})`).FindStringSubmatch(v); len(m) >= 2 {
			return strings.ReplaceAll(m[1], ",", "")
		}
		if m := regexp.MustCompile(`\b([0-9][0-9,]*\.?\d{0,2})\b`).FindStringSubmatch(v); len(m) >= 2 {
			return strings.ReplaceAll(m[1], ",", "")
		}
	}

	// 3) top-right summary "AMOUNT: ETB 4900"
	{
		re := regexp.MustCompile(`(?i)\bamount\s*[:\-]\s*(?:etb|birr)\s*([0-9][0-9,]*\.?\d{0,2})\b`)
		val := ""
		doc.Find("div,span,td,th,p").EachWithBreak(func(_ int, el *goquery.Selection) bool {
			if m := re.FindStringSubmatch(cleanVal(el.Text())); len(m) >= 2 {
				val = strings.ReplaceAll(m[1], ",", "")
				return false
			}
			return true
		})
		if val != "" {
			return val
		}
	}

	// 4) guarded label fallback — only accept labels that explicitly say amount
	if v := extractValueByLabel(doc, "መጠን", "amount", "settled amount", "total amount paid"); v != "" {
		v = strings.TrimSpace(strings.ReplaceAll(v, ",", ""))
		// require either currency prefix or a plain number by itself
		if m := regexp.MustCompile(`(?i)^(?:etb|birr)?\s*([0-9]+(?:\.[0-9]{1,2})?)$`).FindStringSubmatch(v); len(m) >= 2 {
			return m[1]
		}
	}
	return ""
}

// Extracts a clean person name from a region of text by label (e.g. "Receiver name")
func findNameInRegion(regionText string, labelKeys ...string) string {
	rt := strings.ReplaceAll(regionText, "\r", "\n")
	rt = regexp.MustCompile(`[ \t]+`).ReplaceAllString(rt, " ")
	// capture the first non-empty line after the label
	lbl := strings.Join(labelKeys, "|")
	re := regexp.MustCompile(`(?i)(?:` + lbl + `)\s*[:：]?\s*([^\n]+)`)
	if m := re.FindStringSubmatch(rt); len(m) >= 2 {
		s := cleanVal(m[1])
		// stop at first digit or leaking label text
		s = regexp.MustCompile(`(?i)\b(sender|receiver|name|payer|payee|account|mobile|no\.?|number|status)\b.*$`).ReplaceAllString(s, "")
		if i := strings.IndexFunc(s, unicode.IsDigit); i > 0 {
			s = strings.TrimSpace(s[:i])
		}
		return strings.TrimSpace(s)
	}
	return ""
}

// Returns the slice of doc.Text() between any of the start markers and any of the end markers.
func textBetween(doc *goquery.Document, starts []string, ends []string) string {
	all := doc.Text()
	lowAll := strings.ToLower(all)
	startIdx := -1
	for _, s := range starts {
		if i := strings.Index(lowAll, strings.ToLower(s)); i >= 0 {
			startIdx = i
			break
		}
	}
	if startIdx < 0 {
		return ""
	}
	after := lowAll[startIdx:]
	endIdx := len(after)
	for _, e := range ends {
		if i := strings.Index(after, strings.ToLower(e)); i >= 0 && i < endIdx {
			endIdx = i
		}
	}
	// map back to original case with same boundaries
	return all[startIdx : startIdx+endIdx]
}

// ============================ UPDATED PARSER ============================
// locate the transaction details grid and return values under specific headers
func ebirrGrid(doc *goquery.Document) (receiptNo, payDate, settled string) {
	// find a row that contains all 3 headers (in any order)
	doc.Find("table").EachWithBreak(func(_ int, table *goquery.Selection) bool {
		var hdr *goquery.Selection
		table.Find("tr").EachWithBreak(func(_ int, tr *goquery.Selection) bool {
			cells := tr.ChildrenFiltered("th,td")
			if cells.Length() < 3 {
				return true
			}
			have := make(map[string]int)
			for i := 0; i < cells.Length(); i++ {
				t := strings.ToLower(cleanVal(cells.Eq(i).Text()))
				switch {
				case strings.Contains(t, "receipt no"):
					have["r"] = i
				case strings.Contains(t, "payment date"):
					have["d"] = i
				case strings.Contains(t, "settled amount"):
					have["a"] = i
				}
			}
			if len(have) == 3 {
				hdr = tr
				return false
			}
			return true
		})
		if hdr == nil {
			return true
		}

		// get next non-empty row
		row := hdr.Next()
		for row.Length() > 0 && strings.TrimSpace(row.Text()) == "" {
			row = row.Next()
		}
		if row.Length() == 0 {
			return true
		}

		vals := row.ChildrenFiltered("td,th")
		// pull by the same column index
		hdrCells := hdr.ChildrenFiltered("th,td")
		for i := 0; i < hdrCells.Length() && i < vals.Length(); i++ {
			ht := strings.ToLower(cleanVal(hdrCells.Eq(i).Text()))
			v := cleanVal(vals.Eq(i).Text())
			switch {
			case strings.Contains(ht, "receipt no"):
				receiptNo = v
			case strings.Contains(ht, "payment date"):
				payDate = v
			case strings.Contains(ht, "settled amount"):
				// keep only the numeric amount part
				if m := regexp.MustCompile(`(?i)(?:etb|birr)?\s*([0-9][0-9,]*\.?\d{0,2})`).FindStringSubmatch(v); len(m) >= 2 {
					settled = strings.ReplaceAll(m[1], ",", "")
				} else {
					settled = strings.ReplaceAll(v, ",", "")
				}
			}
		}
		return false
	})
	return
}
func sectionTextBetween(all string, startKeys []string, endKeys []string) string {
	lowAll := strings.ToLower(all)
	sIdx := -1
	for _, k := range startKeys {
		if i := strings.Index(lowAll, strings.ToLower(k)); i >= 0 {
			sIdx = i
			break
		}
	}
	if sIdx < 0 {
		return ""
	}
	after := lowAll[sIdx:]
	eIdx := len(after)
	for _, k := range endKeys {
		if i := strings.Index(after, strings.ToLower(k)); i >= 0 && i < eIdx {
			eIdx = i
		}
	}
	return all[sIdx : sIdx+eIdx]
}

func nameAfterLabel(text string, labels ...string) string {
	t := regexp.MustCompile(`[ \t]+`).ReplaceAllString(text, " ")
	re := regexp.MustCompile(`(?i)(?:` + strings.Join(labels, "|") + `)\s*[:：]?\s*([^\n]+)`)
	if m := re.FindStringSubmatch(t); len(m) >= 2 {
		s := cleanVal(m[1])
		// trim trailing label fragments or numbers
		s = regexp.MustCompile(`(?i)\b(sender|receiver|name|account|mobile|status)\b.*$`).ReplaceAllString(s, "")
		if i := strings.IndexFunc(s, unicode.IsDigit); i > 0 {
			s = strings.TrimSpace(s[:i])
		}
		return s
	}
	return ""
}
func digits(s string) string { return regexp.MustCompile(`\D+`).ReplaceAllString(s, "") }
func last9(s string) string {
	d := digits(s)
	if len(d) > 9 {
		return d[len(d)-9:]
	}
	return d
}
func lev1(a, b string) bool {
	// quick allow: equal or one substitution/insertion/deletion
	if a == b {
		return true
	}
	// substitution
	if len(a) == len(b) {
		diff := 0
		for i := range a {
			if a[i] != b[i] {
				diff++
				if diff > 1 {
					return false
				}
			}
		}
		return diff == 1
	}
	// insertion/deletion
	if abs(len(a)-len(b)) == 1 {
		i, j, diff := 0, 0, 0
		for i < len(a) && j < len(b) {
			if a[i] == b[j] {
				i++
				j++
				continue
			}
			diff++
			if diff > 1 {
				return false
			}
			if len(a) > len(b) {
				i++
			} else {
				j++
			}
		}
		return true
	}
	return false
}
func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

// Fuzzy token check with ≤1 edit per token
func fuzzyHasAllTokens(have, want string) bool {
	hTok := nameTokens(have) // you already have this
	wTok := nameTokens(want)
	for _, w := range wTok {
		ok := false
		for _, h := range hTok {
			if h == w || lev1(h, w) {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	return true
}

func isAllowedEBirrReceiver(name, account string) bool {
	// 1) phone/account wins
	exp := []string{"0915418674", "251915418674"} // normalize your expected numbers here
	acc9 := last9(account)
	for _, e := range exp {
		if acc9 != "" && acc9 == last9(e) {
			return true
		}
	}
	// 2) fuzzy name (handles “Mandafro” vs “Mandefro”)
	return fuzzyHasAllTokens(name, AllowedEBirrReceiverName)
}

// parseEBirrReceipt extracts fields from an EBirr receipt page.
// It is robust to mixed Amharic/English labels and avoids pulling values from the wrong section.
// parseEBirrReceipt extracts fields from an EBirr receipt page.
//
// Key fixes vs. earlier versions:
// - Only match labels that include "name/ስም" (avoids grabbing "Receiver Info").
// - Bound name capture up to the next known label (Account/Mobile, Status, etc.).
// - Grid-read "Transaction details" for Invoice/Date/Settled Amount (numeric).
func parseEBirrReceipt(rawURL string) (*TelebirrReceipt, error) {
	if !isAllowedEBirrURL(rawURL) {
		return nil, fmt.Errorf("receipt host not allowed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), HTTPTimeout)
	defer cancel()

	doc, err := httpGetHTML(ctx, rawURL)
	if err != nil {
		log.Printf("EBirr fetch failed: %v", err)
		return nil, fmt.Errorf("failed to fetch HTML: %w", err)
	}

	r := &TelebirrReceipt{Source: "receipt", Link: rawURL}

	// --- debug (local) ---
	debug := map[string]string{"link": rawURL}
	if html, _ := doc.Html(); html != "" {
		if err := os.WriteFile("ebirr_receipt.html", []byte(html), 0644); err == nil {
			log.Printf("Saved raw HTML to ebirr_receipt.html")
		}
		if len(html) > 1200 {
			debug["raw_html_sample"] = html[:1200] + "..."
		} else {
			debug["raw_html_sample"] = html
		}
	}

	/* ================= helpers ================= */

	// Grid reader for: Receipt No | Payment date | Settled Amount
	ebirrGrid := func(doc *goquery.Document) (receiptNo, payDate, settled string) {
		doc.Find("table").EachWithBreak(func(_ int, table *goquery.Selection) bool {
			var hdr *goquery.Selection
			table.Find("tr").EachWithBreak(func(_ int, tr *goquery.Selection) bool {
				cells := tr.ChildrenFiltered("th,td")
				if cells.Length() < 3 {
					return true
				}
				ri, di, ai := -1, -1, -1
				for i := 0; i < cells.Length(); i++ {
					t := strings.ToLower(cleanVal(cells.Eq(i).Text()))
					switch {
					case strings.Contains(t, "receipt no"):
						ri = i
					case strings.Contains(t, "payment date"):
						di = i
					case strings.Contains(t, "settled amount"):
						ai = i
					}
				}
				if ri >= 0 && di >= 0 && ai >= 0 {
					hdr = tr
					return false
				}
				return true
			})
			if hdr == nil {
				return true
			}

			// next non-empty row
			row := hdr.Next()
			for row.Length() > 0 && strings.TrimSpace(row.Text()) == "" {
				row = row.Next()
			}
			if row.Length() == 0 {
				return true
			}

			hc := hdr.ChildrenFiltered("th,td")
			vc := row.ChildrenFiltered("td,th")
			for i := 0; i < hc.Length() && i < vc.Length(); i++ {
				ht := strings.ToLower(cleanVal(hc.Eq(i).Text()))
				val := cleanVal(vc.Eq(i).Text())
				switch {
				case strings.Contains(ht, "receipt no"):
					receiptNo = val
				case strings.Contains(ht, "payment date"):
					payDate = val
				case strings.Contains(ht, "settled amount"):
					if m := reAmtCell.FindStringSubmatch(strings.ToLower(val)); len(m) >= 2 {
						settled = strings.ReplaceAll(m[1], ",", "")
					} else {
						settled = normalizeBirrAmount(val)
					}
				}
			}
			return false
		})
		return
	}

	// Extracts a name right after a specific "name" label (Amharic/English),
	// and stops before any common stopper (Account/Mobile/Status/etc.).
	extractBoundedName := func(all string, labelRE, stopRE *regexp.Regexp) string {
		if all == "" {
			return ""
		}
		m := labelRE.FindStringSubmatchIndex(all)
		if m == nil {
			return ""
		}
		// capture starts where the label ends
		start := m[1]
		rest := all[start:]
		// find first stopper
		end := len(rest)
		if loc := stopRE.FindStringIndex(rest); loc != nil {
			end = loc[0]
		}
		name := cleanVal(rest[:end])

		// heuristics: cut before digits (phone) and remove trailing label echoes
		if i := strings.IndexFunc(name, unicode.IsDigit); i > 0 {
			name = strings.TrimSpace(name[:i])
		}
		// Remove obvious label fragments that might get stuck at the end
		name = regexp.MustCompile(`(?i)\b(account|mobile|status|transaction|receiver|sender|name)\b.*$`).ReplaceAllString(name, "")
		name = cleanVal(name)

		// throw away useless captures like "Info"
		low := strings.ToLower(name)
		if low == "info" || low == "information" || len(low) < 2 {
			return ""
		}
		return name
	}

	// Build regexes for labels and stoppers (case-insensitive, bilingual).
	receiverNameLabel := regexp.MustCompile(`(?i)(የተቀባይ\s*ስም\s*(?:/\s*receiver\s*name)?|receiver\s*name)\s*[:：]?\s*`)
	senderNameLabel := regexp.MustCompile(`(?i)(የከፋይ\s*ስም\s*(?:/\s*sender\s*name)?|sender\s*name|payer\s*name)\s*[:：]?\s*`)

	// things that end the name capture
	stopper := regexp.MustCompile(`(?i)(ተቀባይ\s*ሞባይል|receiver\s*account|account\s*/\s*mobile|account|mobile|transaction\s*status|status|transaction|details|payment\s*date|receipt\s*no)`)

	/* ================= extraction ================= */

	// 1) Grid (authoritative for id/date/amount)
	inv, dt, amt := ebirrGrid(doc)
	if inv == "" {
		// fallback: last path segment
		if u, e := url.Parse(rawURL); e == nil {
			parts := strings.Split(strings.Trim(u.Path, "/"), "/")
			if len(parts) > 0 {
				inv = strings.TrimSpace(parts[len(parts)-1])
			}
		}
	}
	if dt == "" {
		dt = findDateInText(doc.Text())
	}
	if amt == "" {
		amt = findAmountInText(doc.Text())
	}

	r.InvoiceNo = strings.TrimSpace(inv)
	r.PaymentDate = strings.TrimSpace(dt)
	r.SettledAmount = strings.ReplaceAll(strings.TrimSpace(amt), ",", "")

	debug["invoice.extracted"] = r.InvoiceNo
	debug["date.extracted"] = r.PaymentDate
	debug["amount.extracted"] = r.SettledAmount

	// 2) Names from the whole text, but only after "... Name/ስም"
	full := doc.Text()
	r.CreditedPartyName = extractBoundedName(full, receiverNameLabel, stopper)
	r.PayerName = extractBoundedName(full, senderNameLabel, stopper)

	// 3) Receiver Account/Mobile (for logs / optional cross-check)
	recvAcct := ""
	{
		reAcct := regexp.MustCompile(`(?i)(Receiver\s*Account\/Mobile|Receiver\s*Account|Mobile|ሞባይል)\s*[:：]?\s*([0-9 +\-]+)`)
		if m := reAcct.FindStringSubmatch(full); len(m) >= 3 {
			recvAcct = cleanVal(m[2])
		}
	}

	debug["receiver.extracted"] = r.CreditedPartyName
	debug["sender.extracted"] = r.PayerName
	if recvAcct != "" {
		debug["receiver.account"] = recvAcct
	}

	/* ================= sanitize ================= */

	// Ensure amount is plain numeric
	if r.SettledAmount != "" {
		if m := reAmtCell.FindStringSubmatch(strings.ToLower(r.SettledAmount)); len(m) >= 2 {
			r.SettledAmount = strings.ReplaceAll(m[1], ",", "")
		} else {
			r.SettledAmount = normalizeBirrAmount(r.SettledAmount)
		}
	}

	/* ================= validate & log ================= */

	if strings.TrimSpace(r.InvoiceNo) == "" || strings.TrimSpace(r.SettledAmount) == "" || strings.TrimSpace(r.CreditedPartyName) == "" {
		log.Printf("EBirr parse missing fields: inv=%q amt=%q recv=%q", r.InvoiceNo, r.SettledAmount, r.CreditedPartyName)
		log.Printf("EBirr debug: %+v", debug)
		return nil, fmt.Errorf("missing required fields in EBirr receipt")
	}

	log.Printf("Parsed EBirr OK: inv=%s amt=%s sender=%q receiver=%q date=%q acct=%q link=%s",
		r.InvoiceNo, r.SettledAmount, r.PayerName, r.CreditedPartyName, r.PaymentDate, recvAcct, rawURL)

	return r, nil
}

func isTimeoutOrTemp(err error) bool {
	if ne, ok := err.(net.Error); ok && (ne.Timeout() || ne.Temporary()) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "timeout") ||
		strings.Contains(strings.ToLower(err.Error()), "deadline exceeded") ||
		strings.Contains(strings.ToLower(err.Error()), "connection reset") ||
		strings.Contains(strings.ToLower(err.Error()), "connection refused")
}

func jitter(d time.Duration) time.Duration {
	// ±25% jitter
	delta := time.Duration(rand.Int63n(int64(d/2))) - d/4
	return d + delta
}

type createTxnReq struct {
	Reference    string `json:"reference"`
	TelegramID   int64  `json:"telegram_id"`
	Type         string `json:"type"`
	Amount       string `json:"amount"`
	Bonus        string `json:"bonus"`
	Total        string `json:"total"`
	Method       string `json:"method"`
	Account      string `json:"account"`
	Status       string `json:"status"`
	TxID         string `json:"txid"`
	Note         string `json:"note"`
	SenderName   string `json:"sender_name"`
	ReceiverName string `json:"receiver_name"`
	ReceiptURL   string `json:"receipt_url"`
	PaymentDate  string `json:"payment_date"`
}

var (
	ErrDuplicateTxID = fmt.Errorf("duplicate_txid")
	ErrDuplicateRef  = fmt.Errorf("duplicate_reference")
)

type createTxnAPIResp struct {
	OK     bool   `json:"ok"`
	Error  string `json:"error"`
	Field  string `json:"field"`
	Detail string `json:"detail"`
}

func isUserAdmin(telegramID int64) bool {
	dto, err := apiGetUserByTelegramID(telegramID)
	if err != nil {
		log.Printf("isUserAdmin: %v", err)
		return false
	}
	return dto.IsAdmin
}

func denyIfNotAdmin(bot *tgbotapi.BotAPI, chatID, telegramID int64) bool {
	if isUserAdmin(telegramID) {
		return false // allowed
	}
	msg := tgbotapi.NewMessage(chatID, "❌ Only admins can use the transfer feature.")
	bot.Send(msg)
	return true
}

// Optional: role-aware main menu (shows Transfer only for admins)
func sendRoleAwareMenu(bot *tgbotapi.BotAPI, chatID, telegramID int64, miniAppURL string) {
	// clear any reply keyboard
	clear := tgbotapi.NewMessage(chatID, " ")
	clear.ReplyMarkup = tgbotapi.NewRemoveKeyboard(true)
	_, _ = bot.Send(clear)

	btnRows := [][]tgbotapi.InlineKeyboardButton{
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("💰 Deposit", "dep:manual"),
			tgbotapi.NewInlineKeyboardButtonData("💳 Balance", "act:refresh_balance"),
		),
	}

	if isUserAdmin(telegramID) {
		btnRows = append(btnRows,
			tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonData("🔁 Transfer", "act:transfer"),
				tgbotapi.NewInlineKeyboardButtonData("🎁 Bonus", "act:bonus"), // 👈 NEW
			),
		)
	}

	m := tgbotapi.NewMessage(chatID, "Choose an option:")
	m.ReplyMarkup = tgbotapi.NewInlineKeyboardMarkup(btnRows...)
	bot.Send(m)
}

func apiCreateTransaction(req createTxnReq) error {
	var apiResp createTxnAPIResp
	if err := httpPostJSON(APIBase, "/transactions", req, &apiResp); err != nil {
		if isDuplicateTxIDErr(err) {
			return ErrDuplicateTxID
		}
		if isDuplicateRefErr(err) {
			return ErrDuplicateRef
		}
		return err
	}
	if strings.EqualFold(apiResp.Error, "duplicate") {
		switch strings.ToLower(apiResp.Field) {
		case "reference":
			return ErrDuplicateRef
		default:
			return ErrDuplicateTxID
		}
	}
	if !apiResp.OK && strings.EqualFold(apiResp.Error, "duplicate") {
		return ErrDuplicateTxID
	}
	return nil
}

func joinURL(base, p string) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	u.Path = path.Join(strings.TrimSuffix(u.Path, "/"), p)
	return u.String(), nil
}

func expectJSON(resp *http.Response) error {
	ct := resp.Header.Get("Content-Type")
	mt, _, _ := mime.ParseMediaType(ct)
	if mt == "application/json" || mt == "application/problem+json" {
		return nil
	}
	preview, _ := io.ReadAll(io.LimitReader(resp.Body, 600))
	return fmt.Errorf(`expected JSON but got %q (%s). Preview: %q`, ct, resp.Status, string(preview))
}

func apiGetUserByTelegramID(tid int64) (*userDTO, error) {
	var out userDTO
	if err := httpGetJSON(APIBase, fmt.Sprintf("/users/by-telegram/%d", tid), &out); err != nil {
		return nil, err
	}
	return &out, nil
}

type mintTokenResp struct {
	Token string `json:"token"`
}

func apiMintToken(telegramID int64) (string, error) {
	var out mintTokenResp
	if err := httpPostJSON(APIBase, "/telegram/users/mint-token",
		map[string]any{"telegram_id": telegramID}, &out); err != nil {
		return "", err
	}
	return out.Token, nil
}

// helpers/tls_cbe.go
func cbeAllowInsecure() bool {
	v := strings.TrimSpace(os.Getenv("CBE_TLS_INSECURE"))
	return strings.EqualFold(v, "true") || v == "1" || strings.EqualFold(v, "yes")
}

// REPLACE the old 1-arg version with this 2-arg version:

// Add near your config block:
var CBETLSInsecure = strings.EqualFold(getenvDefault("CBE_TLS_INSECURE", "false"), "true")

// labels that indicate the start of a new field — use to “hard stop” the current capture
var cbeStopLabelRE = regexp.MustCompile(`(?i)\b(
	Payer|
	Receiver|
	Receiver\s*Account|
	Account|
	Reason|
	Type\s*of\s*service|
	Payment\s*Date(?:\s*&\s*Time)?|
	Reference(?:\s*No\.?(?:\s*\(VAT Invoice No\))?)?|
	Transferred\s*Amount|
	Commission|
	VAT|
	Total\s*amount|
	Amount\s*in\s*Word
)\b`)

// return only the part of s before the next known label
func cutAtNextLabel(s string) string {
	idx := cbeStopLabelRE.FindStringIndex(s)
	if idx == nil {
		return strings.TrimSpace(s)
	}
	return strings.TrimSpace(s[:idx[0]])
}

// strict FT extractor (from a blob)
func extractFTID(s string) string {
	m := reFTID.FindString(s)
	return strings.ToUpper(strings.TrimSpace(m))
}

// normalize/clean name lines (your earlier trim + a bit more)
func normalizeCBEName(s string) string {
	s = cbeTrimName(s)
	// avoid trailing label spill
	s = cutAtNextLabel(s)
	// compress spaces
	s = regexp.MustCompile(`\s+`).ReplaceAllString(s, " ")
	return s
}

// ensure money looks like “1234.56” or “123.00”
func normalizeBirr(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	// strip unit tokens
	s = strings.ReplaceAll(s, "etb", "")
	s = strings.ReplaceAll(s, "birr", "")
	s = strings.ReplaceAll(s, "br", "")
	s = strings.TrimSpace(strings.ReplaceAll(s, ",", ""))
	// leave as-is if already 0/decimalish
	return s
}
func sanitizeCBEReceipt(r *CBEReceipt) {
	// TXID: pull only true FT… token (some HTML rows contain the entire line)
	if r.TxID == "" {
		// try from other blobs if missing
		r.TxID = extractFTID(strings.Join([]string{
			r.TxID, r.Reason, r.PaymentDate, r.PayerAccount, r.ReceiverAccount,
		}, " "))
	} else {
		r.TxID = extractFTID(r.TxID)
	}

	// Names: remove any label spill / junk tails
	r.PayerName = normalizeCBEName(r.PayerName)
	r.ReceiverName = normalizeCBEName(r.ReceiverName)

	// Accounts: keep only the first token on the line (account numbers often trail labels)
	if i := strings.IndexRune(r.PayerAccount, ' '); i > 0 {
		r.PayerAccount = strings.TrimSpace(r.PayerAccount[:i])
	}
	if i := strings.IndexRune(r.ReceiverAccount, ' '); i > 0 {
		r.ReceiverAccount = strings.TrimSpace(r.ReceiverAccount[:i])
	}

	// Reason/date: cut at next label just in case
	r.Reason = cutAtNextLabel(r.Reason)
	r.PaymentDate = cutAtNextLabel(r.PaymentDate)

	// Amounts
	r.TransferredAmount = normalizeBirr(r.TransferredAmount)
	r.TotalDebited = normalizeBirr(r.TotalDebited)
	r.Commission = normalizeBirr(r.Commission)
	r.VAT = normalizeBirr(r.VAT)
}

func fetchCBE(ctx context.Context, rawURL string) (doc *goquery.Document, pdfText string, err error) {
	var lastErr error
	backoff := CBEBackoffStart

	// inner attempt (strict or insecure)
	try := func(insecure bool) (doc *goquery.Document, pdfText string, err error) {
		for attempt := 1; attempt <= CBEMaxRetries; attempt++ {
			attemptCtx, cancel := context.WithTimeout(ctx, CBEHTTPTimeout)

			req, reqErr := http.NewRequestWithContext(attemptCtx, http.MethodGet, rawURL, nil)
			if reqErr != nil {
				cancel()
				return nil, "", reqErr
			}
			req.Header.Set("User-Agent", UserAgent)
			req.Header.Set("Accept", "text/html,application/xhtml+xml;q=0.9,application/pdf;q=0.8,*/*;q=0.7")
			req.Header.Set("Accept-Language", "am-ET, en-US;q=0.7")

			if insecure {
				log.Printf("[CBE] retrying with insecure TLS (verify skipped) for %s", rawURL)
			}
			client := newCBEHTTPClient(CBEHTTPTimeout, insecure)

			resp, doErr := client.Do(req)
			if doErr != nil {
				lastErr = doErr
				cancel()
				// backoff only on timeouts/temporary network errors
				if attempt < CBEMaxRetries && isTimeoutOrTemp(doErr) {
					time.Sleep(jitter(backoff))
					backoff *= 2
					continue
				}
				return nil, "", doErr
			}

			body, _ := io.ReadAll(io.LimitReader(resp.Body, 32<<20)) // cap 32MB
			resp.Body.Close()
			cancel()

			if isRetryableStatus(resp.StatusCode) && attempt < CBEMaxRetries {
				lastErr = fmt.Errorf("server %s", resp.Status)
				time.Sleep(jitter(backoff))
				backoff *= 2
				continue
			}
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				return nil, "", fmt.Errorf("GET %s => %s", rawURL, resp.Status)
			}

			ct := strings.ToLower(resp.Header.Get("Content-Type"))

			// PDF?
			if strings.HasPrefix(ct, "application/pdf") || looksLikePDF(body) {
				txt, e := extractTextFromPDFBytes(body)
				if e != nil {
					// if mislabelled HTML, fall back to HTML parse
					if errors.Is(e, ErrNotPDF) || looksLikeHTML(body) {
						d, e2 := goquery.NewDocumentFromReader(bytes.NewReader(body))
						if e2 == nil {
							return d, "", nil
						}
						return nil, "", e
					}
					return nil, "", e
				}
				return nil, txt, nil
			}

			// HTML?
			if strings.HasPrefix(ct, "text/html") ||
				strings.HasPrefix(ct, "application/xhtml+xml") ||
				looksLikeHTML(body) {
				d, e := goquery.NewDocumentFromReader(bytes.NewReader(body))
				if e != nil {
					return nil, "", e
				}
				return d, "", nil
			}

			// Unknown type
			return nil, "", fmt.Errorf("unsupported content-type %q from host", ct)
		}

		if lastErr == nil {
			lastErr = fmt.Errorf("failed to fetch CBE receipt")
		}
		return nil, "", lastErr
	}

	// 1) strict attempt
	d, t, e := try(false)
	if e == nil {
		return d, t, nil
	}

	// 2) if the error is unknown-authority for the CBE host, force insecure retry
	if isUnknownAuthorityErr(e) && cbeAllowInsecureForHost(rawURL) {
		return try(true)
	}

	// 3) optional global env switch: CBE_ALLOW_INSECURE=1
	if isUnknownAuthorityErr(e) && cbeAllowInsecureEnv() {
		return try(true)
	}

	return nil, "", e
}

// extractTextFromPDFBytes reads PDF bytes using ledongthuc/pdf and returns a
// normalized plain-text version suitable for regex parsing.
// extractTextFromPDFBytes returns ErrNotPDF if the buffer doesn't look like a PDF.
func extractTextFromPDFBytes(b []byte) (string, error) {
	if !looksLikePDF(b) {
		// Not a real PDF – often an HTML login/interstitial mis-labeled as PDF.
		return "", ErrNotPDF
	}
	rdr := bytes.NewReader(b)
	pr, err := pdf.NewReader(rdr, int64(len(b)))
	if err != nil {
		return "", err
	}
	rs, err := pr.GetPlainText()
	if err != nil {
		return "", err
	}
	raw, err := io.ReadAll(rs)
	if err != nil {
		return "", err
	}
	text := string(raw)
	text = strings.ReplaceAll(text, "\r", "\n")
	text = regexp.MustCompile(`[ \t]+`).ReplaceAllString(text, " ")
	text = regexp.MustCompile(`\n+`).ReplaceAllString(text, "\n")
	return text, nil
}

// find the value after a label in a (PDF) text blob, up to the end of the line.
func findAfterLabelLine(text, label string) string {
	pat := fmt.Sprintf(`(?i)%s\s*[:\-]?\s*([^\n]+)`, regexp.QuoteMeta(label))
	re := regexp.MustCompile(pat)
	m := re.FindStringSubmatch(text)
	if len(m) >= 2 {
		return cleanVal(m[1])
	}
	return ""
}

// REPLACE your current httpPostJSON with this version
func httpPostJSON(base string, p string, body any, out any) error {
	full, err := joinURL(base, p)
	if err != nil {
		return err
	}
	b, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, full, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, */*;q=0.1") // allow non-JSON if out==nil
	req.Header.Set("ngrok-skip-browser-warning", "1")
	req.Header.Set("User-Agent", UserAgent)

	client := &http.Client{Timeout: HTTPTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// status check first
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		slurp, _ := io.ReadAll(io.LimitReader(resp.Body, 1000))
		return fmt.Errorf("POST %s => %s: %s", full, resp.Status, string(slurp))
	}

	// If caller doesn't want a body, don't enforce JSON; just drain and return.
	if out == nil || resp.StatusCode == http.StatusNoContent {
		// drain quietly (avoid holding the TCP conn)
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1024))
		return nil
	}

	// Caller expects JSON -> enforce
	if err := expectJSON(resp); err != nil {
		return err
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func httpGetJSON(base string, p string, out any) error {
	full, err := joinURL(base, p)
	if err != nil {
		return err
	}

	backoff := APIBackoffStart
	var lastErr error

	for attempt := 1; attempt <= APIMaxRetries; attempt++ {
		req, err := http.NewRequest(http.MethodGet, full, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("ngrok-skip-browser-warning", "1")
		req.Header.Set("User-Agent", UserAgent)

		client := newHTTPClient(APIHTTPTimeout)
		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			if attempt < APIMaxRetries && isTimeoutOrTemp(err) {
				time.Sleep(jitter(backoff))
				backoff *= 2
				continue
			}
			return err
		}

		defer resp.Body.Close()

		if isRetryableStatus(resp.StatusCode) && attempt < APIMaxRetries {
			lastErr = fmt.Errorf("server %s", resp.Status)
			time.Sleep(jitter(backoff))
			backoff *= 2
			continue
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			slurp, _ := io.ReadAll(io.LimitReader(resp.Body, 1000))
			return fmt.Errorf("GET %s => %s: %s", full, resp.Status, string(slurp))
		}
		if err := expectJSON(resp); err != nil {
			return err
		}
		return json.NewDecoder(resp.Body).Decode(out)
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("GET %s failed after retries", p)
	}
	return lastErr
}

var ErrNotPDF = errors.New("not a pdf")

func looksLikePDF(b []byte) bool {
	return len(b) >= 4 && bytes.HasPrefix(b, []byte("%PDF"))
}

func looksLikeHTML(b []byte) bool {
	n := 512
	if len(b) < n {
		n = len(b)
	}
	head := strings.ToLower(string(b[:n]))
	return strings.Contains(head, "<html") || strings.Contains(head, "<!doctype html")
}

/* ===================== API calls ===================== */

func apiUpsertUser(telegramID int64, username, firstName, lastName, name, email, startParam string) (*userDTO, error) {
	payload := map[string]any{
		"telegram_id": telegramID,
		"username":    strings.TrimPrefix(username, "@"),
		"first_name":  firstName,
		"last_name":   lastName,
		"name":        name,
		"email":       email,
	}
	if sp := strings.TrimSpace(startParam); sp != "" {
		payload["start_param"] = sp
	}
	var out userDTO
	if err := httpPostJSON(APIBase, "/telegram/users/sync", payload, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func apiCredit(telegramID int64, amountBirr, note string) error {
	payload := map[string]any{"amount": amountBirr, "note": note}
	return httpPostJSON(APIBase, fmt.Sprintf("/wallet/%d/credit", telegramID), payload, nil)
}

func apiDebit(telegramID int64, amountBirr, note string) error {
	payload := map[string]any{"amount": amountBirr, "note": note}
	return httpPostJSON(APIBase, fmt.Sprintf("/wallet/%d/debit", telegramID), payload, nil)
}

func apiGetBalance(telegramID int64) (play string, main string, err error) {
	var out walletResp
	if e := httpGetJSON(APIBase, fmt.Sprintf("/wallet/%d", telegramID), &out); e != nil {
		return "", "", e
	}
	return out.BalanceBirr, out.MainBalanceBirr, nil
}

func apiGetUserByUsername(username string) (*userDTO, error) {
	uname := strings.TrimPrefix(strings.TrimSpace(username), "@")
	var out userDTO
	path := "/users/by-username/" + url.PathEscape(uname)
	if err := httpGetJSON(APIBase, path, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func apiTransfer(fromTelegramID int64, toUsername, amountBirr, target string) error {
	payload := map[string]any{
		"from_telegram_id": fromTelegramID,
		"to_username":      strings.TrimPrefix(toUsername, "@"),
		"amount":           amountBirr,
	}
	t := strings.ToLower(strings.TrimSpace(target))
	if t == "main" || t == "balance" {
		payload["target"] = t
	}
	return httpPostJSON(APIBase, "/wallet/transfer", payload, nil)
}

/* ===================== Utils ===================== */

var sessions = map[int64]*depositSession{}

func sess(chatID int64) *depositSession {
	if s, ok := sessions[chatID]; ok {
		return s
	}
	s := &depositSession{}
	sessions[chatID] = s
	return s
}

func reset(chatID int64) {
	delete(sessions, chatID)
}

func getenvDefault(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("missing env %s", k)
	}
	return v
}

// answerCallback sends a short toast/acknowledgement to a callback query.
func answerCallback(bot *tgbotapi.BotAPI, id, text string) error {
	cb := tgbotapi.NewCallback(id, text)
	_, err := bot.Request(cb)
	return err
}

func randomRef(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func escapeHTML(s string) string {
	repl := strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		"\"", "&quot;",
		"'", "&#39;",
	)
	return repl.Replace(s)
}

func cleanVal(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, "Download PDF", "")
	s = strings.ReplaceAll(s, "download pdf", "")
	s = strings.ReplaceAll(s, "\u00A0", " ")
	fields := strings.Fields(s)
	return strings.Join(fields, " ")
}

func normalizeBirrAmount(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimSuffix(s, "ETB")
	s = strings.TrimSuffix(s, "etb")
	s = strings.TrimSuffix(s, "Birr")
	s = strings.TrimSuffix(s, "birr")
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, ",", "")
	return s
}

var reMoney = regexp.MustCompile(`^\s*\d[\d,]*([.]\d{1,2})?\s*$`)

func isMoneyString(s string) bool {
	return reMoney.MatchString(s)
}

func parseAmountOK(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if !isMoneyString(s) {
		return "", false
	}
	return normalizeBirrAmount(s), true
}

/* ============ Telebirr SMS parse (stricter) ============ */

var (
	reSender    = regexp.MustCompile(`(?i)(?:dear|from|sender)[:\s]+([\p{L} .'-]+)`)
	reAmountSMS = regexp.MustCompile(`(?i)(?:amount|etb|ብር)[:\s]*([\d,]+\.?\d*)`)
	reReceiver  = regexp.MustCompile(`(?i)(?:to|receiver|recipient)[:\s]+([\p{L} .'-]+)`)
	reTxn       = regexp.MustCompile(`(?i)(?:transaction\s+number|txn\s+id|invoice\s+no)[:\s]+([A-Z0-9]+)`)
	reLink      = regexp.MustCompile(`(?i)\bhttps?://\S+`)
	reDate      = regexp.MustCompile(`(?i)\bon\s+([0-9]{2}[-/][0-9]{2}[-/][0-9]{4}\s+[0-9]{2}:[0-9]{2}:[0-9]{2})`)
	reAmtCell   = regexp.MustCompile(`(?i)\b([0-9][0-9,]*\.?\d*)\s*(?:birr|etb)\b`)
)

/* ======= receipt helpers ======= */

func parseTelebirrSMS(s string) (*TelebirrSMS, bool) {
	txt := strings.TrimSpace(s)
	low := strings.ToLower(txt)

	hasLink := reLink.MatchString(txt)
	hasAmount := reAmountSMS.MatchString(txt)
	hasInvoice := reTxn.MatchString(txt)

	hasKeyword := strings.Contains(low, "transferred") ||
		strings.Contains(low, "sent") ||
		strings.Contains(low, "transfer") ||
		strings.Contains(low, "transaction")

	if !(hasKeyword && (hasLink || hasAmount || hasInvoice)) {
		return nil, false
	}

	r := &TelebirrSMS{}
	if m := reTxn.FindStringSubmatch(txt); len(m) > 1 {
		r.Invoice = m[1]
	}
	if m := reAmountSMS.FindStringSubmatch(txt); len(m) > 1 {
		r.Amount = strings.ReplaceAll(m[1], ",", "")
	}
	if m := reSender.FindStringSubmatch(txt); len(m) > 1 {
		r.Payer = strings.TrimSpace(m[1])
	}
	if m := reReceiver.FindStringSubmatch(txt); len(m) > 1 {
		r.Payee = strings.TrimSpace(m[1])
	}
	if m := reLink.FindStringSubmatch(txt); len(m) > 0 {
		r.Link = strings.TrimRight(m[0], ".,;)")
	}
	if m := reDate.FindStringSubmatch(txt); len(m) > 1 {
		r.Date = m[1]
	}
	return r, true
}

// tryLabelChains finds a value next to any of the labels, scanning td/th/div/span/p

func isAllowedReceiptURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	return strings.ToLower(u.Hostname()) == ReceiptAllowedHost
}

func isAllowedCBEURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	return isAllowedCBEHost(u.Hostname())
}

func httpGetHTML(ctx context.Context, rawURL string) (*goquery.Document, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", UserAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")

	client := &http.Client{Timeout: HTTPTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("GET %s => %s: %s", rawURL, resp.Status, string(b))
	}
	ct := strings.ToLower(resp.Header.Get("Content-Type"))
	if !strings.HasPrefix(ct, "text/html") && !strings.HasPrefix(ct, "application/xhtml+xml") {
		return nil, fmt.Errorf("unexpected content-type %q from host", ct)
	}

	r := io.LimitReader(resp.Body, 2<<20) // 2MB guard
	doc, err := goquery.NewDocumentFromReader(r)
	if err != nil {
		return nil, err
	}
	return doc, nil
}

/* ===================== Telebirr receipt parsing (HTML) ===================== */

func valueAfterLabelRow(doc *goquery.Document, labels ...string) string {
	if len(labels) == 0 {
		return ""
	}
	want := make([]string, len(labels))
	for i, l := range labels {
		want[i] = strings.ToLower(strings.TrimSpace(l))
	}
	var out string
	doc.Find("table tr").EachWithBreak(func(_ int, tr *goquery.Selection) bool {
		cells := tr.ChildrenFiltered("th,td")
		if cells.Length() == 0 {
			return true
		}
		for i := 0; i < cells.Length(); i++ {
			c := cells.Eq(i)
			txt := strings.ToLower(cleanVal(c.Text()))
			isLabel := false
			for _, w := range want {
				if w != "" && strings.Contains(txt, w) {
					isLabel = true
					break
				}
			}
			if !isLabel {
				continue
			}
			for j := i + 1; j < cells.Length(); j++ {
				nxt := cells.Eq(j)
				if goquery.NodeName(nxt) != "td" {
					continue
				}
				val := cleanVal(nxt.Text())
				if val != "" {
					out = val
					return false
				}
			}
		}
		return out == ""
	})
	return out
}

func looksLikeInvoiceNo(s string) bool {
	s = strings.ToLower(strings.TrimSpace(s))
	if strings.Contains(s, "invoice") || strings.Contains(s, "የክፍያ") {
		return false
	}
	if len(s) <= 5 {
		return false
	}
	if strings.HasPrefix(s, "chr") {
		return true
	}
	return regexp.MustCompile(`^[A-Z0-9]{8,}$`).MatchString(strings.ToUpper(s))
}

func looksLikeDate(s string) bool {
	sl := strings.TrimSpace(s)
	return regexp.MustCompile(`\d{2}[-/]\d{2}[-/]\d{4}`).MatchString(sl) ||
		regexp.MustCompile(`\d{4}[-/]\d{2}[-/]\d{2}`).MatchString(sl)
}

func looksLikeAmount(s string) bool {
	sl := strings.TrimSpace(s)
	return regexp.MustCompile(`\d+\.?\d*\s*[Bb]irr`).MatchString(sl) ||
		regexp.MustCompile(`\d[\d,]*\.?\d*`).MatchString(sl)
}

func findInvoiceInText(text string) string {
	re := regexp.MustCompile(`(?:CHR|INV|TXN)[A-Z0-9]+`)
	if match := re.FindString(strings.ToUpper(text)); match != "" {
		return match
	}
	re = regexp.MustCompile(`[A-Z0-9]{8,}`)
	return re.FindString(strings.ToUpper(text))
}

func findDateInText(text string) string {
	re := regexp.MustCompile(`\d{2}[-/]\d{2}[-/]\d{4}\s+\d{2}:\d{2}:\d{2}`)
	return re.FindString(text)
}

func findAmountInText(text string) string {
	re := regexp.MustCompile(`(\d[\d,]*\.?\d*)\s*(?:[Bb]irr|ETB)`)
	if matches := re.FindStringSubmatch(text); len(matches) > 1 {
		return matches[1]
	}
	return ""
}

func extractValueByLabel(doc *goquery.Document, labels ...string) string {
	for _, label := range labels {
		if value := findLabelValuePair(doc, label); value != "" {
			return value
		}
	}
	return ""
}

func findLabelValuePair(doc *goquery.Document, label string) string {
	var value string
	doc.Find("td, th, div, span, p").EachWithBreak(func(_ int, el *goquery.Selection) bool {
		if strings.Contains(strings.ToLower(el.Text()), strings.ToLower(label)) {
			next := el.Next()
			if next.Length() > 0 {
				value = cleanVal(next.Text())
				return false
			}
			parent := el.Parent()
			if parent.Length() > 0 {
				parent.Find("td, div, span").EachWithBreak(func(i int, sibling *goquery.Selection) bool {
					if sibling.Get(0) != el.Get(0) {
						candidate := cleanVal(sibling.Text())
						if candidate != "" && !strings.Contains(strings.ToLower(candidate), strings.ToLower(label)) {
							value = candidate
							return false
						}
					}
					return true
				})
			}
		}
		return value == ""
	})
	return value
}

/* ========== NEW: CBE receipt parsing (HTML) ========== */

var (
	reCBELink = regexp.MustCompile(`(?i)\bhttps?://(?:apps|mbreciept)\.cbe\.com\.et(?::\d+)?/\S+`)
	reFTID    = regexp.MustCompile(`\bFT[A-Z0-9]+\b`)
)

// Top-level helper, outside parseCBEReceipt
// Global helper: use this one; do NOT redefine it inside parseCBEReceipt.
func cbeTrimName(s string) string {
	s = cleanVal(s)

	// Only remove obvious trailing junk words, don't touch the core name
	tailJunk := regexp.MustCompile(`(?i)\s+\b(?:ok|done|via|mobile)\b\.?\s*$`)
	s = tailJunk.ReplaceAllString(s, "")

	// Remove only specific leading/trailing label fragments, not middle parts
	leadingJunk := regexp.MustCompile(`(?i)^(Receiver\s*|Payer\s*|Account\s*)[:\-]*\s*`)
	s = leadingJunk.ReplaceAllString(s, "")

	return strings.TrimSpace(s)
}

// ========== CBE receipt parsing (robust + verbose logging) ==========
func parseCBEReceipt(rawURL string) (*CBEReceipt, error) {
	if !isAllowedCBEURL(rawURL) {
		return nil, fmt.Errorf("receipt host not allowed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), HTTPTimeout)
	defer cancel()

	doc, pdfText, err := fetchCBE(ctx, rawURL)
	if err != nil {
		return nil, err
	}

	r := &CBEReceipt{Source: "cbe", Link: rawURL}

	// ---------- local helpers (scoped to this func) ----------
	cbeNextLabelRE := regexp.MustCompile(`(?i)\b(
		Payer|
		Receiver|
		Receiver\s*Account|
		Account|
		Reason|
		Type\s*of\s*service|
		Payment\s*Date(?:\s*&\s*Time)?|
		Reference(?:\s*No\.?(?:\s*\(VAT Invoice No\))?)?|
		Transferred\s*Amount|
		Commission|
		VAT|
		Total\s*amount|
		Amount\s*in\s*Word
	)\b`)

	findAfterLabelSmart := func(text, label string) string {
		re := regexp.MustCompile(`(?i)` + regexp.QuoteMeta(label) + `\s*[:\-]?\s*`)
		loc := re.FindStringIndex(text)
		if loc == nil {
			return ""
		}
		rest := text[loc[1]:]
		var fullName []string
		lines := strings.Split(rest, "\n")
		for i, line := range lines {
			line = cbeTrimName(line)
			if line == "" {
				continue
			}
			fullName = append(fullName, line)
			// Stop if next label is detected or line is empty
			if i+1 < len(lines) && cbeNextLabelRE.MatchString(lines[i+1]) {
				break
			}
		}
		return strings.Join(fullName, " ")
	}
	// Select best candidate: prefer 3+ tokens; else longest cleaned string
	pickBest := func(cands []string) string {
		bestLong := ""
		for _, v := range cands {
			v = cbeTrimName(v)
			if v == "" {
				continue
			}
			if len(nameTokens(v)) >= 3 {
				return v
			}
			if len(v) > len(bestLong) {
				bestLong = v
			}
		}
		return bestLong
	}

	cbeRowValueOrFallback := func(tr *goquery.Selection, label string) string {
		lbl := strings.ToLower(label)
		cells := tr.Find("td,th")

		var cands []string

		// 1) value cell(s) after label
		idx := -1
		for i := 0; i < cells.Length(); i++ {
			if strings.Contains(strings.ToLower(cleanVal(cells.Eq(i).Text())), lbl) {
				idx = i
				break
			}
		}
		if idx >= 0 {
			for j := idx + 1; j < cells.Length(); j++ {
				if v := cbeTrimName(cleanVal(cells.Eq(j).Text())); v != "" {
					cands = append(cands, v)
				}
			}
		}

		// 2) whole row text minus label
		rowText := cleanVal(tr.Text())
		rowText = regexp.MustCompile(`(?i)\b`+label+`\b\s*[:：-]*`).ReplaceAllString(rowText, "")
		if v := cbeTrimName(rowText); v != "" {
			cands = append(cands, v)
		}

		// 3) next row (wrapped value)
		nxt := tr.Next()
		for nxt.Length() > 0 && strings.TrimSpace(nxt.Text()) == "" {
			nxt = nxt.Next()
		}
		if nxt.Length() > 0 {
			nc := nxt.Find("td,th")
			for i := 0; i < nc.Length(); i++ {
				if v := cbeTrimName(cleanVal(nc.Eq(i).Text())); v != "" {
					cands = append(cands, v)
				}
			}
		}

		return pickBest(cands)
	}

	head := func(s string, n int) string {
		if len(s) <= n {
			return s
		}
		return s[:n] + "..."
	}

	// ---------- HTML path (if CBE served HTML) ----------
	if doc != nil {
		accCount := 0
		doc.Find("table tr").Each(func(_ int, tr *goquery.Selection) {
			cells := tr.Find("td,th")
			if cells.Length() < 1 {
				return
			}
			label := strings.ToLower(cleanVal(cells.Eq(0).Text()))

			switch {
			case strings.Contains(label, "payer"):
				if v := cbeRowValueOrFallback(tr, "payer"); v != "" {
					r.PayerName = v
				}
			case label == "account" || strings.Contains(label, "account"):
				accCount++
				val := ""
				if cells.Length() > 1 {
					val = cleanVal(cells.Eq(1).Text())
				}
				if accCount == 1 && r.PayerAccount == "" {
					r.PayerAccount = val
				} else if r.ReceiverAccount == "" {
					r.ReceiverAccount = val
				}
			case strings.Contains(label, "receiver"):
				if v := cbeRowValueOrFallback(tr, "receiver"); v != "" {
					r.ReceiverName = v
				}
			case strings.Contains(label, "payment date"):
				if cells.Length() > 1 {
					r.PaymentDate = cleanVal(cells.Eq(1).Text())
				}
			case strings.Contains(label, "reference"):
				if r.TxID == "" && cells.Length() > 1 {
					r.TxID = strings.ToUpper(strings.TrimSpace(cleanVal(cells.Eq(1).Text())))
				}
			case strings.Contains(label, "transferred amount"):
				if cells.Length() > 1 {
					r.TransferredAmount = cleanVal(cells.Eq(1).Text())
				}
			case strings.Contains(label, "commission or service charge"):
				if cells.Length() > 1 {
					r.Commission = cleanVal(cells.Eq(1).Text())
				}
			case strings.Contains(label, "15% vat"):
				if cells.Length() > 1 {
					r.VAT = cleanVal(cells.Eq(1).Text())
				}
			case strings.Contains(label, "total amount debited"):
				if cells.Length() > 1 {
					r.TotalDebited = cleanVal(cells.Eq(1).Text())
				}
			case strings.Contains(label, "reason"):
				if cells.Length() > 1 {
					r.Reason = cleanVal(cells.Eq(1).Text())
				}
			}
		})
	}

	// ---------- PDF path ----------
	if doc == nil && pdfText != "" {
		// Names and core fields with smart stopping at next label
		r.PayerName = firstNonEmpty(r.PayerName, findAfterLabelSmart(pdfText, "Payer"))
		r.ReceiverName = firstNonEmpty(r.ReceiverName, findAfterLabelSmart(pdfText, "Receiver"))
		r.PaymentDate = firstNonEmpty(r.PaymentDate,
			findAfterLabelSmart(pdfText, "Payment Date & Time"),
			findAfterLabelSmart(pdfText, "Payment Date"),
		)
		if r.TxID == "" {
			r.TxID = strings.ToUpper(firstNonEmpty(
				findAfterLabelSmart(pdfText, "Reference No. (VAT Invoice No)"),
				findAfterLabelSmart(pdfText, "Reference No"),
				reFTID.FindString(strings.ToUpper(pdfText)),
			))
		}
		// Amounts
		if r.TransferredAmount == "" {
			line := findAfterLabelSmart(pdfText, "Transferred Amount")
			if line != "" {
				if m := reAmtCell.FindStringSubmatch(strings.ToLower(line)); len(m) >= 2 {
					r.TransferredAmount = strings.ReplaceAll(m[1], ",", "")
				} else {
					r.TransferredAmount = normalizeBirrAmount(line)
				}
			}
		}
		if r.TotalDebited == "" {
			r.TotalDebited = findAfterLabelSmart(pdfText, "Total amount debited from customers account")
		}
		if r.Commission == "" {
			r.Commission = findAfterLabelSmart(pdfText, "Commission or Service Charge")
		}
		if r.VAT == "" {
			r.VAT = findAfterLabelSmart(pdfText, "15% VAT on Commission")
		}
		// Accounts: collect first two "Account" values
		accRE := regexp.MustCompile(`(?i)^ *Account\s*[:\-]?\s*([^\n]+)$`)
		var accounts []string
		for _, line := range strings.Split(pdfText, "\n") {
			if m := accRE.FindStringSubmatch(line); len(m) >= 2 {
				accounts = append(accounts, cleanVal(m[1]))
			}
		}
		if len(accounts) >= 1 && r.PayerAccount == "" {
			r.PayerAccount = accounts[0]
		}
		if len(accounts) >= 2 && r.ReceiverAccount == "" {
			r.ReceiverAccount = accounts[1]
		}
	}

	// ---------- normalization ----------
	if r.PayerName != "" {
		r.PayerName = cbeTrimName(r.PayerName)
	}
	if r.ReceiverName != "" {
		r.ReceiverName = cbeTrimName(r.ReceiverName)
	}

	if r.TransferredAmount != "" {
		if m := reAmtCell.FindStringSubmatch(strings.ToLower(r.TransferredAmount)); len(m) >= 2 {
			r.TransferredAmount = strings.ReplaceAll(m[1], ",", "")
		} else {
			r.TransferredAmount = normalizeBirrAmount(r.TransferredAmount)
		}
	}

	// Fallback for the new CBE viewer host (Nuxt page shells, data loaded via API).
	// If core fields are still missing, query the public transaction-detail endpoint.
	if (r.TxID == "" || r.TransferredAmount == "" || r.ReceiverName == "") && isMBReceiptHost(rawURL) {
		if fb, ferr := parseCBEMobileReceipt(rawURL); ferr == nil && fb != nil {
			r.TxID = firstNonEmpty(r.TxID, fb.TxID)
			r.TransferredAmount = firstNonEmpty(r.TransferredAmount, fb.TransferredAmount)
			r.ReceiverName = firstNonEmpty(r.ReceiverName, fb.ReceiverName)
			r.PayerName = firstNonEmpty(r.PayerName, fb.PayerName)
			r.PayerAccount = firstNonEmpty(r.PayerAccount, fb.PayerAccount)
			r.ReceiverAccount = firstNonEmpty(r.ReceiverAccount, fb.ReceiverAccount)
			r.PaymentDate = firstNonEmpty(r.PaymentDate, fb.PaymentDate)
			r.Reason = firstNonEmpty(r.Reason, fb.Reason)
			r.Commission = firstNonEmpty(r.Commission, fb.Commission)
			r.VAT = firstNonEmpty(r.VAT, fb.VAT)
			r.TotalDebited = firstNonEmpty(r.TotalDebited, fb.TotalDebited)
		} else if ferr != nil {
			log.Printf("CBE mobile fallback parse failed: %v", ferr)
		}
	}
	log.Printf("Parsed CBE receipt data (pre-validation): %v", r)

	// Fallback: read ?id=FT... from URL if TxID still empty
	if r.TxID == "" {
		if u, err := url.Parse(rawURL); err == nil {
			if id := u.Query().Get("id"); id != "" {
				r.TxID = strings.ToUpper(id)
			}
		}
	}

	// ---------- verbose debug log ----------

	dbg := map[string]any{
		"source":             "cbe",
		"link":               rawURL,
		"txid":               r.TxID,
		"payment_date":       r.PaymentDate,
		"transferred_amount": r.TransferredAmount,
		"commission":         r.Commission,
		"vat":                r.VAT,
		"total_debited":      r.TotalDebited,
		"reason":             r.Reason,
		"payer_name":         r.PayerName,
		"payer_account":      r.PayerAccount,
		"receiver_name":      r.ReceiverName,
		"receiver_account":   r.ReceiverAccount,
	}
	if pdfText != "" {
		dbg["pdf_text_head"] = head(pdfText, 1200)
	}
	if doc != nil {
		if html, _ := doc.Html(); html != "" {
			dbg["html_head"] = head(html, 1200)
		}
	}

	log.Printf("Parsed CBE receipt data (pre-validation): %v", dbg)
	// ---------- final sanity ----------
	if r.TxID == "" || r.TransferredAmount == "" || r.ReceiverName == "" {
		return nil, fmt.Errorf("missing required fields in CBE receipt (txid/amount/receiver)")
	}
	sanitizeCBEReceipt(r)
	return r, nil
}

func isMBReceiptHost(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(u.Hostname()), CBENewAllowedHost)
}

func cbeMobileReceiptID(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	id := strings.Trim(strings.TrimSpace(u.Path), "/")
	if id == "" {
		return ""
	}
	return id
}

type cbeMobileTxnResp struct {
	ID                        string   `json:"id"`
	DebitAmount               string   `json:"debitAmount"`
	AmountCredited            string   `json:"amountCredited"`
	AmountCreditedWithCurrency string  `json:"amountCreditedWithCurrency"`
	AmountDebited             string   `json:"amountDebited"`
	AmountDebitedWithCurrency string   `json:"amountDebitedWithCurrency"`
	TotalChargeAmount         string   `json:"totalChargeAmount"`
	TotalTaxAmount            string   `json:"totalTaxAmount"`
	CreditAccountNo           string   `json:"creditAccountNo"`
	DebitAccountNo            string   `json:"debitAccountNo"`
	CreditAccountHolder       string   `json:"creditAccountHolder"`
	DebitAccountHolder        string   `json:"debitAccountHolder"`
	DateTimes                 []string `json:"dateTimes"`
	PaymentDetails            []string `json:"paymentDetails"`
}

func parseCBEMobileReceipt(rawURL string) (*CBEReceipt, error) {
	id := cbeMobileReceiptID(rawURL)
	if id == "" {
		return nil, fmt.Errorf("missing mobile receipt id in URL")
	}

	base := strings.TrimRight(CBEMobileAPIBase, "/")
	detailURL := fmt.Sprintf("%s/api/v1/transactions/public/transaction-detail/%s", base, url.PathEscape(id))

	req, err := http.NewRequest(http.MethodGet, detailURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", UserAgent)
	req.Header.Set("X-App-ID", CBEMobileAppID)
	req.Header.Set("X-App-Version", CBEMobileAppVersion)

	client := newCBEHTTPClient(CBEHTTPTimeout, false)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("mobile detail api %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var out cbeMobileTxnResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}

	r := &CBEReceipt{
		Source:          "cbe",
		Link:            rawURL,
		TxID:            strings.ToUpper(firstNonEmpty(out.ID, extractFTID(id))),
		PayerName:       out.DebitAccountHolder,
		PayerAccount:    out.DebitAccountNo,
		ReceiverName:    out.CreditAccountHolder,
		ReceiverAccount: out.CreditAccountNo,
		TransferredAmount: firstNonEmpty(
			out.AmountCredited,
			out.DebitAmount,
			out.AmountCreditedWithCurrency,
		),
		Commission:   out.TotalChargeAmount,
		VAT:          out.TotalTaxAmount,
		TotalDebited: firstNonEmpty(out.AmountDebited, out.AmountDebitedWithCurrency),
	}
	if len(out.PaymentDetails) > 0 {
		r.Reason = strings.TrimSpace(strings.Join(out.PaymentDetails, " "))
	}
	if len(out.DateTimes) > 0 {
		dt := strings.TrimSpace(out.DateTimes[0])
		if t, err := time.Parse(time.RFC3339, dt); err == nil {
			r.PaymentDate = t.Format("2006/01/02 15:04:05")
		} else {
			r.PaymentDate = strings.ReplaceAll(strings.TrimSuffix(dt, "Z"), "T", " ")
		}
	}
	sanitizeCBEReceipt(r)
	return r, nil
}

/* ===================== Name & time helpers ===================== */

func firstNonEmpty(ss ...string) string {
	for _, s := range ss {
		if strings.TrimSpace(s) != "" {
			return s
		}
	}
	return ""
}

func isDuplicateTxIDErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())

	if strings.Contains(s, "duplicate key value") ||
		strings.Contains(s, "23505") ||
		strings.Contains(s, "unique constraint") ||
		strings.Contains(s, "idx_transactions_tx_id") ||
		strings.Contains(s, "uniq_tx_reference") {
		return true
	}

	return strings.Contains(s, "409 conflict") ||
		strings.Contains(s, `"error":"duplicate"`) ||
		strings.Contains(s, "txid_or_reference")
}

func isDuplicateRefErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate key value") &&
		(strings.Contains(msg, "idx_transactions_reference") ||
			(strings.Contains(msg, "unique constraint") && strings.Contains(msg, "reference")))
}

func normalizeReceiptDate(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	parts := strings.SplitN(s, " ", 2)
	d := parts[0]
	d = strings.Replace(d, "-", "/", 2)
	if len(parts) == 2 {
		return d + " " + parts[1]
	}
	return d
}

func normalizePersonName(s string) string {
	s = strings.TrimSpace(s)
	fields := strings.Fields(s)
	return strings.ToLower(strings.Join(fields, " "))
}

/* ===================== UI bits ===================== */

func askForPhoneIfMissing(bot *tgbotapi.BotAPI, chatID int64, hasPhone bool) {
	if hasPhone {
		return
	}
	btn := tgbotapi.KeyboardButton{
		Text:           "Share phone number",
		RequestContact: true,
	}
	kb := tgbotapi.NewReplyKeyboard(tgbotapi.NewKeyboardButtonRow(btn))
	kb.OneTimeKeyboard = true
	kb.ResizeKeyboard = true

	msg := tgbotapi.NewMessage(chatID, "Please share your phone number to continue.")
	msg.ReplyMarkup = kb
	bot.Send(msg)
}

// Sends a one-tap button to restart the deposit flow
func sendDepositAgainButton(bot *tgbotapi.BotAPI, chatID int64) {
	row := tgbotapi.NewInlineKeyboardRow(
		tgbotapi.NewInlineKeyboardButtonData("💰 Deposit Again", "dep:manual"),
	)
	msg := tgbotapi.NewMessage(chatID, "If you’d like, you can start a new deposit:")
	msg.ReplyMarkup = tgbotapi.NewInlineKeyboardMarkup(row)
	bot.Send(msg)
}

type WebApp struct {
	URL string `json:"url"`
}
type InlineKeyboardButtonWithWebApp struct {
	Text   string  `json:"text"`
	WebApp *WebApp `json:"web_app,omitempty"`
}
type CustomInlineKeyboardMarkup struct {
	InlineKeyboard [][]InlineKeyboardButtonWithWebApp `json:"inline_keyboard"`
}

func sendMessageWithWebApp(bot *tgbotapi.BotAPI, chatID int64, text string, gameURL string) error {
	// Inline keyboard with a WebApp button (opens inside Telegram)
	keyboard := CustomInlineKeyboardMarkup{
		InlineKeyboard: [][]InlineKeyboardButtonWithWebApp{
			{
				{Text: "🎮 Play", WebApp: &WebApp{URL: gameURL}},
			},
		},
	}

	payload := map[string]interface{}{
		"chat_id":                  chatID,
		"text":                     text,     // contains <b>, <i>, etc.
		"parse_mode":               "HTML",   // ← make Telegram render your HTML
		"disable_web_page_preview": true,     // optional, keeps it clean
		"reply_markup":             keyboard, // your custom inline keyboard
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", bot.Token)
	resp, err := http.Post(apiURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("telegram API error: %s", string(body))
	}
	return nil
}

func playButtons(gameURL string) tgbotapi.InlineKeyboardMarkup {
	btn := tgbotapi.NewInlineKeyboardButtonURL("🎮 Play", gameURL)
	return tgbotapi.NewInlineKeyboardMarkup(tgbotapi.NewInlineKeyboardRow(btn))
}

/* ===================== Main loop ===================== */
func parseNum(s string) (float64, error) {
	// trim and remove thousands separators like "1,234.56"
	s = strings.TrimSpace(strings.ReplaceAll(s, ",", ""))
	return strconv.ParseFloat(s, 64)
}
func canonWallet(t string) string {
	t = strings.ToLower(strings.TrimSpace(t))
	switch t {
	case "main", "main_balance", "mb", "primary":
		return "main_balance"
	case "play", "balance", "pb", "game":
		return "balance"
	default:
		return "main_balance" // safe default
	}
}
func sendPhotoWithWebApp(bot *tgbotapi.BotAPI, chatID int64, photo string, captionHTML string, gameURL string) error {
	kb := CustomInlineKeyboardMarkup{
		InlineKeyboard: [][]InlineKeyboardButtonWithWebApp{
			{{Text: "🎮 Play", WebApp: &WebApp{URL: gameURL}}},
		},
	}
	payload := map[string]any{
		"chat_id":      chatID,
		"photo":        photo,       // URL or file_id
		"caption":      captionHTML, // can be ""
		"parse_mode":   "HTML",
		"reply_markup": kb,
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendPhoto", bot.Token)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("telegram API error: %s", string(body))
	}
	return nil
}

func apiDebitMain(telegramID int64, amountBirr, note string) error {
	w := "main_balance"
	payload := map[string]any{
		"amount": amountBirr,
		"note":   note,
		// send multiple keys to be future-proof across handlers
		"target": w,
		"wallet": w,
		"from":   w,
	}
	path := fmt.Sprintf("/wallet/%d/debit", telegramID)
	return postWalletJSON(telegramID, path, payload, nil)
}

// Replace the old version entirely
func apiWithdrawDebitMain(telegramID int64, amountBirr, method, account, note string) error {
	// Always tell the server to use MAIN wallet (target:"balance")
	payload := map[string]any{
		"amount":  amountBirr,
		"note":    note,
		"method":  method,    // e.g. "telebirr" | "cbe" | "boa"
		"account": account,   // destination number/account
		"target":  "balance", // 👈 force MAIN wallet
	}

	// Try specialized withdraw-debit endpoint first
	if err := httpPostJSON(APIBase, fmt.Sprintf("/wallet/%d/withdraw-debit", telegramID), payload, nil); err == nil {
		return nil
	} else {
		// Fallback to generic debit (some backends only support /debit)
		fallback := map[string]any{
			"amount": amountBirr,
			"note":   fmt.Sprintf("%s (withdraw via %s → %s)", note, strings.ToUpper(method), account),
			"target": "balance", // 👈 still MAIN
		}
		return httpPostJSON(APIBase, fmt.Sprintf("/wallet/%d/debit", telegramID), fallback, nil)
	}
}
func httpPostJSONAuth(base, p string, body any, out any, bearer string) error {
	full, err := joinURL(base, p)
	if err != nil {
		return err
	}
	b, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, full, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, */*;q=0.1")
	req.Header.Set("ngrok-skip-browser-warning", "1")
	req.Header.Set("User-Agent", UserAgent)
	if strings.TrimSpace(bearer) != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}

	client := &http.Client{Timeout: HTTPTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		slurp, _ := io.ReadAll(io.LimitReader(resp.Body, 1000))
		return fmt.Errorf("POST %s => %s: %s", full, resp.Status, string(slurp))
	}
	if out == nil || resp.StatusCode == http.StatusNoContent {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1024))
		return nil
	}
	if err := expectJSON(resp); err != nil {
		return err
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func postWalletJSON(telegramID int64, path string, body any, out any) error {
	// First try unauthenticated (backward compatible)
	if err := httpPostJSON(APIBase, path, body, out); err == nil {
		return nil
	}
	// If that fails (e.g., 401), retry with a JWT for this user
	token, terr := apiMintToken(telegramID)
	if terr != nil {
		// return the original unauthenticated error for clarity
		return fmt.Errorf("wallet call failed and mint-token also failed: %w", terr)
	}
	return httpPostJSONAuth(APIBase, path, body, out, token)
}
func apiDebitTarget(telegramID int64, amountBirr, note, target string) error {
	w := canonWallet(target)
	payload := map[string]any{
		"amount": amountBirr,
		"note":   note,
		"target": w,
		"wallet": w,
		"from":   w,
	}
	path := fmt.Sprintf("/wallet/%d/debit?target=%s&wallet=%s", telegramID, url.QueryEscape(w), url.QueryEscape(w))
	return postWalletJSON(telegramID, path, payload, nil)
}

// ===== bonuses for depositors =====
// ===== bonuses for depositors =====
type BonusDepositorsResp struct {
	Credited     int     `json:"credited"`
	Skipped      int     `json:"skipped"`
	Already      int     `json:"already"`
	CreditedTIDs []int64 `json:"credited_tids"`
}

func apiAdminBonusDepositors(adminTelegramID int64, amount, tag, note string) (*BonusDepositorsResp, error) {
	token, err := apiMintToken(adminTelegramID)
	if err != nil || strings.TrimSpace(token) == "" {
		return nil, fmt.Errorf("mint-token failed: %w", err)
	}
	body := map[string]any{
		"amount": amount,
		"tag":    tag,
		"note":   note,
	}
	var out BonusDepositorsResp
	if err := httpPostJSONAuth(APIBase, "/admin/bonuses/depositors", body, &out, token); err != nil {
		return nil, err
	}
	return &out, nil
}

// ---- API models
type depositorListResp struct {
	TelegramIDs []int64 `json:"telegram_ids"`
}

// GET /admin/depositors  (JWT admin) -> { "telegram_ids": [ ... ] }
func apiListDepositors(adminTelegramID int64) ([]int64, error) {
	token, err := apiMintToken(adminTelegramID)
	if err != nil || strings.TrimSpace(token) == "" {
		return nil, fmt.Errorf("mint-token failed: %w", err)
	}

	full, err := joinURL(APIBase, "/admin/depositors")
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodGet, full, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("ngrok-skip-browser-warning", "1")
	req.Header.Set("User-Agent", UserAgent)

	client := newHTTPClient(APIHTTPTimeout)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1000))
		return nil, fmt.Errorf("GET %s => %s: %s", full, resp.Status, string(b))
	}
	if err := expectJSON(resp); err != nil {
		return nil, err
	}
	var out depositorListResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out.TelegramIDs, nil
}

// Add these functions after the existing API functions

// --- transactions listing for pre-check ---
type Txn struct {
	ID        int64   `json:"id"`
	CreatedAt string  `json:"created_at"`
	Type      string  `json:"type"`   // e.g., "bonus", "deposit"
	Method    string  `json:"method"` // "+", "-", "system"
	Status    string  `json:"status"` // "success", "completed", etc.
	Note      string  `json:"note"`
	Amount    string  `json:"amount"`
	Bonus     string  `json:"bonus"`
	Total     string  `json:"total"`
	Reference *string `json:"reference"`
}

type TxnList []Txn

func apiListTransactions(telegramID int64) (TxnList, error) {
	var out TxnList
	path := fmt.Sprintf("/transactions/%d?limit=%d", telegramID, 50) // look back a bit
	if err := httpGetJSON(APIBase, path, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// true if we already see a phone-verification credit in any plausible form
func hasAnyPhoneVerifyCredit(txns TxnList, noteNeedle string) bool {
	n := strings.ToLower(strings.TrimSpace(noteNeedle))
	for _, t := range txns {
		if !strings.Contains(strings.ToLower(t.Note), n) {
			continue
		}
		// Accept either explicit bonus or a deposit credit that used the same note
		if (strings.EqualFold(t.Type, "bonus") || strings.EqualFold(t.Type, "deposit")) &&
			(strings.EqualFold(t.Status, "completed") || strings.EqualFold(t.Status, "success")) {
			return true
		}
	}
	return false
}

// --- wallet credit using existing endpoint (no server change) ---
func creditWalletSimple(tgID int64, amountETB string, note string) error {
	path := fmt.Sprintf("/wallet/%d/credit", tgID)
	payload := map[string]any{
		"amount": amountETB,               // e.g., "10.00"
		"note":   strings.TrimSpace(note), // "Phone verification bonus"
	}
	var discard struct{}
	return httpPostJSON(APIBase, path, payload, &discard)
}

// Credit user function for referral bonuses
func apiCreditUser(telegramID int64, amountBirr, note string) error {
	payload := map[string]any{
		"amount": amountBirr,
		"note":   note,
	}
	path := fmt.Sprintf("/wallet/%d/credit", telegramID)
	return httpPostJSON(APIBase, path, payload, nil)
}

// Add this function in the utils section
func normalizePhoneEthiopia(p string) string {
	p = strings.TrimSpace(p)
	p = strings.ReplaceAll(p, " ", "")
	p = strings.ReplaceAll(p, "-", "")
	// handle 09xxxxxxx -> +2519xxxxxxx
	if strings.HasPrefix(p, "09") && len(p) == 10 {
		return "+251" + p[1:]
	}
	// handle 9xxxxxxxx -> +2519xxxxxxxx
	if len(p) == 9 && p[0] == '9' {
		return "+251" + p
	}
	// already +251...
	if strings.HasPrefix(p, "+251") {
		return p
	}
	// fallback: return as-is
	return p
}

// notifyInviterAboutBonus sends a DM to the inviter when they earn a bonus
func notifyInviterAboutBonus(bot *tgbotapi.BotAPI, inviterTID, depositorTID int64, depositAmount, bonusAmount string) {
	message := fmt.Sprintf(
		"💰 <b>Referral Deposit Bonus!</b>\n\n"+
			"You earned: <b>%s ብር</b>\n\n"+
			"Keep inviting to earn more on every deposit! 🎯",
		bonusAmount,
	)

	msg := tgbotapi.NewMessage(inviterTID, message)
	msg.ParseMode = "HTML"

	if _, err := bot.Send(msg); err != nil {
		log.Printf("Failed to notify inviter %d about bonus: %v", inviterTID, err)
	}
}
func setupBotCommands(bot *tgbotapi.BotAPI) error {
	commands := []tgbotapi.BotCommand{
		{
			Command:     "start",
			Description: "Start the bot",
		},
		{
			Command:     "play",
			Description: "🎮 Play game",
		},
		{
			Command:     "deposit",
			Description: "💰 Deposit funds",
		},
		{
			Command:     "withdraw",
			Description: "🏧 Withdraw funds",
		},
		{
			Command:     "balance",
			Description: "💳 Check balance",
		},
		{
			Command:     "transfer",
			Description: "🔁 Transfer funds (Admin)",
		},
		{
			Command:     "bonus",
			Description: "🎁 Bonus management (Admin)",
		},
		{
			Command:     "post",
			Description: "📢 Broadcast to depositors (Admin)",
		},
		{
			Command:     "notify",
			Description: "📢 Notify phone-verified users (Admin)", // 👈 ADD THIS
		},
		{
			Command:     "invite",
			Description: "📣 Invite friends",
		},
		{
			Command:     "help",
			Description: "📘 How to play",
		},
		{
			Command:     "contact",
			Description: "☎️ Contact us",
		},
		{
			Command:     "join",
			Description: "👥 Join channel",
		},
	}

	config := tgbotapi.NewSetMyCommands(commands...)
	_, err := bot.Request(config)
	return err
}
func ensureCommandsMenuDefault(bot *tgbotapi.BotAPI) {
	_, err := bot.MakeRequest("setChatMenuButton", tgbotapi.Params{
		"menu_button": `{"type":"commands"}`, // clears any bot-wide web_app
	})
	if err != nil {
		log.Printf("setChatMenuButton (default->commands) failed: %v", err)
	}
}

func ensureCommandsMenuForChat(bot *tgbotapi.BotAPI, chatID int64) {
	_, err := bot.MakeRequest("setChatMenuButton", tgbotapi.Params{
		"chat_id":     strconv.FormatInt(chatID, 10),
		"menu_button": `{"type":"commands"}`, // clears any per-chat web_app
	})
	if err != nil {
		log.Printf("setChatMenuButton (chat->commands) failed: %v", err)
	}
}
func sendDepositMethods(bot *tgbotapi.BotAPI, chatID int64) {
	rows := [][]tgbotapi.InlineKeyboardButton{
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("Telebirr", "dep:telebirr_agent"),
			tgbotapi.NewInlineKeyboardButtonData("CBE", "dep:niged_bank_agent"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("EBIRR", "dep:e_birr_agent"),
		),
	}
	m := tgbotapi.NewMessage(chatID, "Please select the bank option you wish to use for the top-up.")
	m.ReplyMarkup = tgbotapi.NewInlineKeyboardMarkup(rows...)
	bot.Send(m)
}
func showAccountCardStandard(
	bot *tgbotapi.BotAPI,
	chatID int64,
	methodKey string,
	methodDisplay string,
	accountName string,
	accountNumber string,
) {
	// keep selection for validators/parsers later
	sess(chatID).SelectedMethod = strings.ToLower(methodKey)

	steps := []string{
		fmt.Sprintf("<b>1.</b> ከላይ ባለው የ %s አካውንት ገንዘቡን ያስገቡ", escapeHTML(methodDisplay)),
		fmt.Sprintf("<b>2.</b> ብሩን ስትልኩ የከፈላችሁትን መረጃ የያዝ አጭር የጹሁፍ መልክት(sms) ከ %s ይደርሳችኋል", escapeHTML(methodDisplay)),
		"<b>3.</b> የደረሳችሁን አጭር የጹሁፍ መልክት(sms) ሙሉዉን ኮፒ(copy) በማረግ ከታች ባለው የቴሌግራም የጹሁፍ ማስገቢያ ላይ ፔስት(paste) በማረግ ይላኩት",
	}

	var b strings.Builder
	fmt.Fprintf(&b, "👤 ስም: <pre><code>%s</code></pre>\n", escapeHTML(accountName))
	fmt.Fprintf(&b, "🔢 አካውንት: <pre><code>%s</code></pre>\n\n", escapeHTML(accountNumber))
	b.WriteString("<b>🟢 200 ብር ወይም ከዚያ በላይ Deposit ያድርጉ እና 50% ጉርሻ ያገኛሉ! 🟢</b>\n\n")
	b.WriteString("<b>መመሪያ</b>\n\n")
	b.WriteString(strings.Join(steps, "\n\n"))
	b.WriteString("\n\n")

	msg := tgbotapi.NewMessage(chatID, b.String())
	msg.ParseMode = "HTML"
	bot.Send(msg)
}

// Update user's has_deposit field
func updateUserHasDeposit(telegramID int64, hasDeposit bool) error {
	payload := map[string]any{
		"has_deposit": hasDeposit,
	}
	return httpPostJSON(APIBase, fmt.Sprintf("/users/%d", telegramID), payload, nil)
}
func main() {
	_ = godotenv.Load()
	rand.Seed(time.Now().UnixNano())

	botToken := mustEnv("BOT_TOKEN")
	miniAppURL := getenvDefault("MINI_APP_URL", "https://henb.teshie.dev")

	bot, err := tgbotapi.NewBotAPI(botToken)
	if err != nil {
		log.Fatal(err)
	}
	ensureCommandsMenuDefault(bot)

	if err := setupBotCommands(bot); err != nil {
		log.Printf("Failed to set bot commands: %v", err)
	} else {
		log.Printf("Bot commands menu set up successfully")
	}
	log.Printf("Authorized on @%s", bot.Self.UserName)

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 30
	updates := bot.GetUpdatesChan(u)

	for up := range updates {
		/* -------- INLINE CALLBACKS -------- */
		if cq := up.CallbackQuery; cq != nil {
			chatID := cq.Message.Chat.ID
			switch {
			case cq.Data == "act:refresh_balance":
				uid := int64(0)
				if cq.From != nil {
					uid = int64(cq.From.ID)
				}
				if uid == 0 {
					_ = answerCallback(bot, cq.ID, "Cannot identify user.")
					break
				}
				bal, mainBal, err := apiGetBalance(uid)
				if err != nil {
					_ = answerCallback(bot, cq.ID, "Balance error: Please try again")
					break
				}
				_ = answerCallback(bot, cq.ID, "Main: "+mainBal+" ብር • Play: "+bal+" ብር")
			case cq.Data == "act:bonus":
				uid := int64(0)
				if cq.From != nil {
					uid = int64(cq.From.ID)
				}
				if uid == 0 || denyIfNotAdmin(bot, cq.Message.Chat.ID, uid) {
					_ = answerCallback(bot, cq.ID, "Not allowed")
					break
				}
				_ = answerCallback(bot, cq.ID, "Bonus mode")

				s := sess(cq.Message.Chat.ID)
				s.BonusMode = true
				s.AwaitBonusAmount = true
				s.BonusAmountBirr = ""
				s.BonusTag = ""

				bot.Send(tgbotapi.NewMessage(cq.Message.Chat.ID, "Enter the bonus amount (e.g. 50 or 50.00). Type 'cancel' to stop."))

			case cq.Data == "act:transfer":
				uid := int64(0)
				if cq.From != nil {
					uid = int64(cq.From.ID)
				}
				if uid == 0 || denyIfNotAdmin(bot, cq.Message.Chat.ID, uid) {
					_ = answerCallback(bot, cq.ID, "Not allowed") // 👈 add bot
					break
				}

				_ = answerCallback(bot, cq.ID, "Transfer") // 👈 add bot
				s := sess(cq.Message.Chat.ID)
				s.TransferMode = true
				s.AwaitingTransferUsername = true
				s.AwaitingTransferAmount = false
				s.TransferToUsername = ""
				s.TransferTarget = "main"
				bot.Send(tgbotapi.NewMessage(cq.Message.Chat.ID, "Please send the recipient’s Telegram @username (e.g. @someone)."))

			case cq.Data == "tf:target:balance":
				_ = answerCallback(bot, cq.ID, "Target set: Play balance")
				sess(chatID).TransferTarget = "balance"

			case cq.Data == "tf:target:main":
				_ = answerCallback(bot, cq.ID, "Target set: Main balance")
				sess(chatID).TransferTarget = "main"
			case cq.Data == "dep:telebirr_agent":
				_ = answerCallback(bot, cq.ID, "ከቴሌብር ወደ ቴሌብር ብቻ")
				showAccountCardStandard(
					bot, chatID,
					"telebirr", // methodKey
					"Telebirr", // methodDisplay
					AllowedTelebirrReceiverName,
					TelebirrAgentAcct,
				)

			case cq.Data == "dep:niged_bank_agent":
				_ = answerCallback(bot, cq.ID, "ከንግድ ባንክ ወደ ንግድ ባንክ ብቻ")
				showAccountCardStandard(
					bot, chatID,
					"cbe",
					"CBE",
					AllowedCBEBirrReceiverName,
					NigedBankAgentAcct,
				)

			case cq.Data == "dep:e_birr_agent":
				_ = answerCallback(bot, cq.ID, "ከኢብር ወደ ኢብር ብቻ")
				showAccountCardStandard(
					bot, chatID,
					"ebirr",
					"EBIRR",
					AllowedEBirrReceiverName,
					EBIRRAgentAcct,
				)
			case strings.HasPrefix(cq.Data, "wd:method:"):
				_ = answerCallback(bot, cq.ID, "Method selected")
				ws := sess(chatID)
				if !ws.WithdrawMode || !ws.AwaitWithdrawMethod {
					break
				}
				method := strings.TrimPrefix(cq.Data, "wd:method:")
				method = strings.ToLower(method)
				if method != "telebirr" && method != "cbe" && method != "ebirr" {
					method = "telebirr"
				}
				ws.WithdrawMethod = method
				ws.AwaitWithdrawMethod = false
				ws.AwaitWithdrawAccount = true
				bot.Send(tgbotapi.NewMessage(chatID, "Please enter the account/phone number to receive the funds:"))

			case strings.HasPrefix(cq.Data, "wd:confirm:"):
				_ = answerCallback(bot, cq.ID, "Confirmed ✅")
				parts := strings.Split(cq.Data, ":")
				if len(parts) >= 6 {
					uidStr := parts[2]
					amt := parts[3]
					method := parts[4]
					account := parts[5]
					uid, _ := strconv.ParseInt(uidStr, 10, 64)

					now := time.Now().UTC().Format("2006-01-02 15:04:05")
					play, _, _ := apiGetBalance(uid)
					msg := fmt.Sprintf(
						"💸 <b>Withdrawal Successful!</b>\n\n🟢 Amount: %s ብር\n📤 Sent to: %s (%s)\n📅 Date: %s\n💳 Remaining Balance: %s ብር\n\nThank you for using Top Bingo! 🎉",
						escapeHTML(amt), strings.ToUpper(method), escapeHTML(account), escapeHTML(now), escapeHTML(play),
					)
					m := tgbotapi.NewMessage(uid, msg)
					m.ParseMode = "HTML"
					if _, err := bot.Send(m); err != nil {
						log.Printf("confirm notify failed: %v", err)
					}

					edit := tgbotapi.NewEditMessageReplyMarkup(
						cq.Message.Chat.ID,
						cq.Message.MessageID,
						tgbotapi.InlineKeyboardMarkup{InlineKeyboard: [][]tgbotapi.InlineKeyboardButton{}},
					)
					if _, err := bot.Request(edit); err != nil {
						log.Printf("failed to edit admin msg: %v", err)
					}

					newText := cq.Message.Text + "\n\n✅ <b>PAID</b>"
					editText := tgbotapi.NewEditMessageText(cq.Message.Chat.ID, cq.Message.MessageID, newText)
					editText.ParseMode = "HTML"
					if _, err := bot.Send(editText); err != nil {
						log.Printf("failed to edit admin text: %v", err)
					}
				}
			}
			continue
		}

		/* --------------- MESSAGES ---------------- */
		if up.Message == nil {
			continue
		}
		chatID := up.Message.Chat.ID
		text := strings.TrimSpace(up.Message.Text)
		s := sess(chatID)

		/* Capture /start payload early */
		if strings.HasPrefix(text, "/start") {
			parts := strings.Fields(text)
			if len(parts) > 1 {
				s.StartParam = strings.TrimSpace(parts[1]) // e.g. "ref_XXXX"
			}
			log.Printf("[ref] captured start_param=%q chat=%d", s.StartParam, chatID)
			ensureCommandsMenuDefault(bot)

		}

		var (
			userID    int64
			username  string
			firstName string
			lastName  string
			fullName  string
		)
		// ===== POST-TO-ALL-VALID-USERS MODE =====
		if s.PostMode && s.AwaitPostText {
			// allow cancel by text
			if m := up.Message; m != nil && m.Text != "" {
				lt := strings.ToLower(strings.TrimSpace(m.Text))
				if lt == "cancel" || lt == "/cancel" {
					s.PostMode = false
					s.AwaitPostText = false
					bot.Send(tgbotapi.NewMessage(chatID, "Broadcast cancelled."))
					continue
				}
			}

			// fetch all users
			users, err := apiListAllUsers()
			if err != nil {
				bot.Send(tgbotapi.NewMessage(chatID, "Could not load users. Please try again later."))
				continue
			}

			// Filter valid users (non-bot + has phone)
			var validUsers []userDTO
			for _, u := range users {
				if u.TelegramID != 0 && !u.IsBot {
					validUsers = append(validUsers, u)
				}
			}

			// Start background broadcast
			bot.Send(tgbotapi.NewMessage(chatID,
				fmt.Sprintf("🚀 Starting broadcast to %d users in background...", len(validUsers))))

			// Reset state immediately so user can continue using bot
			s.PostMode = false
			s.AwaitPostText = false

			// Run broadcast in background goroutine
			go func(users []userDTO, adminChatID int64, bot *tgbotapi.BotAPI, miniAppURL string) {
				sent, failed := 0, 0
				totalUsers := len(users)

				// Progress tracking
				progressMsg, _ := bot.Send(tgbotapi.NewMessage(adminChatID,
					fmt.Sprintf("📊 Broadcast Progress: 0/%d (0%%)", totalUsers)))

				// Turn ON play button behavior (set to false if you ever want plain broadcast)
				includePlayButton := true

				// Determine message type (based on original admin message)
				var messageType string
				var fileID, caption, textContent string

				if up.Message != nil && len(up.Message.Photo) > 0 {
					messageType = "photo"
					ph := up.Message.Photo
					fileID = ph[len(ph)-1].FileID
					caption = strings.TrimSpace(up.Message.Caption)
				} else if up.Message != nil && up.Message.Text != "" {
					messageType = "text"
					textContent = strings.TrimSpace(up.Message.Text)
				}

				for i, u := range users {
					var err error

					switch messageType {
					case "photo":
						// Default Play URL base
						playURL := strings.TrimRight(miniAppURL, "/")

						if includePlayButton {
							// Try mint token + WebApp photo
							if tok, tokenErr := apiMintToken(u.TelegramID); tokenErr == nil && strings.TrimSpace(tok) != "" {
								playURL = playURL + "/" + tok
								err = sendPhotoWithWebApp(bot, u.TelegramID, fileID, caption, playURL)
							} else {
								// Force fallback path below
								err = fmt.Errorf("token mint failed")
							}
						}

						// Fallback OR no button
						if err != nil || !includePlayButton {
							p := tgbotapi.NewPhoto(u.TelegramID, tgbotapi.FileID(fileID))
							p.Caption = caption
							p.ParseMode = "HTML"

							if includePlayButton {
								// URL button fallback like Bonus mode
								p.ReplyMarkup = tgbotapi.NewInlineKeyboardMarkup(
									tgbotapi.NewInlineKeyboardRow(
										tgbotapi.NewInlineKeyboardButtonURL("🎮 Play Now", playURL),
									),
								)
							}

							_, err = bot.Send(p)
						}

					case "text":
						// Default Play URL base
						playURL := strings.TrimRight(miniAppURL, "/")

						if includePlayButton {
							// Try mint token + WebApp message
							if tok, tokenErr := apiMintToken(u.TelegramID); tokenErr == nil && strings.TrimSpace(tok) != "" {
								playURL = playURL + "/" + tok
								err = sendMessageWithWebApp(bot, u.TelegramID, textContent, playURL)
							} else {
								// Force fallback path below
								err = fmt.Errorf("token mint failed")
							}
						}

						// Fallback OR no button
						if err != nil || !includePlayButton {
							msg := tgbotapi.NewMessage(u.TelegramID, textContent)
							msg.ParseMode = "HTML"
							msg.DisableWebPagePreview = true

							if includePlayButton {
								// URL button fallback like Bonus mode
								msg.ReplyMarkup = tgbotapi.NewInlineKeyboardMarkup(
									tgbotapi.NewInlineKeyboardRow(
										tgbotapi.NewInlineKeyboardButtonURL("🎮 Play Now", playURL),
									),
								)
							}

							_, err = bot.Send(msg)
						}
					}

					if err != nil {
						failed++
						log.Printf("Failed to send to user %d: %v", u.TelegramID, err)
					} else {
						sent++
					}

					// Update progress every 20 users or on completion
					if (i+1)%20 == 0 || (i+1) == totalUsers {
						percent := (i + 1) * 100 / totalUsers
						progressText := fmt.Sprintf("📊 Broadcast Progress: %d/%d (%d%%)", i+1, totalUsers, percent)

						edit := tgbotapi.NewEditMessageText(adminChatID, progressMsg.MessageID, progressText)
						bot.Send(edit)
					}

					// Gentle rate limiting
					time.Sleep(35 * time.Millisecond)
				}

				// Final summary
				summary := fmt.Sprintf("✅ <b>Broadcast Complete</b>\n\n📨 Sent: %d\n❌ Failed: %d\n📊 Total: %d",
					sent, failed, totalUsers)
				finalMsg := tgbotapi.NewMessage(adminChatID, summary)
				finalMsg.ParseMode = "HTML"
				bot.Send(finalMsg)

				// Delete progress message
				deleteMsg := tgbotapi.NewDeleteMessage(adminChatID, progressMsg.MessageID)
				bot.Send(deleteMsg)

			}(validUsers, chatID, bot, miniAppURL)

			// Send immediate confirmation
			bot.Send(tgbotapi.NewMessage(chatID,
				"🎬 Broadcast started in background! You'll receive progress updates. You can continue using the bot normally."))

			continue
		}

		// ===== BONUS MODE (admin only) =====
		if s.BonusMode && s.AwaitBonusAmount {
			amt, ok := parseAmountOK(text)
			if !ok {
				bot.Send(tgbotapi.NewMessage(chatID, "Invalid amount. Use 50 or 50.00. Type 'cancel' to stop."))
				continue
			}

			// Unique idempotency tag for the API
			runTag := time.Now().UTC().Format("20060102-150405") + fmt.Sprintf("-%d", up.Message.From.ID)
			note := "Admin bonus"

			resp, err := apiAdminBonusDepositors(int64(up.Message.From.ID), amt, runTag, note)
			if err != nil {
				bot.Send(tgbotapi.NewMessage(chatID, "❌ Bonus failed: "+err.Error()))
				continue
			}

			// Pull all users and DM ONLY those with has_deposit=true
			users, err := apiListAllUsers() // []UserDTO { TelegramID, HasDeposit, ... }
			if err != nil {
				bot.Send(tgbotapi.NewMessage(chatID, "Bonus credited, but couldn’t fetch users to notify."))
				// still reset + show menu
				s.BonusMode = false
				s.AwaitBonusAmount = false
				sendRoleAwareMenu(bot, chatID, int64(up.Message.From.ID), miniAppURL)
				continue
			}

			// Fancy message (HTML)
			dmHTML := fmt.Sprintf(
				"🌟 <b>Bonus Drop!</b> 🌟\n"+
					"━━━━━━━━━━━━━━━━━━━━\n"+
					"💸 <b>+%s ብር</b> added to your <b>Play</b> wallet.\n\n"+
					"🎯 Ready to win? Tap Play below and jump right in.\n\n"+
					"<i>— Top Bingo</i>",
				escapeHTML(amt),
			)

			dmSent, dmFailed := 0, 0

			for _, u := range users {
				if !u.HasDeposit || u.TelegramID == 0 {
					continue
				}

				// Build a tokenized WebApp URL so it opens inside Telegram
				playURL := strings.TrimRight(miniAppURL, "/")
				if tok, e := apiMintToken(u.TelegramID); e == nil && strings.TrimSpace(tok) != "" {
					playURL = playURL + "/" + tok
				}

				// Prefer your existing helper that sends a WebApp button (in-app open).
				// If it errors (older clients etc.), fall back to a normal URL button.
				if err := sendMessageWithWebApp(bot, u.TelegramID, dmHTML, playURL); err != nil {
					out := tgbotapi.NewMessage(u.TelegramID, dmHTML)
					out.ParseMode = "HTML"
					// Fallback: simple URL button (opens externally if WebApp not supported)
					out.ReplyMarkup = tgbotapi.NewInlineKeyboardMarkup(
						tgbotapi.NewInlineKeyboardRow(
							tgbotapi.NewInlineKeyboardButtonURL("🎮 Play Now", playURL),
						),
					)
					if _, e2 := bot.Send(out); e2 != nil {
						dmFailed++
					} else {
						dmSent++
					}
				} else {
					dmSent++
				}

				time.Sleep(40 * time.Millisecond) // gentle throttle
			}

			// Admin summary
			summary := &strings.Builder{}
			fmt.Fprintf(summary, "🎁 <b>Bonus credited.</b>\n\n")
			fmt.Fprintf(summary, "Amount: %s ብር (Play balance)\n", escapeHTML(amt))
			fmt.Fprintf(summary, "Credited: %d • Already: %d • Skipped: %d\n", resp.Credited, resp.Already, resp.Skipped)
			fmt.Fprintf(summary, "DMs delivered: %d • failed: %d\n", dmSent, dmFailed)

			adminOut := tgbotapi.NewMessage(chatID, summary.String())
			adminOut.ParseMode = "HTML"
			bot.Send(adminOut)

			// Reset flow + back to menu
			s.BonusMode = false
			s.AwaitBonusAmount = false
			sendRoleAwareMenu(bot, chatID, int64(up.Message.From.ID), miniAppURL)
			continue
		}

		/* ====== PHONE SHARE (✅ notify referrer here) ====== */
		if up.Message != nil && up.Message.Contact != nil {
			contact := up.Message.Contact
			rawPhone := strings.TrimSpace(contact.PhoneNumber)
			tgID := contact.UserID
			if tgID == 0 && up.Message.From != nil {
				tgID = int64(up.Message.From.ID)
			}

			// Normalize phone
			phone := normalizePhoneEthiopia(rawPhone)

			// Prefetch existing user
			udto, _ := apiGetUserByTelegramID(tgID)
			prevHadPhone := (udto != nil && udto.HasPhone)

			// Check for referral (start parameter)
			referrerTID := int64(0)
			if s.StartParam != "" && strings.HasPrefix(s.StartParam, "ref_") {
				if refID, err := strconv.ParseInt(strings.TrimPrefix(s.StartParam, "ref_"), 10, 64); err == nil {
					referrerTID = refID
					log.Printf("[REFERRAL] User %d was referred by %d", tgID, referrerTID)
				}
			}

			// --- ALWAYS upsert/sync the phone first ---
			body := map[string]any{
				"telegram_id": tgID,
				"phone":       phone,
			}
			if referrerTID != 0 {
				body["referred_by"] = referrerTID
			}

			var resp sharePhoneResp
			if err := httpPostJSON(APIBase, "/telegram/users/share-phone", body, &resp); err != nil {
				low := strings.ToLower(err.Error())
				switch {
				case strings.Contains(low, "409 conflict"), strings.Contains(low, "phone already used"):
					bot.Send(tgbotapi.NewMessage(up.Message.Chat.ID, "That phone is already linked to another account."))
				case strings.Contains(low, "invalid phone"):
					bot.Send(tgbotapi.NewMessage(up.Message.Chat.ID, "That phone looks invalid. Please try again."))
				default:
					log.Printf("share-phone failed: %v", err)
					bot.Send(tgbotapi.NewMessage(up.Message.Chat.ID, "Could not verify your phone right now. Please try again."))
				}
				continue
			}

			// success UI
			reply := tgbotapi.NewMessage(up.Message.Chat.ID, "Thanks! You're verified. Let's play 🎉")
			reply.ReplyMarkup = tgbotapi.NewRemoveKeyboard(true)
			bot.Send(reply)

			// --- BONUS: only if NOT previously had phone ---
			bonusEligible := !prevHadPhone

			// Pre-check: has this user already received this credit?
			phoneBonusNote := "Phone verification bonus"
			if bonusEligible {
				if txns, err := apiListTransactions(tgID); err != nil {
					log.Printf("WARN list txns %d: %v (continuing with caution)", tgID, err)
				} else if hasAnyPhoneVerifyCredit(txns, phoneBonusNote) {
					bonusEligible = false
					log.Printf("SKIP BONUS: already credited earlier (tg=%d)", tgID)
				}
			}

			if bonusEligible {
				// Credit 10 birr to invitee
				if err := creditWalletSimple(tgID, "5.00", phoneBonusNote); err != nil {
					log.Printf("phone bonus credit failed: %v", err)
					bot.Send(tgbotapi.NewMessage(up.Message.Chat.ID, "Phone saved, but could not add the bonus right now."))
				} else {
					log.Printf("BONUS AWARDED TO INVITEE: tg=%d phone=%s sp=%q amount=10.00",
						tgID, phone, strings.TrimSpace(s.StartParam))
					bot.Send(tgbotapi.NewMessage(up.Message.Chat.ID, "✅ 5 ብር bonus added to your balance."))
				}
			}

			sendMainMenu(bot, up.Message.Chat.ID, miniAppURL)
			continue
		}

		if up.Message.From != nil {
			userID = int64(up.Message.From.ID)
			if up.Message.From.UserName != "" {
				username = "@" + up.Message.From.UserName
			}
			firstName = up.Message.From.FirstName
			lastName = up.Message.From.LastName
			fullName = strings.TrimSpace(firstName + " " + lastName)

			if _, err := apiUpsertUser(userID, username, firstName, lastName, fullName, "", s.StartParam); err != nil {
				log.Printf("upsert user failed: %v", err)
			}
		}

		/* ===== EARLY COMMANDS ===== */
		if strings.HasPrefix(text, "/start") {
			sendMainMenu(bot, chatID, miniAppURL)
			continue
		}

		switch text {
		case "/transfer", "🔁 Transfer", "🎁 Transfer":
			// must have From to know who asked
			if up.Message.From == nil || denyIfNotAdmin(bot, chatID, int64(up.Message.From.ID)) {
				continue
			}
			s.TransferMode = true
			s.AwaitingTransferUsername = true
			s.AwaitingTransferAmount = false
			s.TransferToUsername = ""
			s.TransferTarget = "balance"
			bot.Send(tgbotapi.NewMessage(chatID, "Please send the recipient’s Telegram @username (e.g. @someone)."))
			continue

		// case "/bonus", "🎁 Bonus":
		// 	if up.Message.From == nil || denyIfNotAdmin(bot, chatID, int64(up.Message.From.ID)) {
		// 		continue
		// 	}
		// 	s.BonusMode = true
		// 	s.AwaitBonusAmount = true
		// 	s.BonusAmountBirr = ""
		// 	s.BonusTag = ""
		// 	bot.Send(tgbotapi.NewMessage(chatID, "Enter the bonus amount (e.g. 50 or 50.00). Type 'cancel' to stop."))
		// 	continue

		case "/withdraw", "🏧 Withdraw":
			// Check if user has deposited before allowing withdraw
			_, err := apiGetUserByTelegramID(int64(up.Message.From.ID))
			if err != nil {
				bot.Send(tgbotapi.NewMessage(chatID, "Could not verify your account. Please try again."))
				continue
			}

			// 	if userData != nil && !userData.HasDeposit {
			// 		depositFirstMsg := `🎯 <b>Make Your First Deposit!</b>

			// Before you can withdraw, you need to make your first deposit. This helps us:

			// <b>Ready to get started?</b>
			// 1. Tap "Deposit Now" below
			// 2. Choose your payment method
			// 3. Follow the instructions
			// 4. Start playing and winning!

			// After your first deposit, you'll be able to withdraw your winnings! 🎉`

			// 		msg := tgbotapi.NewMessage(chatID, depositFirstMsg)
			// 		msg.ParseMode = "HTML"

			// 		// Create buttons for deposit methods
			// depositRow1 := tgbotapi.NewInlineKeyboardRow(
			// 	tgbotapi.NewInlineKeyboardButtonData("💰 Deposit Now", "dep:manual"),
			// )
			// depositRow2 := tgbotapi.NewInlineKeyboardRow(
			// 	tgbotapi.NewInlineKeyboardButtonData("💳 Check Balance", "act:refresh_balance"),
			// )
			// msg.ReplyMarkup = tgbotapi.NewInlineKeyboardMarkup(depositRow1, depositRow2)

			// 	bot.Send(msg)
			// 	continue
			// }

			// User has deposited before, proceed with normal withdraw flow
			ws := s
			ws.WithdrawMode = true
			ws.AwaitWithdrawAmount = true
			ws.AwaitWithdrawMethod = false
			ws.AwaitWithdrawAccount = false
			ws.WithdrawAmountBirr = ""
			ws.WithdrawMethod = ""
			ws.WithdrawAccount = ""
			ws.WithdrawRef = randomRef(12)
			bot.Send(tgbotapi.NewMessage(chatID, "Enter the amount to withdraw (e.g. 150 or 150.00). Type 'cancel' to stop."))
			continue
		case "/deposit", "💰 Deposit":
			if up.Message.From == nil {
				bot.Send(tgbotapi.NewMessage(chatID, "Cannot identify user."))
				continue
			}

			uid := int64(up.Message.From.ID)

			// Fetch user to check phone
			dto, err := apiGetUserByTelegramID(uid)
			if err != nil {
				log.Printf("deposit: get user failed: %v", err)
				bot.Send(tgbotapi.NewMessage(chatID, "Please try again."))
				continue
			}

			if dto == nil || !dto.HasPhone {
				// Ask for phone and mark that we should resume the deposit flow after they share it
				s := sess(chatID)
				s.ResumeDepositAfterPhone = true
				askForPhoneIfMissing(bot, chatID, false)
				continue
			}

			// ✅ User already has phone — proceed with the simplified deposit flow
			reset(chatID)
			s := sess(chatID)
			s.SelectedMethod = ""
			s.AwaitingAmount = false
			sendDepositMethods(bot, chatID) // show ONLY the methods
			continue

		case "🎮 Play":
			if up.Message.From == nil {
				bot.Send(tgbotapi.NewMessage(chatID, "Cannot identify user."))
				continue
			}
			userID = int64(up.Message.From.ID)

			if _, err := apiUpsertUser(userID, username, firstName, lastName, fullName, "", s.StartParam); err != nil {
				log.Printf("upsert user failed: %v", err)
			}

			dto, err := apiGetUserByTelegramID(userID)
			if err != nil {
				log.Printf("get user by telegram failed: %v", err)
				bot.Send(tgbotapi.NewMessage(chatID, "Please try again."))
				continue
			}

			if !dto.HasPhone {
				askForPhoneIfMissing(bot, chatID, false)
				continue
			}

			token, err := apiMintToken(userID)
			if err != nil || strings.TrimSpace(token) == "" {
				log.Printf("mint token failed: %v", err)
				bot.Send(tgbotapi.NewMessage(chatID, "Could not get access token. Please try again."))
				continue
			}

			gameURL := strings.TrimRight(miniAppURL, "/") + "/" + token

			if err := sendMessageWithWebApp(bot, chatID, "Choose how you want to open the game:", gameURL); err != nil {
				open := tgbotapi.NewMessage(chatID, "Choose how you want to open the game:")
				open.ReplyMarkup = playButtons(gameURL)
				bot.Send(open)
			}
			continue
		case "/balance", "💰 Check Balance", "💳 Check Balance":
			if userID == 0 {
				bot.Send(tgbotapi.NewMessage(chatID, "Could not identify user."))
				continue
			}

			play, mainBal, err := apiGetBalance(userID)
			if err != nil {
				bot.Send(tgbotapi.NewMessage(chatID, "Balance error: Please try again"))
				continue
			}

			play = strings.TrimSpace(play)
			mainBal = strings.TrimSpace(mainBal)

			playNum, err := parseNum(play)
			if err != nil {
				bot.Send(tgbotapi.NewMessage(chatID, "Balance error: Please try again"))
				continue
			}
			mainNum, err := parseNum(mainBal)
			if err != nil {
				bot.Send(tgbotapi.NewMessage(chatID, "Balance error: Please try again"))
				continue
			}

			total := playNum + mainNum
			totalStr := strconv.FormatFloat(total, 'f', -1, 64) // keep decimals as needed

			msgText := &strings.Builder{}
			fmt.Fprintf(msgText, "<b>💰 BALANCE SUMMARY 💰</b>\n\n")
			fmt.Fprintf(msgText, "🏦 Main Wallet: %s\n", escapeHTML(mainBal))
			fmt.Fprintf(msgText, "🎁 Play Wallet: %s\n", escapeHTML(play))
			fmt.Fprintf(msgText, "━━━━━━━━━━━━━━━━━━\n")
			fmt.Fprintf(msgText, "💵 Total Balance: <b>%s</b>\n", escapeHTML(totalStr))

			reply := tgbotapi.NewMessage(chatID, msgText.String())
			reply.ParseMode = "HTML"
			bot.Send(reply)
			continue
		case "/notify", "📢 Notify Phone Users":
			if up.Message.From == nil || denyIfNotAdmin(bot, chatID, int64(up.Message.From.ID)) {
				continue
			}
			s.NotifyMode = true
			s.AwaitNotifyText = true
			bot.Send(tgbotapi.NewMessage(chatID,
				"📢 <b>Notify Phone-Verified Users</b>\n\n"+
					"Send the message for users who have verified their phone numbers.\n\n"+
					"• Send plain text (HTML allowed)\n"+
					"• Or send a photo with an optional caption\n"+
					"• Type 'cancel' to stop\n\n"+
					"<i>This will send to all users with verified phone numbers.</i>"),
			)
			continue
		case "/post", "📢 Post to Depositors":
			if up.Message.From == nil || denyIfNotAdmin(bot, chatID, int64(up.Message.From.ID)) {
				continue
			}
			s.PostMode = true
			s.AwaitPostText = true
			bot.Send(tgbotapi.NewMessage(chatID,
				"Send the message for depositors.\n\n"+
					"• Send plain text (HTML allowed)\n"+
					"• Or send a photo with an optional caption\n"+
					"Type 'cancel' to stop."),
			)
			continue

		case "📣 Invite":
			if up.Message.From == nil {
				bot.Send(tgbotapi.NewMessage(chatID, "Open from Telegram to get your invite link."))
				continue
			}
			inviterTID := int64(up.Message.From.ID)
			deepLink := fmt.Sprintf("https://t.me/%s?start=ref_%d", bot.Self.UserName, inviterTID)

			// Use Telegram's share URL format
			shareURL := fmt.Sprintf("https://t.me/share/url?url=%s&text=%s",
				url.QueryEscape(deepLink),
				url.QueryEscape("Join me on Top Bingo! 🎮"))
			// እንዴት እንደሚሰራ፡-
			// 🔄እያንዳንዱ የጋበዝከው ሰው Deposit ባደረገ ቁጥር 10 ብር ያገኛሉ!

			// ለምሳሌ፥
			// • 5 ጊዜ ካስገቡ → 50 ብር ያገኛሉ
			// • 10 ጊዜ ካስገቡ → 100 ብር ያገኛሉ
			// • ምንም ገደብ የለም! ገቢዎን ለዘላለም ይቀጥሉ!
			// Enhanced invite message
			txt := fmt.Sprintf(
				"🎉 <b>ጓደኞችን ይጋብዙ እና ተደጋጋሚ ገቢ ያግኙ!</b> 🎉\n\n"+
					"Share your personal link:\n<code>%s</code>\n\n"+
					"<b>እንዴት እንደሚሰራ፡-:</b>\n"+
					"🔄 <b>እያንዳንዱ የጋበዝከው ሰው Deposit ባደረገ ቁጥር 50% ያገኛሉ!</b>\n\n"+
					"<b>ለምሳሌ:</b>\n"+
					"• 50 ብር ጊዜ ካስገቡ → 25 ብር ያገኛሉ\n"+
					"• 100 ጊዜ ካስገቡ → 50 ብር ያገኛሉ\n"+
					"• 200 ጊዜ ካስገቡ → 100 ብር ያገኛሉ\n"+
					"• ምንም ገደብ የለም! ገቢዎን ለዘላለም ይቀጥሉ! 🚀",
				deepLink,
			)
			msg := tgbotapi.NewMessage(chatID, txt)
			msg.ParseMode = "HTML"

			row := tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonURL("🔗 Open Link", deepLink),
				tgbotapi.NewInlineKeyboardButtonURL("📤 Share", shareURL), // Fixed share URL
			)
			msg.ReplyMarkup = tgbotapi.NewInlineKeyboardMarkup(row)
			bot.Send(msg)
			continue
		case "📘 How To Play":
			bot.Send(tgbotapi.NewMessage(chatID, "Pick a stake → choose a board → select numbers."))
			continue
		case "☎️ Contact Us":
			bot.Send(tgbotapi.NewMessage(chatID, "Support: @l8rrl5oii7"))
			continue
		case "👥 Join Us":
			bot.Send(tgbotapi.NewMessage(chatID, "Join our channel: t.me/direbingo"))
			continue
		}

		/* ===== TRANSFER MODE ===== */
		if s.TransferMode {
			lt := strings.ToLower(strings.TrimSpace(text))
			if lt == "cancel" || lt == "/cancel" {
				s.TransferMode = false
				s.AwaitingTransferUsername = false
				s.AwaitingTransferAmount = false
				s.TransferToUsername = ""
				bot.Send(tgbotapi.NewMessage(chatID, "Transfer cancelled."))
				continue
			}

			if s.AwaitingTransferUsername {
				input := strings.TrimSpace(text)

				// Allow cancel
				low := strings.ToLower(input)
				if low == "cancel" || low == "/cancel" {
					s.TransferMode = false
					s.AwaitingTransferUsername = false
					s.AwaitingTransferAmount = false
					s.TransferToUsername = ""
					bot.Send(tgbotapi.NewMessage(chatID, "Transfer cancelled."))
					continue
				}

				var target *userDTO
				var err error
				var display string

				if strings.HasPrefix(input, "@") { // ---- Username path
					u := input
					target, err = apiGetUserByUsername(u)
					if err != nil || target == nil || target.TelegramID == 0 {
						bot.Send(tgbotapi.NewMessage(chatID,
							"User not found or not registered. Send another @username or phone, or type 'cancel'."))
						continue
					}
					// ensure we keep @ for display and for your payload later
					s.TransferToUsername = u
					display = u

				} else if isLikelyPhone(input) { // ---- Phone path
					local, e164, ok := normalizeETHPhone(input)
					if !ok {
						bot.Send(tgbotapi.NewMessage(chatID, "That phone number looks invalid. Use +2519……… or 09………"))
						continue
					}
					// Try both formats
					target, err = apiGetUserByPhone(local)
					if err != nil || target == nil || target.TelegramID == 0 {
						target, err = apiGetUserByPhone(e164)
					}
					if err != nil || target == nil || target.TelegramID == 0 {
						bot.Send(tgbotapi.NewMessage(chatID, "No registered user with that phone. Try another phone/@username or type 'cancel'."))
						continue
					}

					// ✅ Username NOT required anymore.
					s.TransferToUsername = strings.TrimPrefix(target.Username, "@") // may be empty
					if s.TransferToUsername != "" {
						s.TransferToUsername = "@" + s.TransferToUsername
						s.TransferToLabel = s.TransferToUsername
					} else {
						s.TransferToLabel = local // show the phone in prompts/receipts
					}
					s.TransferToID = target.TelegramID

					s.AwaitingTransferUsername = false
					s.AwaitingTransferAmount = true

					bot.Send(tgbotapi.NewMessage(
						chatID,
						fmt.Sprintf("Recipient: %s\nEnter amount (e.g. 150 or 150.00). Type 'cancel' to stop.", s.TransferToLabel),
					))
					continue

				} else { // ---- Not a username or phone
					bot.Send(tgbotapi.NewMessage(chatID,
						"Please send a phone number (+2519… or 09…). Type 'cancel' to stop."))
					continue
				}

				// Prevent self-transfer
				if userID != 0 && target != nil && target.TelegramID == userID {
					bot.Send(tgbotapi.NewMessage(chatID,
						"You cannot transfer to yourself. Send another @username/phone or type 'cancel'."))
					continue
				}

				s.TransferToID = target.TelegramID
				s.AwaitingTransferUsername = false
				s.AwaitingTransferAmount = true

				bot.Send(tgbotapi.NewMessage(chatID,
					fmt.Sprintf("Recipient: %s\nEnter amount (e.g. 150 or 150.00). Type 'cancel' to stop.", display)))
				continue
			}

			if s.AwaitingTransferAmount {
				// validate amount
				if !isMoneyString(text) {
					bot.Send(tgbotapi.NewMessage(chatID, "Invalid amount. Use numbers like 150 or 150.00. Type 'cancel' to stop."))
					continue
				}
				amt := normalizeBirrAmount(text)
				if amt == "" {
					bot.Send(tgbotapi.NewMessage(chatID, "Invalid amount. Type 'cancel' to stop."))
					continue
				}
				if amtF, err := strconv.ParseFloat(amt, 64); err != nil || amtF <= 0 {
					bot.Send(tgbotapi.NewMessage(chatID, "Amount must be greater than 0. Type 'cancel' to stop."))
					continue
				}

				target := s.TransferTarget
				if target == "" {
					target = "balance" // default to Play wallet
				}
				if err := apiTransferFlexible(userID, s.TransferToUsername, s.TransferToID, amt, target); err != nil {
					bot.Send(tgbotapi.NewMessage(chatID, "Transfer failed. Please try again."))
					continue
				}
				// choose a friendly recipient label
				label := strings.TrimSpace(s.TransferToLabel)
				if label == "" {
					label = strings.TrimSpace(s.TransferToUsername)
				}
				if label == "" && s.TransferToID != 0 {
					label = fmt.Sprintf("%d", s.TransferToID)
				}

				// sender confirmation
				bot.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("✅ Transferred %s ብር to %s (main).", amt, label)))

				// notify receiver if we know their Telegram ID
				if s.TransferToID != 0 {
					recvMsg := tgbotapi.NewMessage(s.TransferToID, fmt.Sprintf("✅ You received %s ብር.", amt))
					if _, err := bot.Send(recvMsg); err != nil {
						log.Printf("notify receiver failed (id=%d): %v", s.TransferToID, err)
					}
				}

				// show updated MAIN balance only
				_, mainBal, e := apiGetBalance(userID)
				if e == nil {
					msg := tgbotapi.NewMessage(chatID, "<b>Your Main Balance</b>\n\n💸 <b>"+escapeHTML(mainBal)+" ብር</b>")
					msg.ParseMode = "HTML"
					bot.Send(msg)
				}

				// reset session
				s.TransferMode = false
				s.AwaitingTransferUsername = false
				s.AwaitingTransferAmount = false
				s.TransferToUsername = ""
				s.TransferToLabel = ""
				s.TransferToID = 0
				s.TransferTarget = "main"
				continue
			}

		}

		if s.WithdrawMode {
			lt := strings.ToLower(strings.TrimSpace(text))
			if lt == "cancel" || lt == "/cancel" {
				s.WithdrawMode = false
				s.AwaitWithdrawAmount = false
				s.AwaitWithdrawMethod = false
				s.AwaitWithdrawAccount = false
				bot.Send(tgbotapi.NewMessage(chatID, "Withdrawal cancelled."))
				continue
			}

			if s.AwaitWithdrawAmount {
				amt, ok := parseAmountOK(text)
				if !ok {
					bot.Send(tgbotapi.NewMessage(chatID, "Invalid amount. Use 150 or 150.00. Type 'cancel' to stop."))
					continue
				}

				// Get user data to check if they have deposited before
				// userData, err := apiGetUserByTelegramID(userID)
				if err != nil {
					bot.Send(tgbotapi.NewMessage(chatID, "Could not fetch your user data. Try again later."))
					continue
				}

				// 	// Check if user has never deposited
				// 	if userData != nil && !userData.HasDeposit {
				// 		depositRequiredMsg := `❌ <b>Deposit Required</b>

				// You need to make at least one deposit before you can withdraw.

				// <b>Why this requirement?</b>
				// • Prevents fraudulent activity
				// • Ensures account verification
				// • Confirms payment method ownership

				// 💰 <b>Make your first deposit now!</b>
				// Use the Deposit button below to get started.`

				// 		msg := tgbotapi.NewMessage(chatID, depositRequiredMsg)
				// 		msg.ParseMode = "HTML"

				// 		// Add deposit button for convenience
				// 		depositBtn := tgbotapi.NewInlineKeyboardButtonData("💰 Deposit Now", "dep:manual")
				// 		msg.ReplyMarkup = tgbotapi.NewInlineKeyboardMarkup(tgbotapi.NewInlineKeyboardRow(depositBtn))

				// 		bot.Send(msg)
				// 		continue
				// 	}

				_, mainBal, err := apiGetBalance(userID)
				if err != nil {
					bot.Send(tgbotapi.NewMessage(chatID, "Could not fetch your balance. Try again later."))
					continue
				}
				mainF, _ := strconv.ParseFloat(mainBal, 64)
				amtF, _ := strconv.ParseFloat(amt, 64)

				if amtF < 50 {
					bot.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("❌ Minimum withdraw is 50 ብር. Your main balance is %s ብር.", mainBal)))
					continue
				}
				if amtF > mainF {
					bot.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("❌ Insufficient balance. Your main balance is %s ብር.", mainBal)))
					continue
				}

				s.WithdrawAmountBirr = amt
				s.AwaitWithdrawAmount = false
				s.AwaitWithdrawMethod = true
				sendWithdrawMethodMenu(bot, chatID)
				continue
			}

			if s.AwaitWithdrawAccount {
				acc := strings.TrimSpace(text)
				if acc == "" {
					bot.Send(tgbotapi.NewMessage(chatID, "Please enter a valid account/phone number."))
					continue
				}
				s.WithdrawAccount = acc
				s.AwaitWithdrawAccount = false

				if err := apiDebit(userID, s.WithdrawAmountBirr, "withdraw "+s.WithdrawMethod); err != nil {
					bot.Send(tgbotapi.NewMessage(chatID, "Withdrawal request failed (debit). "+err.Error()))
					s.AwaitWithdrawAmount = true
					continue
				}

				userMsg := fmt.Sprintf(
					"🛑 <b>WITHDRAWAL REQUEST</b> 🛑\n\nTelegram Name: %s\n\n💳 Payment Method: %s\n🔢 Account Number: %s\n💰 Amount: %s ብር\n📌 Status: <b>pending</b>\n🆔 Reference: %s\n\nYour balance has been debited and is now pending processing.",
					escapeHTML(firstName),
					strings.ToUpper(s.WithdrawMethod),
					escapeHTML(acc),
					escapeHTML(s.WithdrawAmountBirr),
					escapeHTML(s.WithdrawRef),
				)
				out := tgbotapi.NewMessage(chatID, userMsg)
				out.ParseMode = "HTML"
				bot.Send(out)

				if NotifyChatID != 0 {
					admin := fmt.Sprintf(
						"🛑 <b>WITHDRAWAL REQUEST</b> 🛑\n\nRequester: %s (%d) %s\n\n👤 Account Name: %s\n💳 Payment Method: %s\n🔢 Account Number: %s\n💰 Amount: %s ብር\n📌 Status: <b>pending</b>\n🆔 Reference: %s",
						escapeHTML(strings.TrimSpace(firstName+" "+lastName)), userID, escapeHTML(username),
						escapeHTML(firstName),
						strings.ToUpper(s.WithdrawMethod),
						escapeHTML(acc),
						escapeHTML(s.WithdrawAmountBirr),
						escapeHTML(s.WithdrawRef),
					)
					btn := tgbotapi.NewInlineKeyboardButtonData(
						"✅ Confirm Paid",
						fmt.Sprintf("wd:confirm:%d:%s:%s:%s:%s", userID, s.WithdrawAmountBirr, s.WithdrawMethod, acc, s.WithdrawRef),
					)
					inline := tgbotapi.NewInlineKeyboardMarkup(tgbotapi.NewInlineKeyboardRow(btn))
					msg := tgbotapi.NewMessage(NotifyChatID, admin)
					msg.ParseMode = "HTML"
					msg.ReplyMarkup = inline
					if _, err := bot.Send(msg); err != nil {
						log.Printf("admin notify failed: %v", err)
					}
				}

				s.WithdrawMode = false
				s.AwaitWithdrawAmount = false
				s.AwaitWithdrawMethod = false
				s.AwaitWithdrawAccount = false
				continue
			}
		}
		/* ===== Deposit amount flow ===== */
		if s.AwaitingAmount && text != "" {
			amt, err := strconv.Atoi(text)
			if err != nil || amt < MinAmountETB || amt > MaxAmountETB {
				bot.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("እባክዎትን ትክክለኛ መጠን ያስገቡ (%d–%d ብር).", MinAmountETB, MaxAmountETB)))
				continue
			}
			s.AwaitingAmount = false
			s.AmountETB = amt
			s.SelectedMethod = "" // make them pick again after new amount
			s.Reference = randomRef(10)
			sendPaymentDetails(bot, chatID, s.AmountETB, s.Reference)
			sendMethodMenu(bot, chatID)
			continue
		}
		// ===== NOTIFY PHONE-VERIFIED USERS MODE =====
		if s.NotifyMode && s.AwaitNotifyText {
			// allow cancel by text
			if m := up.Message; m != nil && m.Text != "" {
				lt := strings.ToLower(strings.TrimSpace(m.Text))
				if lt == "cancel" || lt == "/cancel" {
					s.NotifyMode = false
					s.AwaitNotifyText = false
					bot.Send(tgbotapi.NewMessage(chatID, "Phone users notification cancelled."))
					continue
				}
			}

			// fetch phone-verified users
			users, err := apiListAllUsers()
			if err != nil {
				bot.Send(tgbotapi.NewMessage(chatID, "Could not load users. Please try again later."))
				continue
			}

			// Start processing in background goroutine
			go func(users []userDTO, adminChatID int64, bot *tgbotapi.BotAPI, miniAppURL string) {
				sent, failed, skipped := 0, 0, 0
				totalUsers := 0

				// Count phone-verified users
				for _, u := range users {
					if u.HasPhone && u.TelegramID != 0 && !u.IsBot {
						totalUsers++
					}
				}

				// Send starting message
				startMsg := tgbotapi.NewMessage(adminChatID,
					fmt.Sprintf("🚀 Starting notification to %d phone-verified users...", totalUsers))
				bot.Send(startMsg)

				processed := 0

				// Determine message type and process
				switch {
				case up.Message != nil && len(up.Message.Photo) > 0:
					// PHOTO BROADCAST
					ph := up.Message.Photo
					fileID := ph[len(ph)-1].FileID
					caption := strings.TrimSpace(up.Message.Caption)

					for _, u := range users {
						if !u.HasPhone || u.TelegramID == 0 || u.IsBot {
							skipped++
							continue
						}

						// Try web-app button first
						var err error
						if tok, tokenErr := apiMintToken(u.TelegramID); tokenErr == nil && strings.TrimSpace(tok) != "" {
							gameURL := strings.TrimRight(miniAppURL, "/") + "/" + tok
							err = sendPhotoWithWebApp(bot, u.TelegramID, fileID, caption, gameURL)
						}

						// Fallback to plain photo
						if err != nil {
							p := tgbotapi.NewPhoto(u.TelegramID, tgbotapi.FileID(fileID))
							p.Caption = caption
							p.ParseMode = "HTML"
							if _, e := bot.Send(p); e != nil {
								failed++
							} else {
								sent++
							}
						} else {
							sent++
						}

						processed++
						time.Sleep(40 * time.Millisecond)

						// Progress update every 20 users
						if processed%20 == 0 {
							progressMsg := tgbotapi.NewMessage(adminChatID,
								fmt.Sprintf("📊 Progress: %d/%d users processed...", processed, totalUsers))
							bot.Send(progressMsg)
						}
					}

				case up.Message != nil && up.Message.Text != "":
					// TEXT BROADCAST
					toSend := strings.TrimSpace(up.Message.Text)

					for _, u := range users {
						if !u.HasPhone || u.TelegramID == 0 || u.IsBot {
							skipped++
							continue
						}

						// Try web-app button first
						var err error
						if tok, tokenErr := apiMintToken(u.TelegramID); tokenErr == nil && strings.TrimSpace(tok) != "" {
							gameURL := strings.TrimRight(miniAppURL, "/") + "/" + tok
							err = sendMessageWithWebApp(bot, u.TelegramID, toSend, gameURL)
						}

						// Fallback to plain message
						if err != nil {
							msg := tgbotapi.NewMessage(u.TelegramID, toSend)
							msg.ParseMode = "HTML"
							msg.DisableWebPagePreview = true
							if _, e := bot.Send(msg); e != nil {
								failed++
							} else {
								sent++
							}
						} else {
							sent++
						}

						processed++
						time.Sleep(40 * time.Millisecond)

						// Progress update every 20 users
						if processed%20 == 0 {
							progressMsg := tgbotapi.NewMessage(adminChatID,
								fmt.Sprintf("📊 Progress: %d/%d users processed...", processed, totalUsers))
							bot.Send(progressMsg)
						}
					}

				default:
					// Invalid message type - handled in main goroutine
					return
				}

				// Final summary
				summary := fmt.Sprintf("✅ <b>Notification Complete</b>\n\n📨 Sent: %d\n❌ Failed: %d\n⏭️ Skipped: %d\n📊 Total Users: %d",
					sent, failed, skipped, totalUsers)
				finalMsg := tgbotapi.NewMessage(adminChatID, summary)
				finalMsg.ParseMode = "HTML"
				bot.Send(finalMsg)

			}(users, chatID, bot, miniAppURL)

			// Immediately reset state and respond
			s.NotifyMode = false
			s.AwaitNotifyText = false
			bot.Send(tgbotapi.NewMessage(chatID, "🎬 Notification started in background. You'll receive progress updates. You can continue using the bot normally."))
			continue
		}
		/* ===== Telebirr SMS / receipt parsing ===== */
		if sms, ok := parseTelebirrSMS(text); ok {
			// Decide method
			chosenMethod := ""
			if sms.Link != "" {
				chosenMethod = methodFromReceiptURL(sms.Link) // host wins
			}
			if chosenMethod == "" && s.SelectedMethod != "" {
				chosenMethod = strings.ToLower(s.SelectedMethod)
			}
			if chosenMethod == "" {
				chosenMethod = "telebirr"
			}

			// --- parse by method (do the real parsing here) ---
			var (
				r      *TelebirrReceipt // telebirr/ebirr
				cbeRec *CBEReceipt      // cbe
			)

			switch chosenMethod {
			case "ebirr":
				if sms.Link == "" || !isAllowedEBirrURL(sms.Link) {
					bot.Send(tgbotapi.NewMessage(chatID, "Please send the official EBirr receipt link."))
					continue
				}
				if rr, err := parseEBirrReceipt(sms.Link); err == nil {
					r = rr
				} else {
					log.Printf("ebirr parse err: %v", err)
					bot.Send(tgbotapi.NewMessage(chatID, "❌ Could not read the EBirr receipt. Please open the link and resend."))
					continue
				}

			case "telebirr":
				if sms.Link == "" || !isAllowedReceiptURL(sms.Link) {
					bot.Send(tgbotapi.NewMessage(chatID, "Please send the official Telebirr receipt link."))
					continue
				}
				if rr, err := parseReceiptPage(sms.Link); err == nil {
					r = rr
				} else {
					log.Printf("telebirr parse err: %v", err)
					bot.Send(tgbotapi.NewMessage(chatID, "❌ Could not read the Telebirr receipt. Please open the link and resend."))
					continue
				}
			case "cbe":
				if sms.Link == "" || !isAllowedCBEURL(sms.Link) {
					bot.Send(tgbotapi.NewMessage(chatID, "Please send the official CBE receipt link."))
					continue
				}
				if rr, err := parseCBEReceipt(sms.Link); err == nil {
					cbeRec = rr
					// DO NOT continue here — let the unified post-parse flow run below.
				} else {
					log.Printf("cbe parse err: %v", err)
					bot.Send(tgbotapi.NewMessage(chatID, "❌ Could not read the CBE receipt. Please open the link and resend."))
					continue
				}
			}

			// --- collect fields (NO SMS FALLBACKS FOR CBE) ---
			var (
				invoice, dateStr, amount, sender, receiver string
			)

			if r != nil { // telebirr/ebirr
				invoice = strings.TrimSpace(r.InvoiceNo)
				dateStr = normalizeReceiptDate(r.PaymentDate)
				// After extracting dateStr, add this validation
				if dateStr != "" {
					isValid, err := isWithinLastDays(dateStr, 5)
					if err != nil {
						log.Printf("Date parsing error: %v", err)
						bot.Send(tgbotapi.NewMessage(chatID, 
							"❌ Could not verify the payment date. Please make sure the receipt includes a valid date."))
						continue
					}
					
					if !isValid {
						msg := fmt.Sprintf(
							"❌ <b>Invalid Payment Date</b>\n\n"+
							"The payment date <b>%s</b> is older than 5 days.\n\n"+
							"For security reasons, we only accept deposits made within the last 5 days.\n"+
							"Please make a new payment and send the receipt within 5 days.",
							escapeHTML(dateStr),
						)
						m := tgbotapi.NewMessage(chatID, msg)
						m.ParseMode = "HTML"
						bot.Send(m)
						
						if NotifyChatID != 0 {
							adminMsg := fmt.Sprintf("⚠️ Rejected old payment (user: %d)\nDate: %s", userID, dateStr)
							bot.Send(tgbotapi.NewMessage(NotifyChatID, adminMsg))
						}
						continue
					}
				}
				amount = r.SettledAmount
				sender = firstNonEmpty(r.PayerName, "")
				receiver = firstNonEmpty(r.CreditedPartyName, "")
			} else if cbeRec != nil { // cbe
				invoice = strings.TrimSpace(cbeRec.TxID)
				dateStr = normalizeReceiptDate(cbeRec.PaymentDate)
				// After extracting dateStr, add this validation
				if dateStr != "" {
					isValid, err := isWithinLastDays(dateStr, 5)
					if err != nil {
						log.Printf("Date parsing error: %v", err)
						bot.Send(tgbotapi.NewMessage(chatID, 
							"❌ Could not verify the payment date. Please make sure the receipt includes a valid date."))
						continue
					}
					
					if !isValid {
						msg := fmt.Sprintf(
							"❌ <b>Invalid Payment Date</b>\n\n"+
							"The payment date <b>%s</b> is older than 5 days.\n\n"+
							"For security reasons, we only accept deposits made within the last 5 days.\n"+
							"Please make a new payment and send the receipt within 5 days.",
							escapeHTML(dateStr),
						)
						m := tgbotapi.NewMessage(chatID, msg)
						m.ParseMode = "HTML"
						bot.Send(m)
						
						if NotifyChatID != 0 {
							adminMsg := fmt.Sprintf("⚠️ Rejected old payment (user: %d)\nDate: %s", userID, dateStr)
							bot.Send(tgbotapi.NewMessage(NotifyChatID, adminMsg))
						}
						continue
					}
				}
				amount = cbeRec.TransferredAmount
				sender = firstNonEmpty(cbeRec.PayerName, "")
				receiver = firstNonEmpty(cbeRec.ReceiverName, "")
			} else {
				// Shouldn't happen, but guard anyway
				bot.Send(tgbotapi.NewMessage(chatID, "❌ Unable to read the receipt."))
				continue
			}

			// If SMS host mismatches chosenMethod, stop early
			if sms.Link != "" {
				hostMethod := methodFromReceiptURL(sms.Link)
				if hostMethod != "" && hostMethod != chosenMethod {
					bot.Send(tgbotapi.NewMessage(chatID,
						fmt.Sprintf("❌ The receipt link is for %s, but you selected %s. Please resend the correct receipt.",
							strings.ToUpper(hostMethod), strings.ToUpper(chosenMethod))))
					continue
				}
			}

			// --- validate receiver strictly from parsed page ---
			if !isAllowedReceiverFor(chosenMethod, receiver) {
				expectName := map[string]string{
					"telebirr": AllowedTelebirrReceiverName,
					"ebirr":    AllowedEBirrReceiverName,
					"cbe":      AllowedCBEBirrReceiverName,
				}[chosenMethod]
				msg := fmt.Sprintf(
					"❌ <b>Invalid receipt</b>\n\nThe credited party on your %s receipt is <b>%s</b>.\nIt must include the words: <b>%s</b>.",
					strings.ToUpper(chosenMethod),
					escapeHTML(strings.TrimSpace(receiver)),
					escapeHTML(expectName),
				)
				m := tgbotapi.NewMessage(chatID, msg)
				m.ParseMode = "HTML"
				bot.Send(m)
				if NotifyChatID != 0 {
					admin := fmt.Sprintf("🚫 Invalid receiver (%s)\nGot: %q  Expected≈: %q",
						strings.ToUpper(chosenMethod), strings.TrimSpace(receiver), expectName)
					bot.Send(tgbotapi.NewMessage(NotifyChatID, admin))
				}
				continue
			}

			// --- tx id (namespaced ref if link exists) ---
			txid := strings.ToUpper(invoice)
			if txid == "" && sms.Link != "" {
				if u, err := url.Parse(sms.Link); err == nil {
					if chosenMethod != "cbe" {
						parts := strings.Split(strings.Trim(u.Path, "/"), "/")
						if len(parts) > 0 {
							txid = strings.ToUpper(strings.TrimSpace(parts[len(parts)-1]))
						}
					} else {
						// for CBE we already extracted TxID from query (?id=FT...)
						if id := strings.ToUpper(u.Query().Get("id")); id != "" {
							txid = id
						}
					}
				}
			}
			if txid == "" {
				bot.Send(tgbotapi.NewMessage(chatID, "❌ Could not verify receipt ID. Please open the official receipt link and resend."))
				continue
			}

			// --- amount math ---
			amtBirr := normalizeBirrAmount(amount)
			amtF, err := strconv.ParseFloat(amtBirr, 64)
			if err != nil {
				bot.Send(tgbotapi.NewMessage(chatID, "❌ Invalid amount in receipt."))
				continue
			}

			amtCents := int(math.Round(amtF * 100))
			var bonusPct int
			if amtCents >= 2000*100 {
				bonusPct = 100
			} else if amtCents >= 200*100 {
				bonusPct = 50
			} else {
				bonusPct = 10
			}
			bonusCents := (amtCents * bonusPct) / 100
			totalCents := amtCents + bonusCents
			amountStr := fmt.Sprintf("%.2f", float64(amtCents)/100)
			bonusStr := fmt.Sprintf("%.2f", float64(bonusCents)/100)
			totalStr := fmt.Sprintf("%.2f", float64(totalCents)/100)

			// --- duplicate guard using namespaced ref by host ---
			ref := txid
			if sms.Link != "" {
				if u, err := url.Parse(sms.Link); err == nil {
					ref = strings.ToLower(u.Hostname()) + ":" + strings.ToUpper(txid)
				}
			}
			if alreadyProcessed(txid, ref) {
				msg := fmt.Sprintf("❌ <b>Duplicate receipt</b>\nWe’ve already processed transaction <b>%s</b>.", escapeHTML(txid))
				m := tgbotapi.NewMessage(chatID, msg)
				m.ParseMode = "HTML"
				bot.Send(m)
				if NotifyChatID != 0 {
					admin := fmt.Sprintf("⚠️ Local duplicate guard blocked\nUser: %s (%d) %s\nTxID: %s Ref: %s",
						strings.TrimSpace(firstName+" "+lastName), userID, username, txid, ref)
					bot.Send(tgbotapi.NewMessage(NotifyChatID, admin))
				}
				continue
			}

			// --- build & submit txn ---
			friendly := map[string]string{"telebirr": "Telebirr", "ebirr": "Ebirr", "cbe": "CBE"}[chosenMethod]
			txnReq := createTxnReq{
				Reference:    ref,
				TelegramID:   userID,
				Type:         "deposit",
				Amount:       amountStr,
				Bonus:        bonusStr,
				Total:        totalStr,
				Method:       chosenMethod,
				Account:      "",
				Status:       "pending",
				TxID:         txid,
				Note:         friendly + " deposit (incl. 10% bonus)",
				SenderName:   firstNonEmpty(sender, ""),
				ReceiverName: firstNonEmpty(receiver, ""),
				ReceiptURL:   sms.Link,
				PaymentDate:  firstNonEmpty(normalizeReceiptDate(dateStr), ""),
			}

			requesterName := strings.TrimSpace(firstName + " " + lastName)
			if err := apiCreateTransaction(txnReq); err != nil {
				switch {
				case errors.Is(err, ErrDuplicateTxID), isDuplicateTxIDErr(err):
					txt := fmt.Sprintf("❌ <b>Transaction Failed</b>\n\nThe transaction number <b>%s</b> has already been used.", escapeHTML(txnReq.TxID))
					m := tgbotapi.NewMessage(chatID, txt)
					m.ParseMode = "HTML"
					bot.Send(m)
					sendDepositAgainButton(bot, chatID)
					if NotifyChatID != 0 {
						admin := fmt.Sprintf("⚠️ Duplicate deposit txid (%s)\nUser: %s (%d) %s\nTxID: %s",
							strings.ToUpper(chosenMethod), requesterName, userID, username, txnReq.TxID)
						bot.Send(tgbotapi.NewMessage(NotifyChatID, admin))
					}
					continue
				case errors.Is(err, ErrDuplicateRef), isDuplicateRefErr(err):
					refDisplay := txnReq.Reference
					if refDisplay == "" {
						refDisplay = "(empty)"
					}
					txt := fmt.Sprintf("❌ <b>Transaction Failed</b>\n\nThe reference <b>%s</b> has already been used.", escapeHTML(refDisplay))
					m := tgbotapi.NewMessage(chatID, txt)
					m.ParseMode = "HTML"
					bot.Send(m)
					sendDepositAgainButton(bot, chatID)
					if NotifyChatID != 0 {
						admin := fmt.Sprintf("⚠️ Duplicate deposit reference (%s)\nUser: %s (%d) %s\nReference: %s",
							strings.ToUpper(chosenMethod), requesterName, userID, username, refDisplay)
						bot.Send(tgbotapi.NewMessage(NotifyChatID, admin))
					}
					continue
				default:
					log.Printf("create transaction failed (%s): %v", chosenMethod, err)
					bot.Send(tgbotapi.NewMessage(chatID, "❌ Deposit failed. Please try again later."))
					continue
				}
			}

			// --- credit & notify ---
			note := friendly + " " + firstNonEmpty(txnReq.TxID, "receipt") + " (incl. 10% bonus)"
			if err := apiCredit(userID, totalStr, note); err != nil {
				log.Printf("wallet credit failed (%s): %v", chosenMethod, err)
				bot.Send(tgbotapi.NewMessage(chatID, "❌ Deposit could not be credited. Please try again later."))
				continue
			}

			markProcessed(txnReq.TxID, txnReq.Reference)
			txnID := firstNonEmpty(txnReq.TxID, txnReq.Reference)
			ts := time.Now().Format("02/01/2006 15:04:05")
			playBal, _, _ := apiGetBalance(userID)

			// ✅ UPDATE has_deposit FLAG
			if err := updateUserHasDeposit(userID, true); err != nil {
				log.Printf("Failed to update has_deposit for user %d: %v", userID, err)
			}

			receipt := &strings.Builder{}
			fmt.Fprintf(receipt, "💰 <b>Deposit Successful (%s)!</b>\n\n", friendly)
			fmt.Fprintf(receipt, "🟢 Amount: %s ብር\n", escapeHTML(amountStr))
			fmt.Fprintf(receipt, "🤑 BONUS AMOUNT: %s ብር\n", escapeHTML(bonusStr))
			receipt.WriteString("📥 Credited to: Play Balance\n")
			fmt.Fprintf(receipt, "🆔 Transaction ID: %s\n", escapeHTML(txnID))
			fmt.Fprintf(receipt, "📅 Date: %s\n", escapeHTML(ts))
			fmt.Fprintf(receipt, "💳 New Balance: %s ብር\n\n", escapeHTML(playBal))
			receipt.WriteString("Thank you for using Top Bingo! 🎉")

			userMsg := tgbotapi.NewMessage(chatID, receipt.String())
			userMsg.ParseMode = "HTML"
			bot.Send(userMsg)
			userData, err := apiGetUserByTelegramID(userID)

			if err != nil {
				log.Printf("Failed to get user data for referral check: %v", err)
			} else if userData.InviterTelegramID != nil {
				// Check if deposit amount is 50 birr or more
				amountFloat, err := strconv.ParseFloat(amountStr, 64)
				if err != nil {
					log.Printf("Failed to parse deposit amount for referral check: %v", err)
				} else if amountFloat >= 100.0 {
					// Award referral bonus (only for deposits >= 50 birr)
					inviterTID := *userData.InviterTelegramID
					bonusAmountFloat := 20.0
					bonusAmount := fmt.Sprintf("%.2f", bonusAmountFloat)
					bonusNote := fmt.Sprintf("Referral bonus - user %d deposited %s", userID, amountStr)

					log.Printf("[REFERRAL] Awarding %s to inviter %d for deposit of %s by user %d",
						bonusAmount, inviterTID, amountStr, userID)

					// Credit the inviter
					if err := apiCreditUser(inviterTID, bonusAmount, bonusNote); err != nil {
						log.Printf("[REFERRAL] Failed to credit inviter %d: %v", inviterTID, err)
					} else {
						log.Printf("[REFERRAL] Successfully awarded %s to inviter %d", bonusAmount, inviterTID)

						// Send notification to inviter
						go notifyInviterAboutBonus(bot, inviterTID, userID, amountStr, bonusAmount)
					}
				} else {
					log.Printf("[REFERRAL] Deposit amount %.2f is below 50 birr threshold, no bonus awarded", amountFloat)
				}
			}
			if NotifyChatID != 0 {
				admin := &strings.Builder{}
				fmt.Fprintf(admin, "✅ Deposit credited (%s)\n", friendly)
				fmt.Fprintf(admin, "User: %s (%d) %s\n", strings.TrimSpace(firstName+" "+lastName), userID, username)
				fmt.Fprintf(admin, "Amount: %s ብር\n", escapeHTML(amountStr))
				if txnReq.TxID != "" {
					fmt.Fprintf(admin, "Txn: %s\n", escapeHTML(txnReq.TxID))
				}
				if txnReq.ReceiptURL != "" {
					fmt.Fprintf(admin, "Link: %s\n", escapeHTML(txnReq.ReceiptURL))
				}
				if chosenMethod == "cbe" && cbeRec != nil && (cbeRec.PayerName != "" || cbeRec.ReceiverName != "") {
					fmt.Fprintf(admin, "Payer/Receiver: %s → %s\n",
						escapeHTML(firstNonEmpty(cbeRec.PayerName, "(?)")),
						escapeHTML(firstNonEmpty(cbeRec.ReceiverName, "(?)")))
				} else if r != nil && (r.PayerName != "" || r.CreditedPartyName != "") {
					fmt.Fprintf(admin, "Payer/Receiver: %s → %s\n",
						escapeHTML(firstNonEmpty(r.PayerName, "(?)")),
						escapeHTML(firstNonEmpty(r.CreditedPartyName, "(?)")))
				}
				groupMsg := tgbotapi.NewMessage(NotifyChatID, admin.String())
				groupMsg.ParseMode = "HTML"
				bot.Send(groupMsg)
			}

			continue
		}

		/* ===== EBIRR receipt parsing ===== */
		if reEBirrLink.MatchString(text) {
			link := strings.TrimRight(reEBirrLink.FindString(text), ".,;)")
			if !isAllowedEBirrURL(link) {
				bot.Send(tgbotapi.NewMessage(chatID, "❌ Invalid Ebirr link host."))
				// optional: admin notify
				continue
			}

			r, err := parseEBirrReceipt(link)
			if err != nil {
				log.Printf("EBirr parse error: %v", err)
				bot.Send(tgbotapi.NewMessage(chatID, "❌ Could not parse the Ebirr receipt. Please open the link and resend."))
				continue
			}
			// Receiver validation (“ebirr” route)
			if !isAllowedReceiverFor("ebirr", r.CreditedPartyName) {
				exp := strings.Join(ebirrReceiverVariantsDisplay(), " | ")
				msg := fmt.Sprintf(
					"❌ <b>Invalid receipt</b>\n\nThe credited party on your EBirr receipt is <b>%s</b>.\nIt must include one of: <b>%s</b>.",
					escapeHTML(strings.TrimSpace(r.CreditedPartyName)),
					escapeHTML(exp),
				)
				m := tgbotapi.NewMessage(chatID, msg)
				m.ParseMode = "HTML"
				bot.Send(m)
				if NotifyChatID != 0 {
					bot.Send(tgbotapi.NewMessage(NotifyChatID,
						fmt.Sprintf("🚫 Invalid receiver (EBirr)\nGot: %q  Expected≈ any of: %s",
							strings.TrimSpace(r.CreditedPartyName), exp)))
				}
				continue
			}

			txid := strings.ToUpper(strings.TrimSpace(r.InvoiceNo))
			amtStr := normalizeBirrAmount(r.SettledAmount)
			amtF, err := strconv.ParseFloat(amtStr, 64)
			if err != nil {
				bot.Send(tgbotapi.NewMessage(chatID, "❌ Invalid amount in Ebirr receipt."))
				continue
			}

			amtCents := int(math.Round(amtF * 100))
			var bonusPct int
			if amtCents >= 2000*100 {
				bonusPct = 100
			} else if amtCents >= 200*100 {
				bonusPct = 50
			} else {
				bonusPct = 10
			}
			bonusCents := (amtCents * bonusPct) / 100
			totalCents := amtCents + bonusCents

			amountStr := fmt.Sprintf("%.2f", float64(amtCents)/100)
			bonusStr := fmt.Sprintf("%.2f", float64(bonusCents)/100)
			totalStr := fmt.Sprintf("%.2f", float64(totalCents)/100)

			// namespaced reference by host
			ref := strings.ToLower(EBirrAllowedHost) + ":" + txid

			if alreadyProcessed(txid, ref) {
				msg := fmt.Sprintf(
					"❌ <b>Duplicate receipt</b>\nWe’ve already processed transaction <b>%s</b>. If you believe this is a mistake, contact support.",
					escapeHTML(txid),
				)
				m := tgbotapi.NewMessage(chatID, msg)
				m.ParseMode = "HTML"
				bot.Send(m)
				if NotifyChatID != 0 {
					admin := fmt.Sprintf("⚠️ Local duplicate guard blocked (EBirr)\nUser: %s (%d) %s\nTxID: %s Ref: %s",
						strings.TrimSpace(firstName+" "+lastName), userID, username, txid, ref)
					bot.Send(tgbotapi.NewMessage(NotifyChatID, admin))
				}
				continue
			}

			txnReq := createTxnReq{
				Reference:    ref,
				TelegramID:   userID,
				Type:         "deposit",
				Amount:       amountStr,
				Bonus:        bonusStr,
				Total:        totalStr,
				Method:       "ebirr",
				Account:      "", // you may store Receiver Account/Mobile if you want
				Status:       "pending",
				TxID:         txid,
				Note:         "Ebirr deposit (incl. 10% bonus)",
				SenderName:   firstNonEmpty(r.PayerName, ""),
				ReceiverName: firstNonEmpty(r.CreditedPartyName, ""),
				ReceiptURL:   link,
				PaymentDate:  firstNonEmpty(normalizeReceiptDate(r.PaymentDate), ""),
			}

			requesterName := strings.TrimSpace(firstName + " " + lastName)
			if err := apiCreateTransaction(txnReq); err != nil {
				switch {
				case errors.Is(err, ErrDuplicateTxID), isDuplicateTxIDErr(err):
					txt := fmt.Sprintf(
						"❌ <b>Transaction Failed</b>\n\nThe transaction number <b>%s</b> has already been used.\nPlease submit a different receipt or make a new deposit.",
						escapeHTML(txnReq.TxID),
					)
					m := tgbotapi.NewMessage(chatID, txt)
					m.ParseMode = "HTML"
					bot.Send(m)
					sendDepositAgainButton(bot, chatID)
					if NotifyChatID != 0 {
						admin := fmt.Sprintf("⚠️ Duplicate deposit txid (EBirr)\nUser: %s (%d) %s\nTxID: %s",
							requesterName, userID, username, txnReq.TxID)
						bot.Send(tgbotapi.NewMessage(NotifyChatID, admin))
					}
					continue

				case errors.Is(err, ErrDuplicateRef), isDuplicateRefErr(err):
					refDisplay := txnReq.Reference
					if refDisplay == "" {
						refDisplay = "(empty)"
					}
					txt := fmt.Sprintf(
						"❌ <b>Transaction Failed</b>\n\nThe reference <b>%s</b> has already been used.\nPlease submit a different receipt or make a new deposit.",
						escapeHTML(refDisplay),
					)
					m := tgbotapi.NewMessage(chatID, txt)
					m.ParseMode = "HTML"
					bot.Send(m)
					sendDepositAgainButton(bot, chatID)
					if NotifyChatID != 0 {
						admin := fmt.Sprintf("⚠️ Duplicate deposit reference (EBirr)\nUser: %s (%d) %s\nReference: %s",
							requesterName, userID, username, refDisplay)
						bot.Send(tgbotapi.NewMessage(NotifyChatID, admin))
					}
					continue

				default:
					log.Printf("create transaction failed (EBirr): %v", err)
					bot.Send(tgbotapi.NewMessage(chatID, "❌ Deposit failed. Please try again later."))
					continue
				}
			}

			note := "Ebirr " + firstNonEmpty(txnReq.TxID, "receipt") + " (incl. 10% bonus)"
			if err := apiCredit(userID, totalStr, note); err != nil {
				log.Printf("wallet credit failed (EBirr): %v", err)
				bot.Send(tgbotapi.NewMessage(chatID, "❌ Deposit could not be credited. Please try again later."))
				continue
			}

			markProcessed(txnReq.TxID, txnReq.Reference)

			txnID := firstNonEmpty(txnReq.TxID, txnReq.Reference)
			ts := time.Now().Format("02/01/2006 15:04:05")
			playBal, _, _ := apiGetBalance(userID)

			// ✅ UPDATE has_deposit FLAG
			if err := updateUserHasDeposit(userID, true); err != nil {
				log.Printf("Failed to update has_deposit for user %d: %v", userID, err)
			}

			receipt := &strings.Builder{}
			receipt.WriteString("💰 <b>Deposit Successful (Ebirr)!</b>\n\n")
			fmt.Fprintf(receipt, "🟢 Amount: %s ብር\n", escapeHTML(amountStr))
			fmt.Fprintf(receipt, "🤑 BONUS AMOUNT: %s ብር\n", escapeHTML(bonusStr))
			receipt.WriteString("📥 Credited to: Play Balance\n")
			fmt.Fprintf(receipt, "🆔 Transaction ID: %s\n", escapeHTML(txnID))
			fmt.Fprintf(receipt, "📅 Date: %s\n", escapeHTML(ts))
			fmt.Fprintf(receipt, "💳 New Balance: %s ብር\n\n", escapeHTML(playBal))
			receipt.WriteString("Thank you for using Top Bingo! 🎉")

			userMsg := tgbotapi.NewMessage(chatID, receipt.String())
			userMsg.ParseMode = "HTML"
			bot.Send(userMsg)
			// 👇 REFERRAL BONUS - Give 10 birr to inviter
			// 👇 REFERRAL BONUS - Give 10 birr to inviter only if deposit >= 50 birr
			// 👇 REFERRAL BONUS - Give 10 birr to inviter only if deposit >= 50 birr
			userData, err := apiGetUserByTelegramID(userID)

			if err != nil {
				log.Printf("Failed to get user data for referral check: %v", err)
			} else if userData.InviterTelegramID != nil {
				// Check if deposit amount is 50 birr or more
				amountFloat, err := strconv.ParseFloat(amountStr, 64)
				if err != nil {
					log.Printf("Failed to parse deposit amount for referral check: %v", err)
				} else if amountFloat >= 50.0 {
					// Award referral bonus (only for deposits >= 50 birr)
					inviterTID := *userData.InviterTelegramID
					bonusAmountFloat := 10.0
					bonusAmount := fmt.Sprintf("%.2f", bonusAmountFloat)
					bonusNote := fmt.Sprintf("Referral bonus - user %d deposited %s", userID, amountStr)

					log.Printf("[REFERRAL] Awarding %s to inviter %d for deposit of %s by user %d",
						bonusAmount, inviterTID, amountStr, userID)

					// Credit the inviter
					if err := apiCreditUser(inviterTID, bonusAmount, bonusNote); err != nil {
						log.Printf("[REFERRAL] Failed to credit inviter %d: %v", inviterTID, err)
					} else {
						log.Printf("[REFERRAL] Successfully awarded %s to inviter %d", bonusAmount, inviterTID)

						// Send notification to inviter
						go notifyInviterAboutBonus(bot, inviterTID, userID, amountStr, bonusAmount)
					}
				} else {
					log.Printf("[REFERRAL] Deposit amount %.2f is below 50 birr threshold, no bonus awarded", amountFloat)
				}
			}
			if NotifyChatID != 0 {
				admin := &strings.Builder{}
				fmt.Fprintf(admin, "✅ Deposit credited (EBirr)\n")
				fmt.Fprintf(admin, "User: %s (%d) %s\n", strings.TrimSpace(firstName+" "+lastName), userID, username)
				fmt.Fprintf(admin, "Amount: %s ብር\n", escapeHTML(amountStr))
				if txnReq.TxID != "" {
					fmt.Fprintf(admin, "Txn: %s\n", escapeHTML(txnReq.TxID))
				}
				if link != "" {
					fmt.Fprintf(admin, "Link: %s\n", escapeHTML(link))
				}
				if r.PayerName != "" || r.CreditedPartyName != "" {
					fmt.Fprintf(admin, "Payer/Receiver: %s → %s\n",
						escapeHTML(firstNonEmpty(r.PayerName, "(?)")),
						escapeHTML(firstNonEmpty(r.CreditedPartyName, "(?)")))
				}
				groupMsg := tgbotapi.NewMessage(NotifyChatID, admin.String())
				groupMsg.ParseMode = "HTML"
				bot.Send(groupMsg)
			}
			continue
		}

		/* ===== CBE SMS / receipt parsing ===== */
		if reCBELink.MatchString(text) {
			link := strings.TrimRight(reCBELink.FindString(text), ".,;)")
			if !isAllowedCBEURL(link) {
				bot.Send(tgbotapi.NewMessage(chatID, "❌ Invalid CBE link host."))
				continue
			}

			r, err := parseCBEReceipt(link)
			if err != nil {
				log.Printf("CBE parse error: %v", err)
				bot.Send(tgbotapi.NewMessage(chatID, "❌ Could not parse the CBE receipt. Please open the link and resend."))
				continue
			}

			// Validate receiver
			if !isAllowedReceiverFor("cbe-birr", r.ReceiverName) {
				msg := fmt.Sprintf(
					"❌ <b>Invalid receipt</b>\n\nThe credited party on your CBE receipt is <b>%s</b>.\nIt must include the words: <b>%s</b>.",
					escapeHTML(strings.TrimSpace(r.ReceiverName)),
					escapeHTML(AllowedCBEBirrReceiverName),
				)
				m := tgbotapi.NewMessage(chatID, msg)
				m.ParseMode = "HTML"
				bot.Send(m)
				if NotifyChatID != 0 {
					admin := fmt.Sprintf("🚫 Invalid receiver (CBE)\nGot: %q  Expected≈: %q", strings.TrimSpace(r.ReceiverName), AllowedCBEBirrReceiverName)
					bot.Send(tgbotapi.NewMessage(NotifyChatID, admin))
				}
				continue
			}

			txid := strings.ToUpper(strings.TrimSpace(r.TxID))
			amtStr := normalizeBirrAmount(r.TransferredAmount)
			amtF, err := strconv.ParseFloat(amtStr, 64)
			if err != nil {
				bot.Send(tgbotapi.NewMessage(chatID, "❌ Invalid amount in CBE receipt."))
				continue
			}
			amtCents := int(math.Round(amtF * 100))
			var bonusPct int
			if amtCents >= 2000*100 {
				bonusPct = 100
			} else if amtCents >= 200*100 {
				bonusPct = 50
			} else {
				bonusPct = 10
			}
			bonusCents := (amtCents * bonusPct) / 100
			totalCents := amtCents + bonusCents
			amountStr := fmt.Sprintf("%.2f", float64(amtCents)/100)
			bonusStr := fmt.Sprintf("%.2f", float64(bonusCents)/100)
			totalStr := fmt.Sprintf("%.2f", float64(totalCents)/100)

			// reference: namespaced by receipt host
			refHost := strings.ToLower(CBEAllowedHost)
			if u, err := url.Parse(link); err == nil && strings.TrimSpace(u.Hostname()) != "" {
				refHost = strings.ToLower(u.Hostname())
			}
			ref := refHost + ":" + txid

			if alreadyProcessed(txid, ref) {
				msg := fmt.Sprintf(
					"❌ <b>Duplicate receipt</b>\nWe’ve already processed transaction <b>%s</b>. If you believe this is a mistake, contact support.",
					escapeHTML(txid),
				)
				m := tgbotapi.NewMessage(chatID, msg)
				m.ParseMode = "HTML"
				bot.Send(m)
				if NotifyChatID != 0 {
					admin := fmt.Sprintf("⚠️ Local duplicate guard blocked (CBE)\nUser: %s (%d) %s\nTxID: %s Ref: %s",
						strings.TrimSpace(firstName+" "+lastName), userID, username, txid, ref)
					bot.Send(tgbotapi.NewMessage(NotifyChatID, admin))
				}
				continue
			}

			txnReq := createTxnReq{
				Reference:    ref,
				TelegramID:   userID,
				Type:         "deposit",
				Amount:       amountStr,
				Bonus:        bonusStr,
				Total:        totalStr,
				Method:       "cbe",
				Account:      r.ReceiverAccount,
				Status:       "pending",
				TxID:         txid,
				Note:         "CBE deposit (incl. 10% bonus)",
				SenderName:   firstNonEmpty(r.PayerName, ""),
				ReceiverName: firstNonEmpty(r.ReceiverName, ""),
				ReceiptURL:   link,
				PaymentDate:  firstNonEmpty(normalizeReceiptDate(r.PaymentDate), ""),
			}

			requesterName := strings.TrimSpace(firstName + " " + lastName)
			if err := apiCreateTransaction(txnReq); err != nil {
				switch {
				case errors.Is(err, ErrDuplicateTxID), isDuplicateTxIDErr(err):
					txt := fmt.Sprintf(
						"❌ <b>Transaction Failed</b>\n\nThe transaction number <b>%s</b> has already been used.\nPlease submit a different receipt or make a new deposit.",
						escapeHTML(txnReq.TxID),
					)
					m := tgbotapi.NewMessage(chatID, txt)
					m.ParseMode = "HTML"
					bot.Send(m)
					sendDepositAgainButton(bot, chatID)
					if NotifyChatID != 0 {
						admin := fmt.Sprintf("⚠️ Duplicate deposit txid (CBE)\nUser: %s (%d) %s\nTxID: %s",
							requesterName, userID, username, txnReq.TxID)
						bot.Send(tgbotapi.NewMessage(NotifyChatID, admin))
					}
					continue

				case errors.Is(err, ErrDuplicateRef), isDuplicateRefErr(err):
					refDisplay := txnReq.Reference
					if refDisplay == "" {
						refDisplay = "(empty)"
					}
					txt := fmt.Sprintf(
						"❌ <b>Transaction Failed</b>\n\nThe reference <b>%s</b> has already been used.\nPlease submit a different receipt or make a new deposit.",
						escapeHTML(refDisplay),
					)
					m := tgbotapi.NewMessage(chatID, txt)
					m.ParseMode = "HTML"
					bot.Send(m)
					sendDepositAgainButton(bot, chatID)
					if NotifyChatID != 0 {
						admin := fmt.Sprintf("⚠️ Duplicate deposit reference (CBE)\nUser: %s (%d) %s\nReference: %s",
							requesterName, userID, username, refDisplay)
						bot.Send(tgbotapi.NewMessage(NotifyChatID, admin))
					}
					continue

				default:
					log.Printf("create transaction failed (CBE): %v", err)
					bot.Send(tgbotapi.NewMessage(chatID, "❌ Deposit failed. Please try again later."))
					continue
				}
			}

			note := "CBE " + firstNonEmpty(txnReq.TxID, "receipt") + " (incl. 10% bonus)"
			if err := apiCredit(userID, totalStr, note); err != nil {
				log.Printf("wallet credit failed (CBE): %v", err)
				bot.Send(tgbotapi.NewMessage(chatID, "❌ Deposit could not be credited. Please try again later."))
				continue
			}

			markProcessed(txnReq.TxID, txnReq.Reference)

			txnID := firstNonEmpty(txnReq.TxID, txnReq.Reference)
			ts := time.Now().Format("02/01/2006 15:04:05")
			playBal, _, _ := apiGetBalance(userID)

			// ✅ UPDATE has_deposit FLAG
			if err := updateUserHasDeposit(userID, true); err != nil {
				log.Printf("Failed to update has_deposit for user %d: %v", userID, err)
			}

			receipt := &strings.Builder{}
			receipt.WriteString("💰 <b>Deposit Successful (CBE)!</b>\n\n")
			fmt.Fprintf(receipt, "🟢 Amount: %s ብር\n", escapeHTML(amountStr))
			fmt.Fprintf(receipt, "🤑 BONUS AMOUNT: %s ብር\n", escapeHTML(bonusStr))
			receipt.WriteString("📥 Credited to: Play Balance\n")
			fmt.Fprintf(receipt, "🆔 Transaction ID: %s\n", escapeHTML(txnID))
			fmt.Fprintf(receipt, "📅 Date: %s\n", escapeHTML(ts))
			fmt.Fprintf(receipt, "💳 New Balance: %s ብር\n\n", escapeHTML(playBal))
			receipt.WriteString("Thank you for using Top Bingo! 🎉")

			userMsg := tgbotapi.NewMessage(chatID, receipt.String())
			userMsg.ParseMode = "HTML"
			bot.Send(userMsg)
			// 👇 REFERRAL BONUS - Give 10 birr to inviter
			// 👇 REFERRAL BONUS - Give 10 birr to inviter only if deposit >= 50 birr
			// 👇 REFERRAL BONUS - Give 10 birr to inviter only if deposit >= 50 birr
			userData, err := apiGetUserByTelegramID(userID)

			if err != nil {
				log.Printf("Failed to get user data for referral check: %v", err)
			} else if userData.InviterTelegramID != nil {
				// Check if deposit amount is 50 birr or more
				amountFloat, err := strconv.ParseFloat(amountStr, 64)
				if err != nil {
					log.Printf("Failed to parse deposit amount for referral check: %v", err)
				} else if amountFloat >= 50.0 {
					// Award referral bonus (only for deposits >= 50 birr)
					inviterTID := *userData.InviterTelegramID
					bonusAmountFloat := 10.0
					bonusAmount := fmt.Sprintf("%.2f", bonusAmountFloat)
					bonusNote := fmt.Sprintf("Referral bonus - user %d deposited %s", userID, amountStr)

					log.Printf("[REFERRAL] Awarding %s to inviter %d for deposit of %s by user %d",
						bonusAmount, inviterTID, amountStr, userID)

					// Credit the inviter
					if err := apiCreditUser(inviterTID, bonusAmount, bonusNote); err != nil {
						log.Printf("[REFERRAL] Failed to credit inviter %d: %v", inviterTID, err)
					} else {
						log.Printf("[REFERRAL] Successfully awarded %s to inviter %d", bonusAmount, inviterTID)

						// Send notification to inviter
						go notifyInviterAboutBonus(bot, inviterTID, userID, amountStr, bonusAmount)
					}
				} else {
					log.Printf("[REFERRAL] Deposit amount %.2f is below 50 birr threshold, no bonus awarded", amountFloat)
				}
			}
			if NotifyChatID != 0 {
				admin := &strings.Builder{}
				fmt.Fprintf(admin, "✅ Deposit credited (CBE)\n")
				fmt.Fprintf(admin, "User: %s (%d) %s\n", strings.TrimSpace(firstName+" "+lastName), userID, username)
				fmt.Fprintf(admin, "Amount: %s ብር\n", escapeHTML(amountStr))
				if txnReq.TxID != "" {
					fmt.Fprintf(admin, "Txn: %s\n", escapeHTML(txnReq.TxID))
				}
				if link != "" {
					fmt.Fprintf(admin, "Link: %s\n", escapeHTML(link))
				}
				if r.PayerName != "" || r.ReceiverName != "" {
					fmt.Fprintf(admin, "Payer/Receiver: %s → %s\n",
						escapeHTML(firstNonEmpty(r.PayerName, "(?)")),
						escapeHTML(firstNonEmpty(r.ReceiverName, "(?)")))
				}
				groupMsg := tgbotapi.NewMessage(NotifyChatID, admin.String())
				groupMsg.ParseMode = "HTML"
				bot.Send(groupMsg)
			}

			continue
		}

		/* ===== Fallback: show menu ===== */
		sendMainMenu(bot, chatID, miniAppURL)
	}
}

/* ====================== Telebirr HTML parser ======================== */

func parseReceiptPage(rawURL string) (*TelebirrReceipt, error) {
	if !isAllowedReceiptURL(rawURL) {
		return nil, fmt.Errorf("receipt host not allowed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), HTTPTimeout)
	defer cancel()

	doc, err := httpGetHTML(ctx, rawURL)
	if err != nil {
		return nil, err
	}

	r := &TelebirrReceipt{Source: "receipt", Link: rawURL}

	r.PayerName = valueAfterLabelRow(doc, "የከፋይ ስም", "payer name")
	r.CreditedPartyName = valueAfterLabelRow(doc, "የገንዘብ ተቀባይ ስም", "credited party name")

	if r.PayerName == "" {
		r.PayerName = extractValueByLabel(doc, "የከፋይ ስም", "Payer Name")
	}
	if r.CreditedPartyName == "" {
		r.CreditedPartyName = extractValueByLabel(doc, "የገንዘብ ተቀባይ ስም", "Credited Party name")
	}

	doc.Find("table").Each(func(i int, table *goquery.Selection) {
		tableText := strings.ToLower(table.Text())
		if strings.Contains(tableText, "invoice") || strings.Contains(tableText, "የክፍያ") ||
			strings.Contains(tableText, "payment") || strings.Contains(tableText, "amount") {

			table.Find("tr").Each(func(j int, row *goquery.Selection) {
				cells := row.Find("td")
				if cells.Length() >= 3 {
					c1 := cleanVal(cells.Eq(0).Text())
					c2 := cleanVal(cells.Eq(1).Text())
					c3 := cleanVal(cells.Eq(2).Text())
					if looksLikeInvoiceNo(c1) && looksLikeDate(c2) && looksLikeAmount(c3) {
						r.InvoiceNo = c1
						r.PaymentDate = c2
						r.SettledAmount = c3
					}
				}
			})
		}
	})

	if r.InvoiceNo == "" {
		r.InvoiceNo = findInvoiceInText(doc.Text())
	}
	if r.PaymentDate == "" {
		r.PaymentDate = findDateInText(doc.Text())
	}
	if r.SettledAmount == "" {
		r.SettledAmount = findAmountInText(doc.Text())
	}

	if r.SettledAmount != "" {
		m := reAmtCell.FindStringSubmatch(strings.ToLower(r.SettledAmount))
		if len(m) >= 2 {
			r.SettledAmount = strings.ReplaceAll(m[1], ",", "")
		}
	}

	if r.InvoiceNo == "" {
		if u, err := url.Parse(rawURL); err == nil {
			parts := strings.Split(strings.Trim(u.Path, "/"), "/")
			if len(parts) > 1 {
				r.InvoiceNo = strings.TrimSpace(parts[len(parts)-1])
			}
		}
	}

	if r.InvoiceNo == "" && r.SettledAmount == "" && r.PayerName == "" && r.CreditedPartyName == "" {
		return nil, fmt.Errorf("could not parse receipt page (empty key fields)")
	}

	return r, nil
}

/* ====================== UI ======================== */
func handleStart(bot *tgbotapi.BotAPI, chatID int64) error {
	// Example: whatever you currently do in your /start handler
	msg := tgbotapi.NewMessage(chatID, "Welcome To Top Bingo! 🎉")
	_, err := bot.Send(msg)
	return err
}

// Put your /start behavior here (or wherever you already have it).

// Shows a multi-row menu like your screenshots.
// "Transfer" only appears for admins.
// isLikelyPhone reports whether s looks like an ET phone (+2519..., 09..., or 9........)
func isLikelyPhone(s string) bool {
	ss := strings.TrimSpace(s)
	// very permissive: starts with +, 0 or digit and has at least 9 digits total
	if ss == "" {
		return false
	}
	if ss[0] == '+' || ss[0] == '0' || (ss[0] >= '0' && ss[0] <= '9') {
		// keep only digits
		d := digits(ss)
		return len(d) >= 9
	}
	return false
}

// normalizeETHPhone returns ("09xxxxxxxx", "+2519xxxxxxxx", true) if s looks valid.
func normalizeETHPhone(s string) (local string, e164 string, ok bool) {
	d := digits(s) // only digits
	switch {
	// +2519xxxxxxxx or 2519xxxxxxxx
	case strings.HasPrefix(s, "+2519") && len(d) == 12:
		return "0" + d[3:], "+251" + d[3:], true
	case strings.HasPrefix(d, "2519") && len(d) == 12:
		return "0" + d[3:], "+251" + d[3:], true

	// 09xxxxxxxx
	case strings.HasPrefix(d, "09") && len(d) == 10:
		return d, "+251" + d[1:], true

	// 9xxxxxxxx (no leading zero)
	case strings.HasPrefix(d, "9") && len(d) == 9:
		return "0" + d, "+251" + d, true
	}
	return "", "", false
}
func apiGetUserByPhone(phone string) (*userDTO, error) {
	p := "/users/by-phone/" + url.PathEscape(strings.TrimSpace(phone))
	var out userDTO
	if err := httpGetJSON(APIBase, p, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func sendMainMenu(bot *tgbotapi.BotAPI, chatID int64, miniAppURL string) {
	rows := [][]tgbotapi.KeyboardButton{
		// Row 1
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton("🎮 Play"),
		),
		// Row 2
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton("💰 Deposit"),
			tgbotapi.NewKeyboardButton("🏧 Withdraw"),
		),
		// Row 3
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton("💳 Check Balance"),
		),
		// Row 4
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton("📣 Invite"),
			tgbotapi.NewKeyboardButton("📘 How To Play"),
		),
		// Row 5
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton("☎️ Contact Us"),
			tgbotapi.NewKeyboardButton("👥 Join Us"),
		),
		// Row 6 (Transfer)
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton("🎁 Transfer"),
		),
	}

	kb := tgbotapi.NewReplyKeyboard(rows...)
	kb.ResizeKeyboard = true

	msg := tgbotapi.NewMessage(chatID, "Main menu:")
	msg.ReplyMarkup = kb
	bot.Send(msg)
}

func sendDepositEntry(bot *tgbotapi.BotAPI, chatID int64) {
	m := tgbotapi.NewMessage(chatID, "Choose Your Preferred Deposit Method")
	row := tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("Manual", "dep:manual"))
	m.ReplyMarkup = tgbotapi.NewInlineKeyboardMarkup(row)
	bot.Send(m)
}

func sendPaymentDetails(bot *tgbotapi.BotAPI, chatID int64, amount int, reference string) {
	card := fmt.Sprintf(
		"<b>የክፍያ ዝርዝር</b>\n\n"+
			"<b>ስም:</b> %s\n"+
			"<b>ስልክ:</b> %s\n"+
			"<b>መጠን:</b> %d ብር\n"+
			"<b>ሬፈረንስ:</b> %s",
		escapeHTML(PayeeName),
		escapeHTML(PayeePhone),
		amount,
		reference,
	)
	msg1 := tgbotapi.NewMessage(chatID, card)
	msg1.ParseMode = "HTML"
	bot.Send(msg1)
}

func sendMethodMenu(bot *tgbotapi.BotAPI, chatID int64) {
	row1 := tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("Telebirr", "dep:telebirr_agent"))
	row2 := tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("CBE", "dep:niged_bank_agent"))
	row3 := tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("EBIRR", "dep:e_birr_agent"))
	inline := tgbotapi.NewInlineKeyboardMarkup(row1, row2, row3)
	m := tgbotapi.NewMessage(chatID, "እባክዎትን የመያዣ አማራጭ ይምረጡ።")
	m.ReplyMarkup = inline
	bot.Send(m)
}

func sendWithdrawMethodMenu(bot *tgbotapi.BotAPI, chatID int64) {
	row := tgbotapi.NewInlineKeyboardRow(
		tgbotapi.NewInlineKeyboardButtonData("Telebirr", "wd:method:telebirr"),
		tgbotapi.NewInlineKeyboardButtonData("CBE", "wd:method:cbe"),
		tgbotapi.NewInlineKeyboardButtonData("BOA", "wd:method:boa"),
		tgbotapi.NewInlineKeyboardButtonData("EBIRR", "wd:method:ebirr"),
	)
	m := tgbotapi.NewMessage(chatID, "Select payment method:")
	m.ReplyMarkup = tgbotapi.NewInlineKeyboardMarkup(row)
	bot.Send(m)
}

// NEW: pass the method-specific expected receiver name explicitly
func showAccountAndInstructions(
	bot *tgbotapi.BotAPI,
	chatID int64,
	methodTitle string,
	account string,
	expectedReceiver string,
) {
	card := fmt.Sprintf(
		"<b>የክፍያ አካውንት</b>\n\n"+
			"<b>ስም:</b> <pre><code>%s</code></pre>\n"+
			"<b>አካውንት / ስልክ:</b> <pre><code>%s</code></pre>\n"+
			"<b>አማራጭ:</b> %s",
		escapeHTML(expectedReceiver),
		escapeHTML(account),
		escapeHTML(methodTitle),
	)
	msg := tgbotapi.NewMessage(chatID, card)
	msg.ParseMode = "HTML"
	bot.Send(msg)

	bot.Send(tgbotapi.NewMessage(chatID, "የክፍያ ማረጋገጫ መልዕክት(SMS)/Link እባክዎ እዚህ ይመልሱ 👇👇"))
}
