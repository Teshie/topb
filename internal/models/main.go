package models

import "time"

// --- Users / Wallet ---

type User struct {
	ID               uint      `json:"id" gorm:"primaryKey"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
	TelegramID       int64     `json:"telegram_id" gorm:"uniqueIndex;not null"`
	Username         string    `json:"username"`
	FirstName        string    `json:"first_name"`
	LastName         string    `json:"last_name"`
	Name             string    `json:"name"`
	Email            string    `json:"email"`
	Phone            string    `json:"phone" gorm:"uniqueIndex;type:varchar(32)"`
	PhoneVerifiedAt  *time.Time
	BalanceCents     int64 `json:"-" gorm:"not null;default:0"`
	MainBalanceCents int64 `json:"-" gorm:"not null;default:0"`
}

type Transaction struct {
	ID         uint      `json:"id" gorm:"primaryKey"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
	TelegramID int64     `json:"telegram_id" gorm:"index"`

	Type    string `json:"type" gorm:"type:varchar(20)"`   // deposit|withdraw|stake|win...
	Method  string `json:"method" gorm:"type:varchar(30)"` // -, +
	Status  string `json:"status" gorm:"type:varchar(20)"`
	Note    string `json:"note" gorm:"type:text"`
	Amount  string `json:"amount" gorm:"type:numeric(18,2)"`
	Bonus   string `json:"bonus" gorm:"type:numeric(18,2)"`
	Total   string `json:"total" gorm:"type:numeric(18,2)"`
	Reference   *string `json:"reference" gorm:"type:varchar(64)"`
	TxID        *string `json:"txid" gorm:"type:varchar(64)"`
	Account     string  `json:"account" gorm:"type:varchar(64)"`
	SenderName  string  `json:"sender_name" gorm:"type:varchar(128)"`
	ReceiverName string `json:"receiver_name" gorm:"type:varchar(128)"`
	ReceiptURL   string `json:"receipt_url" gorm:"type:text"`
	PaymentDate  string `json:"payment_date" gorm:"type:varchar(64)"`
}

// --- Game rooms (moved from ws.go) ---

type Room struct {
	RoomID      string     `gorm:"primaryKey"`
	StakeAmount int64      `gorm:"not null"`                  // birr
	Status      string     `gorm:"type:varchar(32);not null"` // Ready|pending|about_to_start|playing|claimed
	StartTime   *time.Time `gorm:""`
	PossibleWin int64      `gorm:"not null;default:0"` // cents
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type RoomPlayer struct {
	ID          uint      `gorm:"primaryKey"`
	RoomID      string    `gorm:"index:uniq_room_tid,unique;not null"`
	TelegramID  int64     `gorm:"index:uniq_room_tid,unique;not null"`
	BoardNumber *int      `gorm:""`
	JoinedAt    time.Time `gorm:"not null;autoCreateTime"`
	CreatedAt   time.Time
	UpdatedAt   time.Time
}
