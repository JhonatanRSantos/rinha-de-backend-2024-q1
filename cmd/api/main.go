package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/JhonatanRSantos/gocore/pkg/gocontext"
	"github.com/JhonatanRSantos/gocore/pkg/godb"
	"github.com/JhonatanRSantos/gocore/pkg/goenv"
	"github.com/JhonatanRSantos/gocore/pkg/golog"
	"github.com/JhonatanRSantos/gocore/pkg/goweb"

	"github.com/bytedance/sonic"
	"github.com/gofiber/fiber/v2"
)

var (
	dbRead  godb.DB
	dbWrite godb.DB
	ctx     = gocontext.FromContext(context.Background())
)

func main() {
	var (
		err    error
		routes = []goweb.WebRoute{}
	)
	golog.SetEnv(goenv.Local)

	connected := false
	for attemp := 0; attemp < 100; attemp++ {
		if dbWrite, dbRead, err = GetDatabaseConnections(); err != nil {
			golog.Log().Debug(ctx, fmt.Sprintf("failed to connect to the database. Attemp [%d]. Cause: %s", attemp, err))
			time.Sleep(time.Millisecond * 100)
			continue
		}
		connected = true
		break
	}

	if !connected {
		golog.Log().Error(ctx, "failed to connect to the database. Cause: max attemps reached")
		return
	}

	defer dbWrite.Close()
	defer dbRead.Close()

	ws := goweb.NewWebServer(goweb.DefaultConfig(goweb.WebServerDefaultConfig{
		AppName: "rinha-de-backend-2024-q1-with-mariadb",
		JSONConfig: goweb.JSONConfig{
			Encoder: sonic.Marshal,
			Decoder: sonic.Unmarshal,
		},
	}))

	routes = append(routes, goweb.WebRoute{
		Method:   "POST",
		Path:     "/clientes/:id/transacoes",
		Handlers: []func(c *fiber.Ctx) error{PostTransactions},
	}, goweb.WebRoute{
		Method:   "GET",
		Path:     "/clientes/:id/extrato",
		Handlers: []func(c *fiber.Ctx) error{GetStatement},
	})

	ws.AddRoutes(routes...)

	if err := ws.Listen(fmt.Sprintf(":%s", os.Getenv("API_PORT"))); err != nil {
		golog.Log().Error(ctx, fmt.Sprintf("failed to graceful shutdown. Cause: %s", err))
	}
}

// GetDatabaseConnections returns the database connections
func GetDatabaseConnections() (dbWrite godb.DB, dbRead godb.DB, err error) {
	dbConfigs := godb.DBConfig{
		Host:             os.Getenv("DB_HOST"),
		Port:             os.Getenv("DB_PORT"),
		User:             os.Getenv("DB_USER"),
		Password:         os.Getenv("DB_PASSWORD"),
		Database:         os.Getenv("DB_NAME"),
		DatabaseType:     godb.MySQLDB,
		ConnectTimeout:   time.Second * 1,
		ConnectionParams: godb.MySQLDefaultParams,
	}
	dbConfigs.ConnectionParams["parseTime"] = "true"

	if dbWrite, err = godb.NewDB(dbConfigs); err != nil {
		return nil, nil, fmt.Errorf("failed to connect to the database (write). Cause: %w", err)
	}

	if dbRead, err = godb.NewDB(dbConfigs); err != nil {
		return nil, nil, fmt.Errorf("failed to connect to the database (read). Cause: %w", err)
	}

	dbWrite.SetMaxOpenConns(45)
	dbWrite.SetMaxIdleConns(10)
	dbWrite.SetConnMaxLifetime(time.Minute * 1)
	dbWrite.SetConnMaxIdleTime(time.Minute * 1)

	dbRead.SetMaxOpenConns(15)
	dbRead.SetMaxIdleConns(10)
	dbRead.SetConnMaxLifetime(time.Minute * 1)
	dbRead.SetConnMaxIdleTime(time.Minute * 1)

	return
}

type Client struct {
	ID      string `db:"id"`
	Limit   int64  `db:"limit"`
	Balance int64  `db:"balance"`
}

type StatementAmout struct {
	Total int64  `json:"total"`
	Date  string `json:"data_extrato"`
	Limit int64  `json:"limite"`
}

type Transaction struct {
	Amount      int64     `db:"amount"      json:"valor"`
	Type        string    `db:"type"        json:"tipo"`
	Description string    `db:"description" json:"descricao"`
	CreatedAt   time.Time `db:"created_at"  json:"realizada_em"`
}

type GetStatementResponse struct {
	StatementAmout StatementAmout `json:"saldo"`
	Transactions   []Transaction  `json:"ultimas_transacoes"`
}

type PostTransactionRequest struct {
	Amount      int64  `json:"valor"`
	Type        string `json:"tipo"`
	Description string `json:"descricao"`
}

type PostTransactionResponse struct {
	Limit   int64 `json:"limite"`
	Balance int64 `json:"saldo"`
}

// PostTransactions creates a new transaction for a client
func PostTransactions(c *fiber.Ctx) error {
	var (
		tx       godb.Tx
		err      error
		client   Client
		body     []byte
		balance  int64
		clientID string
		request  PostTransactionRequest
	)

	body = c.Body()
	clientID = c.Params("id")

	if len(body) == 0 {
		return c.SendStatus(http.StatusUnprocessableEntity)
	}

	if err := json.Unmarshal(body, &request); err != nil {
		return c.SendStatus(http.StatusUnprocessableEntity)
	}

	if request.Type != "c" && request.Type != "d" {
		return c.SendStatus(http.StatusUnprocessableEntity)
	}

	descriptionLen := len(request.Description)
	if descriptionLen < 1 || descriptionLen > 10 {
		return c.SendStatus(http.StatusUnprocessableEntity)
	}

	// Desing decision: I'm not using defer to commit or rollback the transaction to avoid wasting time
	if tx, err = dbWrite.Begin(); err != nil {
		return c.SendStatus(http.StatusUnprocessableEntity)
	}

	if err = tx.GetContext(c.Context(), &client, "SELECT * FROM clients WHERE id = ? FOR UPDATE", clientID); err != nil {
		tx.Rollback()
		return c.SendStatus(http.StatusNotFound)
	}

	switch request.Type {
	case "c":
		balance = client.Balance + request.Amount
	case "d":
		availableBalance := client.Limit + client.Balance

		if availableBalance == 0 {
			tx.Rollback()
			return c.SendStatus(http.StatusUnprocessableEntity)
		}

		if availableBalance-request.Amount < 0 {
			tx.Rollback()
			return c.SendStatus(http.StatusUnprocessableEntity)
		}

		balance = client.Balance - request.Amount
	}

	if _, err = tx.ExecContext(c.Context(), "UPDATE clients SET balance = ? WHERE id = ?", balance, clientID); err != nil {
		tx.Rollback()
		return c.SendStatus(http.StatusUnprocessableEntity)
	}

	if _, err = tx.ExecContext(
		c.Context(),
		"INSERT INTO transactions (client_id, amount, type, description, created_at) VALUES (?, ?, ?, ?, ?)",
		clientID,
		request.Amount,
		request.Type,
		request.Description,
		time.Now().UTC(),
	); err != nil {
		tx.Rollback()
		return c.SendStatus(http.StatusUnprocessableEntity)
	}
	tx.Commit()
	return c.JSON(PostTransactionResponse{
		Limit:   client.Limit,
		Balance: balance,
	})
}

// GetStatement returns the statement of a client
func GetStatement(c *fiber.Ctx) error {
	var (
		tx       godb.Tx
		err      error
		client   Client
		response GetStatementResponse
		clientID = c.Params("id")
	)

	// Desing decision: I'm not using defer to commit or rollback the transaction to avoid wasting time
	if tx, err = dbRead.Begin(); err != nil {
		return c.SendStatus(http.StatusNotFound)
	}

	if err = tx.GetContext(c.Context(), &client, "SELECT * FROM clients WHERE id = ? FOR UPDATE", clientID); err != nil {
		tx.Rollback()
		return c.SendStatus(http.StatusNotFound)
	}

	response.StatementAmout.Date = time.Now().UTC().Format(time.RFC3339Nano)
	response.StatementAmout.Limit = client.Limit
	response.StatementAmout.Total = client.Balance
	response.Transactions = []Transaction{}

	if err = tx.SelectContext(
		c.Context(),
		&response.Transactions,
		"SELECT amount, type, description, created_at FROM transactions WHERE client_id = ? ORDER BY created_at DESC LIMIT 10",
		clientID,
	); err != nil {
		tx.Rollback()
		return c.SendStatus(http.StatusNotFound)
	}
	tx.Rollback()
	return c.JSON(response)
}
