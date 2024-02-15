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

	"github.com/gofiber/fiber/v2"
)

var (
	dbRead    godb.DB
	dbWrite   godb.DB
	err       error
	ctx       = gocontext.FromContext(context.Background())
	routes    = []goweb.WebRoute{}
	dbConfigs = godb.DBConfig{
		Host:           os.Getenv("DB_HOST"),
		Port:           os.Getenv("DB_PORT"),
		User:           os.Getenv("DB_USER"),
		Password:       os.Getenv("DB_PASSWORD"),
		Database:       os.Getenv("DB_NAME"),
		DatabaseType:   godb.MySQLDB,
		ConnectTimeout: time.Second * 1,
	}
	API_PORT = os.Getenv("API_PORT")
)

func main() {
	golog.SetEnv(goenv.Local)

	if dbWrite, err = godb.NewDB(dbConfigs); err != nil {
		golog.Log().Error(ctx, fmt.Sprintf("failed to connect to the database (write). Cause: %s", err))
		return
	}
	defer dbWrite.Close()

	dbWrite.SetMaxOpenConns(45)
	dbWrite.SetMaxIdleConns(10)
	dbWrite.SetConnMaxLifetime(time.Minute * 1)
	dbWrite.SetConnMaxIdleTime(time.Minute * 1)

	if dbRead, err = godb.NewDB(dbConfigs); err != nil {
		golog.Log().Error(ctx, fmt.Sprintf("failed to connect to the database (read). Cause: %s", err))
		return
	}
	defer dbRead.Close()

	dbRead.SetMaxOpenConns(15)
	dbRead.SetMaxIdleConns(10)
	dbRead.SetConnMaxLifetime(time.Minute * 1)
	dbRead.SetConnMaxIdleTime(time.Minute * 1)

	ws := goweb.NewWebServer(goweb.DefaultConfig(goweb.WebServerDefaultConfig{
		AppName: "rinha-de-backend-mysql",
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

	if err := ws.Listen(fmt.Sprintf(":%s", API_PORT)); err != nil {
		golog.Log().Error(ctx, fmt.Sprintf("failed to graceful shutdown. Cause: %s", err))
	}
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
	Amount      int64  `db:"amount"      json:"valor"`
	Type        string `db:"type"        json:"tipo"`
	Description string `db:"description" json:"descricao"`
	Date        string `db:"date"        json:"realizada_em"`
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
		// tx       godb.Tx
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

	if err = dbWrite.GetContext(c.Context(), &client, "SELECT * FROM clients WHERE id = ?", clientID); err != nil {
		return c.SendStatus(http.StatusNotFound)
	}

	switch request.Type {
	case "c":
		balance = client.Balance + request.Amount
	case "d":
		availableBalance := client.Limit + client.Balance

		if availableBalance == 0 {
			return c.SendStatus(http.StatusUnprocessableEntity)
		}

		if availableBalance-request.Amount < 0 {
			return c.SendStatus(http.StatusUnprocessableEntity)
		}

		balance = client.Balance - request.Amount
	}

	// Desging decision: I'm not using transactions to make it simple and fast!!!
	if _, err = dbWrite.ExecContext(c.Context(), "UPDATE clients SET balance = ? WHERE id = ?", balance, clientID); err != nil {
		return c.SendStatus(http.StatusUnprocessableEntity)
	}

	if _, err = dbWrite.ExecContext(
		c.Context(),
		"INSERT INTO transactions (client_id, amount, type, description, date) VALUES (?, ?, ?, ?, ?)",
		clientID,
		request.Amount,
		request.Type,
		request.Description,
		time.Now().UTC().Format(time.RFC3339Nano),
	); err != nil {
		return c.SendStatus(http.StatusUnprocessableEntity)
	}

	return c.JSON(PostTransactionResponse{
		Limit:   client.Limit,
		Balance: balance,
	})
}

// GetStatement returns the statement of a client
func GetStatement(c *fiber.Ctx) error {
	var (
		client   Client
		response GetStatementResponse
		clientID = c.Params("id")
	)

	if err := dbRead.GetContext(c.Context(), &client, "SELECT * FROM clients WHERE id = ?", clientID); err != nil {
		return c.SendStatus(http.StatusNotFound)
	}

	response.StatementAmout.Date = time.Now().UTC().Format(time.RFC3339Nano)
	response.StatementAmout.Limit = client.Limit
	response.StatementAmout.Total = client.Balance
	response.Transactions = []Transaction{}

	if err := dbRead.SelectContext(
		c.Context(),
		&response.Transactions,
		"SELECT amount, type, description, date FROM transactions WHERE client_id = ? ORDER BY date DESC LIMIT 10",
		clientID,
	); err != nil {
		return c.SendStatus(http.StatusNotFound)
	}

	return c.JSON(response)
}
