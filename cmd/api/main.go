package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
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
	API_PORT  = os.Getenv("API_PORT")
	LOCK_PORT = os.Getenv("LOCK_PORT")
	LOCK_HOST = os.Getenv("LOCK_HOST")
)

func main() {
	golog.SetEnv(goenv.Local)

	if os.Getenv("SERVER_TYPE") == "api" {
		runAPIServer()
	}
	runLockServer()
}

func runAPIServer() {
	if dbWrite, err = godb.NewDB(dbConfigs); err != nil {
		golog.Log().Error(ctx, fmt.Sprintf("failed to connect to the database (write). Cause: %s", err))
		return
	}
	defer dbWrite.Close()

	dbWrite.SetMaxOpenConns(45) // 50 60
	dbWrite.SetMaxIdleConns(10) // 5
	dbWrite.SetConnMaxLifetime(time.Minute * 1)
	dbWrite.SetConnMaxIdleTime(time.Minute * 1)

	if dbRead, err = godb.NewDB(dbConfigs); err != nil {
		golog.Log().Error(ctx, fmt.Sprintf("failed to connect to the database (read). Cause: %s", err))
		return
	}
	defer dbRead.Close()

	dbRead.SetMaxOpenConns(15) // 50 60
	dbRead.SetMaxIdleConns(10) // 5
	dbRead.SetConnMaxLifetime(time.Minute * 1)
	dbRead.SetConnMaxIdleTime(time.Minute * 1)

	ws := goweb.NewWebServer(goweb.DefaultConfig(goweb.WebServerDefaultConfig{
		AppName: "rinha-de-backend-api-server",
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
func PostTransactions(ctx *fiber.Ctx) error {
	var (
		// tx       godb.Tx
		err      error
		client   Client
		body     []byte
		balance  int64
		clientID string
		request  PostTransactionRequest
	)

	body = ctx.Body()
	clientID = ctx.Params("id")

	// Just to make sure the body is not empty
	if len(body) == 0 {
		return ctx.SendStatus(http.StatusUnprocessableEntity)
	}

	// Unmarshal the body into the transaction struct
	if err := json.Unmarshal(body, &request); err != nil {
		return ctx.SendStatus(http.StatusUnprocessableEntity)
	}

	// Validations
	if request.Type != "c" && request.Type != "d" {
		return ctx.SendStatus(http.StatusUnprocessableEntity)
	}

	descriptionLen := len(request.Description)
	if descriptionLen < 1 || descriptionLen > 10 {
		return ctx.SendStatus(http.StatusUnprocessableEntity)
	}

	// // aquire lock
	// if _, err := http.Get(fmt.Sprintf("http://%s:%s/lock/%s", LOCK_HOST, LOCK_PORT, clientID)); err != nil {
	// 	return ctx.SendStatus(http.StatusUnprocessableEntity)
	// }
	// // defer release lock
	// defer func() {
	// 	if req, err := http.NewRequest("DELETE", fmt.Sprintf("http://%s:%s/lock/%s", LOCK_HOST, LOCK_PORT, clientID), nil); err == nil {
	// 		http.DefaultClient.Do(req)
	// 	}
	// }()

	// Get the client
	if err = dbWrite.GetContext(ctx.Context(), &client, "SELECT * FROM clients WHERE id = ?", clientID); err != nil {
		return ctx.SendStatus(http.StatusNotFound)
	}

	switch request.Type {
	case "c":
		balance = client.Balance + request.Amount
	case "d":
		availableBalance := client.Limit + client.Balance

		if availableBalance == 0 {
			return ctx.SendStatus(http.StatusUnprocessableEntity)
		}

		if availableBalance-request.Amount < 0 {
			return ctx.SendStatus(http.StatusUnprocessableEntity)
		}

		balance = client.Balance - request.Amount
	}

	// if tx, err = dbWrite.Begin(); err != nil {
	// 	return ctx.SendStatus(http.StatusUnprocessableEntity)
	// }

	if _, err = dbWrite.ExecContext(ctx.Context(), "UPDATE clients SET balance = ? WHERE id = ?", balance, clientID); err != nil {
		return ctx.SendStatus(http.StatusUnprocessableEntity)
	}

	if _, err = dbWrite.ExecContext(
		ctx.Context(),
		"INSERT INTO transactions (client_id, amount, type, description, date) VALUES (?, ?, ?, ?, ?)",
		clientID, request.Amount, request.Type, request.Description, time.Now().UTC().Format(time.RFC3339Nano),
	); err != nil {
		return ctx.SendStatus(http.StatusUnprocessableEntity)
	}

	// tx.Commit()
	return ctx.JSON(PostTransactionResponse{
		Limit:   client.Limit,
		Balance: balance,
	})
}

// GetStatement returns the statement of a client
func GetStatement(ctx *fiber.Ctx) error {
	var (
		client   Client
		response GetStatementResponse
		clientID = ctx.Params("id")
	)

	// // aquire lock
	// if _, err := http.Get(fmt.Sprintf("http://%s:%s/lock/%s", LOCK_HOST, LOCK_PORT, clientID)); err != nil {
	// 	return ctx.SendStatus(http.StatusUnprocessableEntity)
	// }
	// // defer release lock
	// defer func() {
	// 	if req, err := http.NewRequest("DELETE", fmt.Sprintf("http://%s:%s/lock/%s", LOCK_HOST, LOCK_PORT, clientID), nil); err == nil {
	// 		http.DefaultClient.Do(req)
	// 	}
	// }()

	if err := dbRead.GetContext(ctx.Context(), &client, "SELECT * FROM clients WHERE id = ?", clientID); err != nil {
		return ctx.SendStatus(http.StatusNotFound)
	}

	response.StatementAmout.Date = time.Now().UTC().Format(time.RFC3339Nano)
	response.StatementAmout.Limit = client.Limit
	response.StatementAmout.Total = client.Balance
	response.Transactions = []Transaction{}

	// if err := dbRead.SelectContext(
	// 	ctx.Context(),
	// 	&response.Transactions,
	// 	"SELECT amount, type, description, date FROM transactions WHERE client_id = ? ORDER BY date DESC LIMIT 10",
	// 	clientID,
	// ); err != nil {
	// 	fmt.Println(err)
	// 	return ctx.SendStatus(http.StatusNotFound)
	// }

	return ctx.JSON(response)
}

var (
	mutex  sync.RWMutex
	locker = map[int]time.Time{}
)

func runLockServer() {
	ws := goweb.NewWebServer(goweb.DefaultConfig(goweb.WebServerDefaultConfig{
		AppName: "rinha-de-backend-lock-server",
	}))

	routes = append(routes, goweb.WebRoute{
		Method:   "GET",
		Path:     "/lock/:id",
		Handlers: []func(c *fiber.Ctx) error{GetLock},
	}, goweb.WebRoute{
		Method:   "DELETE",
		Path:     "/lock/:id",
		Handlers: []func(c *fiber.Ctx) error{ReleaseLock},
	})

	ws.AddRoutes(routes...)
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		for range ticker.C {
			mutex.Lock()
			for k, v := range locker {
				if time.Since(v) > time.Second*9 {
					delete(locker, k)
				}
			}
			mutex.Unlock()
		}
	}()

	if err := ws.Listen(fmt.Sprintf(":%s", LOCK_PORT)); err != nil {
		golog.Log().Error(ctx, fmt.Sprintf("failed to graceful shutdown. Cause: %s", err))
	}
}

// GetLock
func GetLock(ctx *fiber.Ctx) error {
	var (
		id             int
		err            error
		maxRetries     = 10
		totalRetriries = 0
	)

	if id, err = strconv.Atoi(ctx.Params("id")); err != nil {
		return ctx.Status(http.StatusInternalServerError).SendString(err.Error())
	}

	mutex.RLock()
	for {
		if _, ok := locker[id]; !ok {
			break
		}
		totalRetriries++
		time.Sleep(time.Second)
		if totalRetriries == maxRetries {
			mutex.RUnlock()
			return ctx.SendStatus(http.StatusUnprocessableEntity)
		}
	}
	mutex.RUnlock()
	mutex.Lock()
	defer mutex.Unlock()
	locker[id] = time.Now()
	return ctx.SendStatus(http.StatusOK)
}

// ReleaseLock
func ReleaseLock(ctx *fiber.Ctx) error {
	var (
		id  int
		err error
	)

	if id, err = strconv.Atoi(ctx.Params("id")); err != nil {
		return ctx.SendStatus(http.StatusInternalServerError)
	}

	mutex.Lock()
	defer mutex.Unlock()
	delete(locker, id)
	return ctx.SendStatus(http.StatusOK)
}
