package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	"log"
	"net/http"
	"time"

	_ "github.com/lib/pq"
)

const (
	dbDriver       = "postgres"
	dbURI          = "postgres://postgres:postgres@localhost/postgres?sslmode=disable"
	redisAddr      = "localhost:6379"
	redisDB        = 0
	redisCacheTime = time.Minute
	natsAddr       = "localhost:4222"
)

type Projects struct {
	ID        int       `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

type Goods struct {
	ID          int       `json:"id"`
	ProjectID   int       `json:"project_id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Priority    int       `json:"priority"`
	Removed     bool      `json:"removed"`
	CreatedAt   time.Time `json:"created_at"`
}

type NewPriority struct {
	NewPriority int `json:"newPriority"`
}

func main() {
	db, err := sql.Open(dbDriver, dbURI)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	redisClient := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", redisAddr, redisDB),
	})

	natsConn, err := nats.Connect(natsAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer natsConn.Close()

	router := mux.NewRouter()

	router.HandleFunc("/projects", listProjectsHandler(db)).Methods("GET")
	router.HandleFunc("/goods/list", listGoodsHandler(db, redisClient, natsConn)).Methods("GET")
	router.HandleFunc("/good/create", createGoodHandler(db, redisClient, natsConn)).Methods("POST")
	router.HandleFunc("/good/update", updateGoodHandler(db, redisClient, natsConn)).Methods("PATCH")
	router.HandleFunc("/good/delete", removeGoodHandler(db, natsConn)).Methods("DELETE")
	router.HandleFunc("/goods/reprioritize", reprioritizeGoodHandler(db, natsConn)).Methods("PATCH")

	log.Fatal(http.ListenAndServe(":8080", router))
}

func listProjectsHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var projects []Projects

		rows, err := db.Query("SELECT id, name, created_at FROM projects")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var project Projects
			err := rows.Scan(&project.ID, &project.Name, &project.CreatedAt)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			projects = append(projects, project)
		}

		if err := rows.Err(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		respondWithJSON(w, http.StatusOK, projects)
	}
}

func createGoodHandler(db *sql.DB, redisClient *redis.Client, natsConn *nats.Conn) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var good Goods
		err := json.NewDecoder(r.Body).Decode(&good)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var maxPriority int
		err = db.QueryRow("SELECT COALESCE(MAX(priority), 0) FROM goods").Scan(&maxPriority)
		if err != nil && err != sql.ErrNoRows {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		good.Priority = int(maxPriority) + 1

		tx, err := db.Begin()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer tx.Rollback()

		_, err = tx.Exec("INSERT INTO goods (name, description, priority, removed, created_at) VALUES ($1, $2, $3, $4, $5)",
			good.Name, good.Description, good.Priority, good.Removed, time.Now())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		err = tx.Commit()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		data, err := json.Marshal(good)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		redisClient.Set(context.Background(), fmt.Sprintf("goods: %d", good.ID), data, redisCacheTime)

		if err := natsConn.Publish("new_good_created", data); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		respondWithJSON(w, http.StatusCreated, good)
	}
}

func listGoodsHandler(db *sql.DB, redisClient *redis.Client, natsConn *nats.Conn) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var goods []Goods

		cachedGoods, err := redisClient.Get(context.Background(), "goods").Result()
		if err == nil {
			err = json.Unmarshal([]byte(cachedGoods), &goods)
			if err == nil {
				respondWithJSON(w, http.StatusOK, goods)
				return
			}
		}

		rows, err := db.Query("SELECT id, project_id, name, description, priority, removed, created_at FROM goods")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var good Goods
			err := rows.Scan(&good.ID, &good.ProjectID, &good.Name, &good.Description, &good.Priority, &good.Removed, &good.CreatedAt)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			goods = append(goods, good)
		}

		if err := rows.Err(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Кэширование данных в Redis
		data, err := json.Marshal(goods)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		redisClient.Set(context.Background(), "goods", data, redisCacheTime)

		if err := natsConn.Publish("list_goods", []byte(fmt.Sprintf("Goods list %s", goods))); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		respondWithJSON(w, http.StatusOK, goods)
	}
}

func updateGoodHandler(db *sql.DB, redisClient *redis.Client, natsConn *nats.Conn) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var good Goods
		err := json.NewDecoder(r.Body).Decode(&good)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		tx, err := db.Begin()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer tx.Rollback()

		_, err = tx.Exec("UPDATE goods SET name = $1, description = $2, priority = $3, removed = $4",
			good.Name, good.Description, good.Priority, good.Removed)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		err = tx.Commit()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		data, err := json.Marshal(good)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		redisClient.Set(context.Background(), fmt.Sprintf("goods:%d", good.ID), data, redisCacheTime)

		if err := natsConn.Publish("good_updated", data); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		respondWithJSON(w, http.StatusOK, good)
	}
}

func removeGoodHandler(db *sql.DB, natsConn *nats.Conn) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tx, err := db.Begin()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer tx.Rollback()

		_, err = tx.Exec("DELETE FROM goods")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		err = tx.Commit()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if err := natsConn.Publish("good_deleted", []byte(fmt.Sprintf("Goods with deleted"))); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

func reprioritizeGoodHandler(db *sql.DB, natsConn *nats.Conn) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var newPriority NewPriority
		var good Goods
		err := json.NewDecoder(r.Body).Decode(&newPriority)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		tx, err := db.Begin()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer tx.Rollback()

		_, err = tx.Exec("UPDATE goods SET priority = $1", newPriority.NewPriority)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		err = tx.Commit()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if err := natsConn.Publish("good_reprioritized",
			[]byte(fmt.Sprintf("Goods reprioritized to %d", newPriority.NewPriority))); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		response := struct {
			Priorities []struct {
				ID       int `json:"id"`
				Priority int `json:"priority"`
			} `json:"priorities"`
		}{
			Priorities: []struct {
				ID       int `json:"id"`
				Priority int `json:"priority"`
			}{
				{
					ID:       good.ID,
					Priority: newPriority.NewPriority,
				},
			},
		}

		respondWithJSON(w, http.StatusOK, response)
	}
}

func respondWithJSON(w http.ResponseWriter, statusCode int, data ...interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}
