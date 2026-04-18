package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	// Connect to Redka via Unix socket using redis/go-redis/v9.
	rdb := redis.NewClient(&redis.Options{
		Addr: "/tmp/redka.sock",
	})

	ctx := context.Background()

	// Ping test.
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("failed to ping: %v", err)
	}
	fmt.Println("PING: PONG")

	// String operations test.
	if err := rdb.Set(ctx, "hello", "world", 0).Err(); err != nil {
		log.Fatalf("failed to SET: %v", err)
	}
	fmt.Println("SET: hello = world")

	val, err := rdb.Get(ctx, "hello").Result()
	if err != nil {
		log.Fatalf("failed to GET: %v", err)
	}
	fmt.Println("GET: hello =", val)

	// INCR test.
	if err := rdb.Set(ctx, "counter", "10", 0).Err(); err != nil {
		log.Fatalf("failed to SET counter: %v", err)
	}
	cnt, err := rdb.Incr(ctx, "counter").Result()
	if err != nil {
		log.Fatalf("failed to INCR: %v", err)
	}
	fmt.Println("INCR: counter =", cnt)

	// Expire test.
	if err := rdb.Set(ctx, "temp", "value", 0).Err(); err != nil {
		log.Fatalf("failed to SET temp: %v", err)
	}
	if err := rdb.Expire(ctx, "temp", time.Second).Err(); err != nil {
		log.Fatalf("failed to EXPIRE: %v", err)
	}
	ttl, err := rdb.TTL(ctx, "temp").Result()
	if err != nil {
		log.Fatalf("failed to TTL: %v", err)
	}
	fmt.Println("TTL: temp =", ttl)

	// Hash test.
	if err := rdb.HSet(ctx, "user:1", map[string]interface{}{
		"name": "Alice",
		"age":  30,
	}).Err(); err != nil {
		log.Fatalf("failed to HSET: %v", err)
	}
	fmt.Println("HSET: user:1 {name: Alice, age: 30}")

	user, err := rdb.HGetAll(ctx, "user:1").Result()
	if err != nil {
		log.Fatalf("failed to HGETALL: %v", err)
	}
	fmt.Println("HGETALL: user:1 =", user)

	// List test.
	if err := rdb.RPush(ctx, "tasks", "task1", "task2", "task3").Err(); err != nil {
		log.Fatalf("failed to RPUSH: %v", err)
	}
	fmt.Println("RPUSH: tasks = [task1, task2, task3]")

	tasks, err := rdb.LRange(ctx, "tasks", 0, -1).Result()
	if err != nil {
		log.Fatalf("failed to LRANGE: %v", err)
	}
	fmt.Println("LRANGE: tasks =", tasks)

	// Delete keys.
	if err := rdb.Del(ctx, "hello", "counter", "temp", "user:1", "tasks").Err(); err != nil {
		log.Fatalf("failed to DEL: %v", err)
	}
	fmt.Println("DEL: deleted 5 keys")

	// Close connection.
	if err := rdb.Close(); err != nil {
		log.Fatalf("failed to close: %v", err)
	}
	fmt.Println("Connection closed.")
}