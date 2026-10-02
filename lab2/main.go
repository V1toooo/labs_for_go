package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Film struct {
	ID       int    `json:"id"`
	Title    string `json:"title"`
	Year     int    `json:"year"`
	Director string `json:"director"`
}

func fetchFilm(ctx context.Context, id int, timeout time.Duration) (Film, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	url := fmt.Sprintf("https://homeworksite.site/%d/info.0.json", id)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Film{}, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return Film{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return Film{}, fmt.Errorf("статус %d", resp.StatusCode)
	}

	var f Film
	if err := json.NewDecoder(resp.Body).Decode(&f); err != nil {
		return Film{}, fmt.Errorf("битый JSON: %w", err)
	}
	return f, nil
}

type Result struct {
	ID   int
	Film Film
	Err  error
}

func worker(ctx context.Context, timeout time.Duration, jobs <-chan int, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()
	for id := range jobs {
		film, err := fetchFilm(ctx, id, timeout)
		results <- Result{ID: id, Film: film, Err: err}
	}
}

func main() {
	from := flag.Int("from", -1, "id первого фильма")
	to := flag.Int("to", -1, "id последнего фильма")
	workers := flag.Int("workers", 10, "сколько воркеров")
	timeout := flag.Duration("timeout", 5*time.Second, "таймаут запроса")
	flag.Parse()

	if *from == -1 || *to == -1 {
		fmt.Fprintln(os.Stderr, "ошибка: нужно указать --from и --to")
		os.Exit(1)
	}
	if *from > *to {
		fmt.Fprintln(os.Stderr, "ошибка: --from не может быть больше --to")
		os.Exit(1)
	}
	if *workers <= 0 {
		fmt.Fprintln(os.Stderr, "ошибка: --workers должен быть больше 0")
		os.Exit(1)
	}
	if *timeout <= 0 {
		fmt.Fprintln(os.Stderr, "ошибка: --timeout должен быть больше 0")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	jobs := make(chan int)
	results := make(chan Result)
	var wg sync.WaitGroup

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go worker(ctx, *timeout, jobs, results, &wg)
	}

	go func() {
		defer close(jobs)
		for id := *from; id <= *to; id++ {
			select {
			case jobs <- id:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	for res := range results {
		if res.Err != nil {
			fmt.Fprintf(os.Stderr, "id %d: ошибка: %v\n", res.ID, res.Err)
		} else {
			fmt.Printf("%d — %s — %d — %s\n", res.Film.ID, res.Film.Title, res.Film.Year, res.Film.Director)
		}
	}
}
