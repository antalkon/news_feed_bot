package fetcher

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/antalkon/news_feed_bot/internal/model"
)

type ArticlesStorage interface {
	Store(ctx context.Context, article model.Article) error
}

type SourceProvider interface {
	// Возвращаем уже готовые источники, умеющие Fetch.
	Sources(ctx context.Context) ([]Source, error)
}

type Source interface {
	ID() int64
	Name() string
	Fetch(ctx context.Context) ([]model.Item, error)
}

type Fetcher struct {
	articles ArticlesStorage
	sources  SourceProvider

	fetchInterval  time.Duration
	filterKeywords []string
}

func New(
	articleStorage ArticlesStorage,
	sourceProvider SourceProvider,
	fetchInterval time.Duration,
	filterKeywords []string,
) *Fetcher {
	return &Fetcher{
		articles:       articleStorage,
		sources:        sourceProvider,
		fetchInterval:  fetchInterval,
		filterKeywords: filterKeywords,
	}
}

func (f *Fetcher) Start(ctx context.Context) error {
	ticker := time.NewTicker(f.fetchInterval)
	defer ticker.Stop()

	// Первый прогон сразу
	if err := f.Fetch(ctx); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := f.Fetch(ctx); err != nil {
				log.Printf("[ERROR] fetching items: %v", err)
			}
		}
	}
}

func (f *Fetcher) Fetch(ctx context.Context) error {
	sources, err := f.sources.Sources(ctx)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	for _, src := range sources {
		wg.Add(1)

		go func(src Source) {
			defer wg.Done()

			items, err := src.Fetch(ctx)
			if err != nil {
				log.Printf("[ERROR] fetching items from source %q: %v", src.Name(), err)
				return
			}
			if err := f.processItems(ctx, items); err != nil {
				log.Printf("[ERROR] processing items from source %q: %v", src.Name(), err)
				return
			}
		}(src)
	}
	wg.Wait()
	return nil
}

func (f *Fetcher) processItems(ctx context.Context, source Source, items []model.Item) error {
	for _, item := range items {
		item.Date = item.Date.UTC()

		if f.itemShouldBeSkipped(item) {
			log.Printf("[INFO] item %q (%s) from source %q should be skipped", item.Title, item.Link, source.Name())
			continue
		}

		if err := f.articles.Store(ctx, model.Article{
			SourceID:    source.ID(),
			Title:       item.Title,
			Link:        item.Link,
			Summary:     item.Summary,
			PublishedAt: item.Date,
		}); err != nil {
			return err
		}
	}

	return nil
}

func (f *Fetcher) itemShouldBeSkipped(item model.Item) bool {
	titleLower := strings.ToLower(item.Title)

	for _, keyword := range f.filterKeywords {
		kw := strings.ToLower(keyword)

		// По заголовку
		if strings.Contains(titleLower, kw) {
			return true
		}

		// По категориям (без учёта регистра)
		for _, cat := range item.Categories {
			if strings.EqualFold(cat, keyword) {
				return true
			}
		}
	}

	return false
}
