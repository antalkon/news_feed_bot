package storage

import (
	"context"

	"github.com/antalkon/news_feed_bot/internal/model"
	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
)

type SourcePostgressStorage struct {
	db *sqlx.DB
}

func (s *SourcePostgressStorage) Sources(ctx context.Context) ([]model.Source, error) {
	conn, err := s.db.Connx(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	var dbSources []dbSource
	if err := conn.SelectContext(ctx, &dbSources, "SELECT * FROM sources"); err != nil {
		return nil, err
	}

	return lo.Map(dbSources, func(dbSource dbSource, _ int) model.Source {
		return model.Source(dbSource)
	}), nil
}

func (s *SourcePostgressStorage) SourceByID(ctx context.Context, id int64) (model.Source, error) {
	conn, err := s.db.Connx(ctx)
	if err != nil {
		return model.Source{}, err
	}
	defer conn.Close()

	var dbSource dbSource
	if err := conn.GetContext(ctx, &dbSource, "SELECT * FROM sources WHERE id=$1", id); err != nil {
		return model.Source{}, err
	}
	return (*model.Source)(&dbSource), nil
}

func (s *SourcePostgressStorage) Add(ctx context.Context, source model.Source) (int64, error) {
	conn, err := s.db.Connx(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	var dbSource dbSource
	if err := conn.GetContext(ctx, &dbSource, "INSERT INTO sources (name, feed_url) VALUES ($1, $2) RETURNING id", source.Name, source.FeedURL); err != nil {
		return 0, err
	}

	return dbSource.ID, nil
}

func (s *SourcePostgressStorage) Delete(ctx context.Context, id int64) error {
	conn, err := s.db.Connx(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "DELETE FROM sources WHERE id=$1", id); err != nil {
		return err
	}

	return nil
}

type dbSource struct {
	ID      int64  `db:"id"`
	Name    string `db:"name"`
	FeedURL string `db:"feed_url"`
	AddedAt string `db:"created_at"`
}
