package cache

import (
	"context"
	"errors"
	"fmt"
	pool "github.com/bitleak/go-redis-pool/v3"
	"github.com/zeromicro/go-zero/core/jsonx"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/mathx"
	"github.com/zeromicro/go-zero/core/stat"
	"github.com/zeromicro/go-zero/core/syncx"
	"log"
	"math"
	"time"
)

type cacheMasterSlave struct {
	master         string
	slaves         []string
	rds            *pool.Pool
	expiry         time.Duration
	unstableExpiry mathx.Unstable
	notFoundExpiry time.Duration
	barrier        syncx.SingleFlight
	stat           *Stat
	errNotFound    error
}

func NewMasterSlave(master, mPass, sPass string, slaves []string, barrier syncx.SingleFlight, st *Stat,
	errNotFound error, opts ...Option) Cache {
	o := newOptions(opts...)
	p, err := pool.NewHA(&pool.HAConfig{
		Master:           master,
		Slaves:           slaves,
		Password:         mPass, // set master password
		ReadonlyPassword: sPass, // use password if no set
	})
	if err != nil {
		log.Fatal(err.Error())
	}

	return cacheMasterSlave{
		master:         master,
		slaves:         slaves,
		rds:            p,
		expiry:         o.Expiry,
		notFoundExpiry: o.NotFoundExpiry,
		barrier:        barrier,
		unstableExpiry: mathx.NewUnstable(expiryDeviation),
		stat:           st,
		errNotFound:    errNotFound,
	}
}

func (c cacheMasterSlave) Del(keys ...string) error {
	return c.DelCtx(context.Background(), keys...)
}

func (c cacheMasterSlave) DelCtx(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}

	logger := logx.WithContext(ctx)
	if _, err := c.rds.Del(ctx, keys...); err != nil {
		logger.Errorf("failed to clear cache with keys: %q, error: %v", formatKeys(keys), err)
		c.asyncRetryDelCache(keys...)
	}

	return nil
}

func (c cacheMasterSlave) Get(key string, val any) error {
	return c.GetCtx(context.Background(), key, val)
}

func (c cacheMasterSlave) GetCtx(ctx context.Context, key string, val any) error {
	err := c.doGetCache(ctx, key, val)
	if err == errPlaceholder {
		return c.errNotFound
	}

	return err
}

func (c cacheMasterSlave) IsNotFound(err error) bool {
	return errors.Is(err, c.errNotFound)
}

func (c cacheMasterSlave) Set(key string, val any) error {
	return c.SetCtx(context.Background(), key, val)
}

func (c cacheMasterSlave) SetCtx(ctx context.Context, key string, val any) error {
	return c.SetWithExpireCtx(ctx, key, val, c.aroundDuration(c.expiry))
}

func (c cacheMasterSlave) SetWithExpire(key string, val any, expire time.Duration) error {
	return c.SetWithExpireCtx(context.Background(), key, val, expire)
}

func (c cacheMasterSlave) SetWithExpireCtx(ctx context.Context, key string, val any, expire time.Duration) error {
	data, err := jsonx.Marshal(val)
	if err != nil {
		return err
	}

	return c.rds.Set(ctx, key, string(data), time.Duration(int(math.Ceil(expire.Seconds())))*time.Second).Err()
}

func (c cacheMasterSlave) String() string {
	return "master_slave"
}

func (c cacheMasterSlave) Take(val any, key string, query func(val any) error) error {
	return c.TakeCtx(context.Background(), val, key, query)
}

func (c cacheMasterSlave) TakeCtx(ctx context.Context, val any, key string, query func(val any) error) error {
	return c.doTake(ctx, val, key, query, func(v any) error {
		return c.SetCtx(ctx, key, v)
	})
}

func (c cacheMasterSlave) TakeWithExpire(val any, key string, query func(val any, expire time.Duration) error) error {
	return c.TakeWithExpireCtx(context.Background(), val, key, query)
}

func (c cacheMasterSlave) TakeWithExpireCtx(ctx context.Context, val any, key string, query func(val any, expire time.Duration) error) error {
	expire := c.aroundDuration(c.expiry)
	return c.doTake(ctx, val, key, func(v any) error {
		return query(v, expire)
	}, func(v any) error {
		return c.SetWithExpireCtx(ctx, key, v, expire)
	})
}

func (c cacheMasterSlave) aroundDuration(duration time.Duration) time.Duration {
	return c.unstableExpiry.AroundDuration(duration)
}

func (c cacheMasterSlave) asyncRetryDelCache(keys ...string) {
	AddCleanTask(func() error {
		_, err := c.rds.Del(context.Background(), keys...)
		return err
	}, keys...)
}

func (c cacheMasterSlave) doGetCache(ctx context.Context, key string, v any) error {
	data, err := c.rds.Get(ctx, key).Result()
	if err != nil {
		return err
	}

	return c.processCache(ctx, key, data, v)
}

func (c cacheMasterSlave) doTake(ctx context.Context, v any, key string,
	query func(v any) error, cacheVal func(v any) error) error {
	logger := logx.WithContext(ctx)
	val, fresh, err := c.barrier.DoEx(key, func() (any, error) {
		if err := c.doGetCache(ctx, key, v); err != nil {
			if err == errPlaceholder {
				return nil, c.errNotFound
			} else if err != c.errNotFound {
				// why we just return the error instead of query from db,
				// because we don't allow the disaster pass to the dbs.
				// fail fast, in case we bring down the dbs.
				return nil, err
			}

			if err = query(v); err == c.errNotFound {
				if err = c.setCacheWithNotFound(ctx, key); err != nil {
					logger.Error(err)
				}

				return nil, c.errNotFound
			} else if err != nil {
				c.stat.IncrementDbFails()
				return nil, err
			}

			if err = cacheVal(v); err != nil {
				logger.Error(err)
			}
		}

		return jsonx.Marshal(v)
	})
	if err != nil {
		return err
	}
	if fresh {
		return nil
	}

	// got the result from previous ongoing query.
	// why not call IncrementTotal at the beginning of this function?
	// because a shared error is returned, and we don't want to count.
	// for example, if the db is down, the query will be failed, we count
	// the shared errors with one db failure.
	c.stat.IncrementTotal()
	c.stat.IncrementHit()

	return jsonx.Unmarshal(val.([]byte), v)
}

func (c cacheMasterSlave) processCache(ctx context.Context, key, data string, v any) error {
	err := jsonx.Unmarshal([]byte(data), v)
	if err == nil {
		return nil
	}

	report := fmt.Sprintf("unmarshal cache key: %s, value: %s, error: %v", key, data, err)
	logger := logx.WithContext(ctx)
	logger.Error(report)
	stat.Report(report)
	if _, e := c.rds.Del(ctx, key); e != nil {
		logger.Errorf("delete invalid cache key: %s, value: %s, error: %v", key, data, e)
	}

	// returns errNotFound to reload the value by the given queryFn
	return c.errNotFound
}

func (c cacheMasterSlave) setCacheWithNotFound(ctx context.Context, key string) error {
	seconds := int(math.Ceil(c.aroundDuration(c.notFoundExpiry).Seconds()))
	_, err := c.rds.SetNX(ctx, key, notFoundPlaceholder, time.Duration(seconds)*time.Second).Result()
	return err
}
