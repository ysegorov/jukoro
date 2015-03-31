# Jukoro

## About

A small library to keep code under control.

**NB. No docstrings for now. Sorry.**


## Tests

For `jukoro.redis` tests it is expected Redis to be running locally on standard
port (`redis://localhost:6379`).

For `jukoro.pg` tests you will have to create PostgreSQL database named
`jukoro_test` or specify db connection uri using `PG_URI` environment
variable.

To perform `jukoro.pg` tests a separate time-stamped schema prefixed with
`ju_` will be created (something like `ju_20150330124323`).

```bash
$ pip install nose

```

Run tests:

```bash
$ nosetests

```

or

```bash
$ PG_URI="postgresql://username:userpass@localhost:5432/test_db" nosetests

```

You can keep PostgreSQL test db schema created during tests with `KEEP_SCHEMA`
environment variable:

```bash
$ KEEP_SCHEMA=y nosetests

```


## License

MIT
