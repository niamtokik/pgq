# pgq

An Erlang library to use [pgq](https://pgq.github.io/extension/pgq)
postgresql extension.

This project was created to understand `pgq` and how to study queueing
systems on PostgreSQL. `pgq` application should not be used in
production for the moment.

## Build

```erlang
rebar3 compile
```

## Installation and Boostrap

Download and install pgq postgresql extension.

```sh
git clone https://github.com/pgq/pgq
make 
make install
```

Create a new database (or use an existing one) and enable the
extension.

```sql
CREATE DATABASE my_database;
\c my_database
CREATE EXTENSION pgq;
```

## Usage

### Main Interface

TODO

### Low Level Interface

```erlang
% Requires an active and valid epgsql connection.
{ok, C} = epgsql:connect(Args).

% check if pgq is installed.
true = pgq_low:is_installed(C).

% check the version
{ok, Version} = pgq_low:version(C).

% create a queue
{ok, _} = pgq_low:create_queue(C, "new_queue").

% insert data
{ok, _} = pgq_low:insert_event(C, "new_queue", "event_type", "event_value").
```

### Producer Process

TODO

### Consumer Process

TODO

### PubSub Pattern

TODO

### Topics Pattern

TODO

### Routing Pattern

TODO

### RPC Pattern

TODO

## References and Resources

[pgq official documentation](https://pgq.github.io/extension/pgq)

[pgq official repository](https://github.com/pgq/pgq)
