Attolytics
==========

This fork is an updated version of the [base repository](https://github.com/ttencate/attolytics/) that supports compilation on modern Rust nightly versions (tested with rustc 1.77.0-nightly). The packages were updated to their newest versions. It also makes the systemd dependency optional and adds Docker support.

Attolytics (a portmanteau of the SI prefix "atto" meaning 10<sup>-18</sup> and
"analytics") is a small web service that receives analytics events and inserts
them into a PostgreSQL database. These events can subsequently be processed and
displayed using frameworks like [Cube.js](https://cube.dev/), but that is
outside the scope of this application.

Attolytics is written in [Rust](https://rust-lang.org/) using the
[Rocket](https://rocket.rs/) framework.

Compiling
---------

* Install Rust nightly, e.g.
  [using rustup](https://www.rust-lang.org/tools/install).

* Clone this repository:

        $ git clone https://github.com/ttencate/attolytics

* Compile the binary:

        $ cargo build --release

Running
-------

* Set up PostgreSQL using some appropriate guide for your system.

* Create a database, e.g. owned by your current user and named `attolytics`:

        $ createdb -o $(whoami) attolytics

* Create a schema file, typically named `schema.conf.yaml`. This file tells
  Attolytics which tables exist, and which apps write to which tables. See
  [`schema-example.conf.yaml`](schema-example.conf.yaml) for a documented
  example of the format.

* Run the executable, passing it the location of your schema file and the URL
  of your database:

        $ ./target/release/attolytics --schema ./schema.conf.yaml --db_url postgres://$(whoami)@localhost/attolytics

  For full documentation of supported options, run:

        $ ./target/release/attolytics --help

Deploying
---------

To compile attolytics with support for systemd notifications, use the following build command:
        
        $ cargo build --release --features systemd

This has been made an optional feature as on some systems (e.g. Docker containers), it's tricky to get access to libsystemd using pkg-config.

Systemd launch notifications are supported. So to run Attolytics on a Linux
machine with systemd behind an nginx proxy, a unit file like the following can
be used:

**`/etc/systemd/system/attolytics.service`**

    [Unit]
    Description=Attolytics analytics events ingestion service
    Requires=network.target
    After=network.target

    [Service]
    Type=notify
    NotifyAccess=main
    WorkingDirectory=/var/www/attolytics.frozenfractal.com
    ExecStart=/path/to/attolytics --schema /path/to/schema.conf.yaml --db_url postgres://attolytics@%%2Frun%%2Fpostgresql --port 8005 --verbose
    User=attolytics
    Group=attolytics
    Restart=on-failure

    [Install]
    WantedBy=nginx.service

Note that a warning will be emitted in the logs:

    Warning: environment is 'production', but no `secret_key` is configured

This is harmless, because this `secret_key` is not related to the per-app
`secret_key` specified it the schema. This one here is only [used by the Rocket
framework](https://rocket.rs/v0.4/guide/requests/#secret-key) to encrypt
cookies, but Attolytics doesn't use cookies.

Here is the corresponding nginx configuration:

**`/etc/nginx/sites-enabled/attolytics.conf`**

    upstream attolytics {
      server 127.0.0.1:8005 fail_timeout=0;
    }

    server {
      server_name attolytics.example.com;
      listen 443 ssl;

      location / {
        proxy_pass http://attolytics;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $remote_addr;
        proxy_set_header X-Forwarded-Proto $scheme;
      }

      ssl_certificate /path/to/fullchain.pem;
      ssl_certificate_key /path/to/privkey.pem;
    }

REST API
--------

Events can be inserted into the database by making an HTTP POST request. One
endpoint exists for every event type of every app:

    POST /apps/<app_id>/events
    Content-Type: application/json

    {
      "secret_key": "<app_secret_key>",
      "events": [
        ...
      ]
    }

The `events` array contains the events to be uploaded. Each event is an object,
which must contain these fields:

* `_t`: name of the table to insert into

The remainder of the fields must have keys matching column names in PostgreSQL.
The corresponding values must be of the correct type for those columns.

Continuing with the above example of the `game_events` table:

      "events": [
        {"_t": "events", "timestamp": 1554130180, "event_type": "game_start"},
        {"_t": "events", "timestamp": 1554130213, "event_type": "game_end", "score": 42}
      ]

Schema changes
--------------

If you want to add, remove or alter columns in a table, this requires some
manual work:

* Stop the server.
* Update the configuration file.
* Update the database using `ALTER TABLE` statements.
* Start the server.
