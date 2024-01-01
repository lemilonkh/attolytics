#![feature(decl_macro)]
#![feature(never_type)]
#![feature(proc_macro_hygiene)]

#[macro_use] extern crate rocket;

use std::error::Error;
use std::fmt::Display;
use std::fs;
use std::ops::Deref;
use std::process::exit;

use clap::{arg, Command, value_parser};
use postgres::NoTls;
use r2d2::Pool;
use r2d2_postgres::PostgresConnectionManager;
use rocket::config::LogLevel;
use rocket::data::{Limits, ToByteUnit};
use rocket::figment::providers::Env;
use rocket::{Config, State};
use rocket::http::{Method, Status, HeaderMap};
use rocket::outcome::Outcome;
use rocket::request::{FromRequest, Request};
use rocket::response::Responder;
use rocket::serde::json::Json;
use serde::Deserialize;

#[cfg(feature = "systemd")]
use rocket::fairing::AdHoc;

use schema::{App, Schema};
use db::DbError;

mod schema;
mod db;
mod types;

#[derive(Debug, Deserialize)]
struct EventPostData {
    secret_key: String,
    events: Vec<serde_json::Value>,
}

#[derive(Debug)]
struct Headers<'a>(&'a HeaderMap<'a>);

#[rocket::async_trait]
impl<'r> FromRequest<'r> for Headers<'r> {
    type Error = !;
    async fn from_request(request: &'r Request<'_>) -> rocket::request::Outcome<Self, Self::Error> {
        Outcome::Success(Headers(request.headers()))
    }
}

impl<'a> Deref for Headers<'a> {
    type Target = &'a HeaderMap<'a>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

fn events_cors_options(app: &App) -> rocket_cors::Cors {
    let allowed_origins = if app.access_control_allow_origin == "*" {
        rocket_cors::AllowedOrigins::all()
    } else {
        rocket_cors::AllowedOrigins::some_exact(&[&app.access_control_allow_origin])
    };
    rocket_cors::CorsOptions {
        allowed_origins,
        allowed_methods: vec![Method::Post].into_iter().map(From::from).collect(),
        ..Default::default()
    }.to_cors().expect("valid CORS options")
}

#[options("/apps/<app_id>/events")]
fn events_options<'r, 'o: 'r>(app_id: String, schema: &State<Schema>)
    -> Option<impl Responder<'r, 'o>>
{
    let app = schema.apps.get(&app_id)?;
    Some(events_cors_options(app).respond_owned(|guard| guard.responder("".to_string())))
}

#[post("/apps/<app_id>/events", format = "json", data = "<data>")]
fn events_post<'r, 'o: 'r>(
    app_id: String,
    headers: Headers<'r>,
    data: Json<EventPostData>,
    schema: &'r State<Schema>,
    db_conn_pool: &'r State<Pool<PostgresConnectionManager<NoTls>>>
) -> Option<impl Responder<'r, 'o>> {
    // There should be a way to get rid of the clone() but I'm tired of fighting the borrow checker
    // over it.
    let app = schema.apps.get(&app_id)?.clone();
    Some(events_cors_options(&app).respond_owned(move |guard| {
        if data.secret_key != app.secret_key {
            return Err(Status::Forbidden);
        }

        for event in &data.events {
            let table_name = event["_t"].as_str()
                .ok_or(Status::BadRequest)?
                .to_owned();
            if !app.tables.contains(&table_name) {
                return Err(Status::NotFound);
            }
        }

        let mut conn = db_conn_pool.get()
            .map_err(|err| {
                println!("error connecting to database: {}", err);
                Status::InternalServerError
            })?;
        let mut trans = conn.transaction()
            .map_err(|err| {
                println!("error starting transaction: {}", err);
                Status::InternalServerError
            })?;

        for event in &data.events {
            let table_name = event["_t"].as_str().unwrap();
            let table = schema.tables.get(table_name)
                .ok_or(Status::InternalServerError)?; // Table is in app.tables so it must be here.
            db::insert_event(&table, &mut trans, &event, &*headers)
                .map_err(|err| {
                    println!("error inserting event into database: {}", err);
                    match err {
                        DbError::ConversionError(_, _) => Status::BadRequest,
                        _ => Status::InternalServerError
                    }
                })?;
        }

        trans.commit()
            .map_err(|err| {
                println!("error committing transaction: {}", err);
                Status::InternalServerError
            })?;

        Ok(guard.responder("".to_string()))
    }))
}

#[derive(Debug)]
struct RunError(String);

impl Display for RunError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl Error for RunError {}

async fn run() -> Result<(), RunError> {
    let matches = Command::new("Attolytics")
        .bin_name("attolytics")
        .author(clap::crate_authors!())
        .version(clap::crate_version!())
        .about("A simple web server that stores analytics events into a database")
        // .setting(AppSettings::NextLineHelp)
        .arg(arg!(--schema <SCHEMA>)
            .short('s')
            .value_name("path/to/schema.conf.yaml")
            .help("Schema configuration file to use")
            .default_value("./schema.conf.yaml"))
        .arg(arg!(--db_url <DB_URL>)
             .short('d')
             .value_name("postgres://user:pass@host:port/database")
             .help("URL of the PostgreSQL database; see https://github.com/sfackler/rust-postgres#connecting for the format")
             .required(true))
        .arg(arg!(--host <HOST>)
             .short('h')
             .value_name("host")
             .help("Hostname or IP address to listen on")
             .default_value("localhost"))
        .arg(arg!(--port <PORT>)
             .short('p')
             .value_name("port_number")
             .help("Port number to listen on")
             .default_value("8000")
             .value_parser(value_parser!(u16).range(1..)))
        .arg(arg!(-v --verbose ... "Produce more verbose logging; may be given up to 2 times"))
        .arg(arg!(-q --quiet ... "Produce no output"))
        .get_matches();

    let schema_file_name = matches.get_one::<String>("schema").unwrap();
    let schema_yaml_str = fs::read_to_string(schema_file_name)
        .map_err(|err| RunError(format!("failed to read schema file {}: {}", schema_file_name, err)))?;
    let schema = Schema::from_yaml(&schema_yaml_str)
        .map_err(|err| RunError(format!("failed to parse schema file {}: {}", schema_file_name, err)))?;

    let manager = PostgresConnectionManager::new(matches.get_one::<String>("db_url").unwrap().to_owned().parse().unwrap(), NoTls);
        // .map_err(|err| RunError(format!("failed to open database: {}", err)))?;
    let db_conn_pool = Pool::new(manager)
        .map_err(|err| RunError(format!("failed to create connection pool: {}", err)))?;

    let mut conn = db_conn_pool.get()
        .map_err(|err| RunError(format!("failed to create database connection: {}", err)))?;
    db::create_tables(&schema, &mut conn)
        .map_err(|err| RunError(format!("failed to initialize database tables: {}", err)))?;

    let verbosity = 1i32 + *matches.get_one::<u8>("verbose").unwrap() as i32 - *matches.get_one::<u8>("quiet").unwrap() as i32;
    let logging_level = match verbosity {
        0 => LogLevel::Off,
        1 => LogLevel::Critical,
        2 => LogLevel::Normal,
        3 => LogLevel::Debug,
        _ => if verbosity < 0 { LogLevel::Off } else { LogLevel::Debug },
    };
    let config = Config::figment()
        .merge(Env::prefixed("APP_").global())
        .merge(("address", matches.get_one::<String>("host").unwrap()))
        .merge(("port", *matches.get_one::<u16>("port").unwrap()))
        .merge(("keep_alive", 0))
        .merge(("log_level", logging_level))
        .merge(("limits", Limits::default().limit("json", 32.kibibytes())));

    #[allow(unused_mut)]
    let mut rocket = rocket::custom(config)
        .manage(schema)
        .manage(db_conn_pool)
        .mount("/", routes![
            events_options,
            events_post,
        ]);

    #[cfg(feature = "systemd")]
    {
        // "A launch callback, represented by the Fairing::on_launch() method, is called immediately
        // before the Rocket application has launched. At this point, Rocket has opened a socket for
        // listening but has not yet begun accepting connections."
        // It would be better if we could wait for the latter too, but there seems to be no support for
        // that in Rocket.
        rocket = rocket.attach(AdHoc::on_liftoff("systemd launch notifier", |_| Box::pin(async move {
            match systemd::daemon::notify(true /* unset_environment */, [(systemd::daemon::STATE_READY, "1")].iter()) {
                Ok(true) => {},
                Ok(false) => eprintln!("failed to contact systemd"),
                Err(err) => eprintln!("failed to notify systemd of launch: {}", err),
            }
        })));
    }

    let res = rocket.launch().await;
    if res.is_err() {
        return Err(RunError(format!("failed to launch web server: {}", res.err().unwrap())));
    }
    Ok(())
}

#[rocket::main]
async fn main() {
    if let Err(RunError(msg)) = run().await {
        eprintln!("error: {}", msg);
        exit(1);
    } else {
        exit(0);
    }
}
