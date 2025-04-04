use chrono::Utc;
use postgres::{Client, NoTls, Error, GenericClient};

fn main() -> Result<(), Error> {
    let mut client = Client::connect(
        "host=localhost user=postgres password=123456 dbname=postgres connect_timeout=10 tcp_user_timeout=300",
        NoTls,
    )?;

    let s = client.prepare("insert into people (id, name, age, tm) values ($1, $2, $3, $4 )")?;
    for t in s.params() {
        println!("{:?}", t)
    }
    //client.batch_execute()
    // 类型必须完全匹配
    /*client.execute(
        "insert into people (id, name, age, tm) values ($1, $2, $3, $4)",
        &[&1i64, &"Alice", &10i32, &Utc::now()],
    )?;*/
    client.execute(
        "insert into people (id, name, age, tm) values ($1, $2, $3, $4)",
        &[&1i64, &"Alice", &10i32, &"2025-04-02 22:12:13"],
    )?;

    Ok(())
}