use std::error::Error;
use std::time::Duration;
use chrono::Local;

fn test_get() -> Result<(), Box<dyn Error>> {
    // 发送 GET 请求
    let url = "https://jsonplaceholder.typicode.com/posts/1";
    let response = reqwest::blocking::get(url)?;

    // 检查响应状态码
    if response.status().is_success() {
        // 读取响应体
        let body: serde_json::Value = response.json()?;
        println!("响应内容: {}", serde_json::to_string_pretty(&body)?);
    } else {
        println!("请求失败，状态码: {}", response.status());
    }

    Ok(())
}

fn test_post() -> Result<(), Box<dyn Error>> {
    let client = reqwest::blocking::Client::new();
    let url = "https://jsonplaceholder.typicode.com/posts";
    let body = r#"
        {
            "title": "foo",
            "body": "bar",
            "userId": 1
        }
    "#;

    // 发送 POST 请求
    let response = client.post(url)
        .header("Content-Type", "application/json")
        .body(body) // 或者使用 .json(&body), 不需要手动设置 Content-Type
        .send()?;

    // 检查响应状态码
    if response.status().is_success() {
        let response_body = response.text()?;
        println!("响应内容: {}", response_body);
    } else {
        println!("请求失败，状态码: {}", response.status());
    }

    Ok(())
}

fn test_timeout() -> Result<(), Box<dyn Error>> {
    println!("{} start", Local::now());
    let client = reqwest::blocking::Client::builder()
        .pool_idle_timeout(Duration::from_secs(5))
        //.connect_timeout(Some(Duration::from_secs(5)))
        .build()?;
    let url = "http://127.0.0.1:8000/slow3";

    // 发送 POST 请求
    let response = client.put(url)
        .timeout(Duration::from_secs(30))
        .send()?;

    // 检查响应状态码
    if response.status().is_success() {
        let response_body = response.text()?;
        println!("{} 响应内容: {}", Local::now(), response_body);
    } else {
        println!("{} 请求失败，状态码: failed: {}, {}", Local::now(), response.status(), response.text()?);
    }

    Ok(())
}

fn test_request_clone_timeout() -> Result<(), Box<dyn Error>> {
    // 0.12.12版本时候，try_clone()不会复制timeout
    let client = reqwest::blocking::Client::builder()
        .pool_idle_timeout(Duration::from_secs(5))
        //.connect_timeout(Some(Duration::from_secs(5)))
        .build()?;
    let url = "http://127.0.0.1:8000/slow3";

    // 发送 POST 请求
    let request = client.put(url)
        .timeout(Duration::from_secs(30))
        .build()?;
    println!("request: {:?}", request);
    println!("request timeout: {:?}", request.timeout());
    let request2 = request.try_clone().unwrap();
    println!("request: {:?}", request);
    println!("request timeout: {:?}", request.timeout());
    println!("request2: {:?}", request2);
    println!("request2 timeout: {:?}", request2.timeout());

    Ok(())
}

fn main() {
    //test_get().unwrap();
    //test_post().unwrap();
    /*if let Err(e) = test_timeout() {
        println!("{} Error: {:?}", Local::now(), e);
    }*/
    test_request_clone_timeout().unwrap();
}