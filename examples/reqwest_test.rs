use std::error::Error;

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

fn main() {
    //test_get().unwrap();
    test_post().unwrap();
}