use csv::{ReaderBuilder, Terminator, WriterBuilder};

fn test_read_csv() {
    let line = "1,aa,3";
    let mut rdr = ReaderBuilder::new()
        .has_headers(false)
        .quote(b'"').quoting(true).double_quote(true).delimiter(b',').escape(Some(b'\\'))
        .from_reader(line.as_bytes());
    for result in rdr.records() {
        // The iterator yields Result<StringRecord, Error>, so we check the
        // error here..
        let record = result.unwrap();
        println!("{:?}", record);
    }

    let line = "1,\"aa\",3";
    let mut rdr = ReaderBuilder::new()
        .has_headers(false)
        .from_reader(line.as_bytes());
    for result in rdr.records() {
        // The iterator yields Result<StringRecord, Error>, so we check the
        // error here..
        let record = result.unwrap();
        println!("{:?}", record);
    }

    let line = "1,\"a,a\",  \n";
    let mut rdr = ReaderBuilder::new()
        .has_headers(false)
        .from_reader(line.as_bytes());
    for result in rdr.records() {
        // The iterator yields Result<StringRecord, Error>, so we check the
        // error here..
        let record = result.unwrap();
        println!("{:?}", record);
    }
}

fn test_write_csv() {
    let mut wtr = csv::Writer::from_writer(std::io::stdout());
    wtr.write_record(&["1", "aa", "3"]).unwrap();
    wtr.write_record(&["1", "a\"a", "3"]).unwrap();
    wtr.write_record(&["1", "a,a", "3"]).unwrap();
    wtr.flush().unwrap();
}

fn test_write_csv2() {
    let mut wtr = WriterBuilder::new()
        .delimiter(b',')
        .has_headers(false)
        .from_writer(vec![]);
    wtr.write_record(&["1", "aa", "3"]).unwrap();
    let rst = wtr.into_inner().unwrap();
    println!("{:?}", String::from_utf8(rst).unwrap());

    let mut wtr = WriterBuilder::new()
        .delimiter(b',')
        .has_headers(false)
        .from_writer(vec![]);
    wtr.write_record(&["1", "a\"a", "3"]).unwrap();
    let rst = wtr.into_inner().unwrap();
    println!("{:?}", String::from_utf8(rst).unwrap());

    let mut wtr = WriterBuilder::new()
        .delimiter(b',')
        .has_headers(false)
        .terminator(Terminator::Any(b'\n')) // 不添加换行符
        .from_writer(vec![]);
    wtr.write_record(&["1", "a,a", "3"]).unwrap();
    let rst = wtr.into_inner().unwrap();
    println!("{:?}", String::from_utf8(rst).unwrap());
}

fn main() {
    test_read_csv();
    test_write_csv();
    test_write_csv2();
}