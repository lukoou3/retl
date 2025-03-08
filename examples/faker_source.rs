use retl::connector::faker::{CharsStringFaker, Faker, FakerSource, RangeIntFaker, RangeLongFaker, RegexStringFaker};
use retl::connector::Source;
use retl::execution::PrintCollector;
use retl::types::Schema;

fn main() {
    let fakes:Vec<(usize, Box<dyn Faker>)> = vec![
        (0, Box::new(RangeLongFaker::new(1, i64::MAX, false))),
        (0,Box::new(RangeIntFaker::new(0, 2000, true))),
        (0,Box::new(RangeLongFaker::new(0, 2000, true))),
        (0,Box::new(CharsStringFaker::new(vec!['a', 'b', 'c', 'd', 'e', 'f', 'g'], 4))),
        (0,Box::new(CharsStringFaker::new("123456".chars().collect(), 4))),
        (0,Box::new(RegexStringFaker::new("12[a-z]{2}"))),
        (0,Box::new(RegexStringFaker::new("12[a-z]{2,4}"))),
    ];
    let mut source: Box<dyn Source> = Box::new(FakerSource::new(Schema::new(Vec::new()), fakes, 3, 1000, 1000));
    let mut out = PrintCollector;
    source.open().unwrap();
    source.run(&mut out);
}