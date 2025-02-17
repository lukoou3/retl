use retl::connector::faker::{CharsStringFaker, Faker, FakerSource, RangeIntFaker, RangeLongFaker, RegexStringFaker};
use retl::connector::{PrintCollector, Source};

fn main() {
    let fakes:Vec<Box<dyn Faker>> = vec![
        Box::new(RangeLongFaker::new(1, i64::MAX, false)),
        Box::new(RangeIntFaker::new(0, 2000, true)),
        Box::new(RangeLongFaker::new(0, 2000, true)),
        Box::new(CharsStringFaker::new(vec!['a', 'b', 'c', 'd', 'e', 'f', 'g'], 4)),
        Box::new(CharsStringFaker::new("123456".chars().collect(), 4)),
        Box::new(RegexStringFaker::new("12[a-z]{2}")),
        Box::new(RegexStringFaker::new("12[a-z]{2,4}")),
    ];
    let mut source: Box<dyn Source> = Box::new(FakerSource::new(fakes, 3));
    let out = &PrintCollector;
    source.open().unwrap();
    source.run(out);
}