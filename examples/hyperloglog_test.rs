use chrono::Local;
use retl::sketch::hll::hll::Hll;

fn test_hll_cardinality() {
    let ns = vec![10, 100, 1000, 10000, 100000, 1000000, 10000000];
    let ps = [11, 12];
    for &p in &ps {
        println!("p:{}", p);
        for &n in &ns {
            let mut hll = Hll::new(p, 6);
            for i in 0..n {
                hll.add(&i);
                //hll.add(&i.to_string());
                //hll.add_str(&i.to_string());
                //hll.add_u32(i);
            }
            let estimate = hll.cardinality();
            let percent_err = (estimate - n as f64).abs() * 100.0 / n as f64;
            println!("n:{},estimate:{},percentErr:{}", n, estimate, percent_err);
        }
    }
}

fn main() {
    println!("{}", Local::now());
    test_hll_cardinality();
    println!("{}", Local::now());
    /*use hyperloglog_rs::prelude::*;

    let mut hll = HyperLogLog::<Precision12, 5>::default();
    hll.insert(&1);
    hll.insert(&2);
    hll.insert(&3);

    let mut hll2 = HyperLogLog::<Precision12, 5>::default();
    hll2.insert(&3);
    hll2.insert(&4);
    hll2.insert(&5);

    let union = hll | hll2;

    let size = hll.estimate_cardinality();
    let size2 = hll2.estimate_cardinality();
    let union_size = union.estimate_cardinality();
    println!("{} {} {}", size, size2, union_size);*/
}