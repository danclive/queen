use queen::Queen;
use queen::node::Control;
use nson::msg;

fn main() {

    let queen = Queen::new().unwrap();

    Control::bind(&queen).unwrap();

    queen.emit("aaa", msg!{});


    queen.on("hello", |c| {
        //println!("{:?}", c);

        let count = c.message.get_i32("count").unwrap();

        //println!("{:?}", count);

        c.queen.emit("hello", msg!{
            "hello": "world",
            //"delay": 1000
            "count": count + 1
        });
    });

    queen.emit("hello", msg!{
        "hello": "world",
        "delay": 10,
        "count": 1
    });


    queen.run(4, true);
}
