use std::io::{self, Write};

struct InputBuffer {
    buffer: String,
}

impl InputBuffer {
    fn new() -> Self {
        InputBuffer {
            buffer: String::new(),
        }
    }

    fn close(self) {
        println!("Exiting...");
    }
}

enum MetaCommandResult {
    Success,
    UnrecognizedCommand,
}

enum PrepareResult {
    Success,
    UnrecognizedStatement,
}

enum StatementType {
    Insert,
    Select,
}

struct Statement {
    stype: StatementType,
}

fn print_prompt() {
    print!("db > ");
    io::stdout().flush().unwrap();
}

fn read_input(input_buffer: &mut InputBuffer) {
    io::stdin()
        .read_line(&mut input_buffer.buffer)
        .expect("Failed to read line");

    // Remove the trailing newline
    if let Some('\n') = input_buffer.buffer.chars().last() {
        input_buffer.buffer.pop();
    }
    if let Some('\r') = input_buffer.buffer.chars().last() {
        input_buffer.buffer.pop();
    }
}

fn do_meta_command(input_buffer: &InputBuffer) -> MetaCommandResult {
    if input_buffer.buffer == ".exit" {
        std::process::exit(0);
    } else {
        MetaCommandResult::UnrecognizedCommand
    }
}

fn prepare_statement(input_buffer: &InputBuffer, statement: &mut Statement) -> PrepareResult {
    if input_buffer.buffer.starts_with("insert") {
        statement.stype = StatementType::Insert;
        PrepareResult::Success
    } else if input_buffer.buffer == "select" {
        statement.stype = StatementType::Select;
        PrepareResult::Success
    } else {
        PrepareResult::UnrecognizedStatement
    }
}

fn execute_statement(statement: &Statement) {
    match statement.stype {
        StatementType::Insert => println!("This is where we would do an insert."),
        StatementType::Select => println!("This is where we would do a select."),
    }
}

fn main() {
    let mut input_buffer = InputBuffer::new();

    loop {
        print_prompt();
        read_input(&mut input_buffer);

        if input_buffer.buffer.starts_with('.') {
            match do_meta_command(&input_buffer) {
                MetaCommandResult::Success => {
                    input_buffer.buffer.clear(); // Clear the buffer after processing the command
                    continue;
                }
                MetaCommandResult::UnrecognizedCommand => {
                    println!("Unrecognized command '{}'", input_buffer.buffer);
                    input_buffer.buffer.clear(); // Clear the buffer after processing the command
                    continue;
                }
            }
        }

        let mut statement = Statement {
            stype: StatementType::Insert,
        };
        match prepare_statement(&input_buffer, &mut statement) {
            PrepareResult::Success => (),
            PrepareResult::UnrecognizedStatement => {
                println!(
                    "Unrecognized keyword at start of '{}'.",
                    input_buffer.buffer
                );
                input_buffer.buffer.clear(); // Clear the buffer after processing the command
                continue;
            }
        }

        execute_statement(&statement);
        println!("Executed.");

        input_buffer.buffer.clear(); // Clear the buffer at the end of the loop
    }
}
