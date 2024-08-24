use std::io::{self, Write};

const COLUMN_USERNAME_SIZE: usize = 32;
const COLUMN_EMAIL_SIZE: usize = 255;
const TABLE_MAX_PAGES: usize = 100;
const PAGE_SIZE: usize = 4096;
const ROW_SIZE: usize = COLUMN_USERNAME_SIZE + COLUMN_EMAIL_SIZE + std::mem::size_of::<u32>();
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

#[derive(Debug)]
struct Row {
    id: u32,
    username: String,
    email: String,
}

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
    NegativeId,
    StringTooLong,
    SyntaxError,
    UnrecognizedStatement,
}

enum StatementType {
    Insert,
    Select,
}

struct Statement {
    stype: StatementType,
    row_to_insert: Option<Row>,
}

struct Table {
    num_rows: usize,
    pages: Vec<Option<Vec<u8>>>,
}

impl Table {
    fn new() -> Self {
        Table {
            num_rows: 0,
            pages: vec![None; TABLE_MAX_PAGES],
        }
    }

    fn row_slot(&mut self, row_num: usize) -> &mut [u8] {
        let page_num = row_num / ROWS_PER_PAGE;
        if self.pages[page_num].is_none() {
            self.pages[page_num] = Some(vec![0; PAGE_SIZE]);
        }

        let page = self.pages[page_num].as_mut().unwrap();
        let row_offset = row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;
        &mut page[byte_offset..byte_offset + ROW_SIZE]
    }

    fn serialize_row(row: &Row, destination: &mut [u8]) {
        let id_bytes = row.id.to_ne_bytes();
        destination[..4].copy_from_slice(&id_bytes);
        let username_bytes = row.username.as_bytes();
        let email_bytes = row.email.as_bytes();

        destination[4..4 + COLUMN_USERNAME_SIZE].copy_from_slice(username_bytes);
        destination[36..36 + COLUMN_EMAIL_SIZE].copy_from_slice(email_bytes);
    }

    fn deserialize_row(source: &[u8]) -> Row {
        let id = u32::from_ne_bytes([source[0], source[1], source[2], source[3]]);
        let username = String::from_utf8(source[4..36].to_vec()).unwrap();
        let email = String::from_utf8(source[36..].to_vec()).unwrap();

        Row {
            id,
            username,
            email,
        }
    }
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
    input_buffer.buffer = input_buffer.buffer.trim().to_string();
}

fn do_meta_command(input_buffer: &InputBuffer, table: &mut Table) -> MetaCommandResult {
    if input_buffer.buffer == ".exit" {
        std::process::exit(0);
    } else {
        MetaCommandResult::UnrecognizedCommand
    }
}

fn prepare_insert(input_buffer: &InputBuffer, statement: &mut Statement) -> PrepareResult {
    let input = input_buffer.buffer.trim();
    let parts: Vec<&str> = input.split_whitespace().collect();

    if parts.len() < 4 {
        return PrepareResult::SyntaxError;
    }

    let id: i32 = parts[1].parse().unwrap_or(-1);
    let username = parts[2].to_string();
    let email = parts[3].to_string();

    if id < 0 {
        return PrepareResult::NegativeId;
    }
    if username.len() > COLUMN_USERNAME_SIZE {
        return PrepareResult::StringTooLong;
    }
    if email.len() > COLUMN_EMAIL_SIZE {
        return PrepareResult::StringTooLong;
    }

    statement.stype = StatementType::Insert;
    statement.row_to_insert = Some(Row {
        id: id as u32,
        username,
        email,
    });

    PrepareResult::Success
}

fn prepare_statement(input_buffer: &InputBuffer, statement: &mut Statement) -> PrepareResult {
    if input_buffer.buffer.starts_with("insert") {
        return prepare_insert(input_buffer, statement);
    }
    if input_buffer.buffer == "select" {
        statement.stype = StatementType::Select;
        return PrepareResult::Success;
    }

    PrepareResult::UnrecognizedStatement
}

fn execute_insert(statement: &Statement, table: &mut Table) -> Result<(), String> {
    if table.num_rows >= TABLE_MAX_ROWS {
        return Err("Table full.".into());
    }

    if let Some(ref row) = statement.row_to_insert {
        let slot = table.row_slot(table.num_rows);
        Table::serialize_row(row, slot);
        table.num_rows += 1;
    }
    Ok(())
}

fn execute_select(table: &Table) {
    for i in 0..table.num_rows {
        let slot = table.row_slot(i);
        let row = Table::deserialize_row(slot);
        println!("({}, {}, {})", row.id, row.username, row.email);
    }
}

fn execute_statement(statement: &Statement, table: &mut Table) -> Result<(), String> {
    match statement.stype {
        StatementType::Insert => execute_insert(statement, table),
        StatementType::Select => {
            execute_select(table);
            Ok(())
        }
    }
}

fn main() {
    let mut input_buffer = InputBuffer::new();
    let mut table = Table::new();

    loop {
        print_prompt();
        read_input(&mut input_buffer);

        if input_buffer.buffer.starts_with('.') {
            match do_meta_command(&input_buffer, &mut table) {
                MetaCommandResult::Success => continue,
                MetaCommandResult::UnrecognizedCommand => {
                    println!("Unrecognized command '{}'", input_buffer.buffer);
                    continue;
                }
            }
        }

        let mut statement = Statement {
            stype: StatementType::Insert,
            row_to_insert: None,
        };

        match prepare_statement(&input_buffer, &mut statement) {
            PrepareResult::Success => (),
            PrepareResult::NegativeId => {
                println!("ID must be positive.");
                continue;
            }
            PrepareResult::StringTooLong => {
                println!("String is too long.");
                continue;
            }
            PrepareResult::SyntaxError => {
                println!("Syntax error. Could not parse statement.");
                continue;
            }
            PrepareResult::UnrecognizedStatement => {
                println!(
                    "Unrecognized keyword at start of '{}'.",
                    input_buffer.buffer
                );
                continue;
            }
        }

        match execute_statement(&statement, &mut table) {
            Ok(()) => println!("Executed."),
            Err(e) => println!("Error: {}", e),
        }

        input_buffer.buffer.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_script(commands: Vec<&str>) -> Vec<String> {
        let mut table = Table::new();
        let mut results = Vec::new();

        for command in commands {
            let mut input_buffer = InputBuffer {
                buffer: command.to_string(),
            };
            if input_buffer.buffer.starts_with('.') {
                match do_meta_command(&input_buffer, &mut table) {
                    MetaCommandResult::Success => continue,
                    MetaCommandResult::UnrecognizedCommand => {
                        results.push(format!("Unrecognized command '{}'", input_buffer.buffer));
                        continue;
                    }
                }
            }

            let mut statement = Statement {
                stype: StatementType::Insert,
                row_to_insert: None,
            };

            match prepare_statement(&input_buffer, &mut statement) {
                PrepareResult::Success => (),
                PrepareResult::NegativeId => {
                    results.push("ID must be positive.".to_string());
                    continue;
                }
                PrepareResult::StringTooLong => {
                    results.push("String is too long.".to_string());
                    continue;
                }
                PrepareResult::SyntaxError => {
                    results.push("Syntax error. Could not parse statement.".to_string());
                    continue;
                }
                PrepareResult::UnrecognizedStatement => {
                    results.push(format!(
                        "Unrecognized keyword at start of '{}'.",
                        input_buffer.buffer
                    ));
                    continue;
                }
            }

            match execute_statement(&statement, &mut table) {
                Ok(()) => results.push("Executed.".to_string()),
                Err(e) => results.push(format!("Error: {}", e)),
            }
        }
        results
    }

    #[test]
    fn test_insert_and_retrieve_row() {
        let result = run_script(vec![
            "insert 1 user1 person1@example.com",
            "select",
            ".exit",
        ]);
        assert_eq!(
            result,
            vec![
                "Executed.",
                "(1, user1, person1@example.com)".to_string(),
                "Executed.",
            ]
        );
    }

    #[test]
    fn test_table_full() {
        let script: Vec<String> = (1..=1401)
            .map(|i| format!("insert {} user{} person{}@example.com", i, i, i))
            .collect();
        let mut script = script.clone();
        script.push(".exit".to_string());

        let result = run_script(script.iter().map(|s| s.as_str()).collect());
        assert_eq!(result[result.len() - 2], "Error: Table full.");
    }

    #[test]
    fn test_maximum_length_strings() {
        let long_username = "a".repeat(COLUMN_USERNAME_SIZE);
        let long_email = "a".repeat(COLUMN_EMAIL_SIZE);
        let result = run_script(vec![
            &format!("insert 1 {} {}", long_username, long_email),
            "select",
            ".exit",
        ]);

        assert_eq!(
            result,
            vec![
                "Executed.",
                &format!("(1, {}, {})", long_username, long_email),
                "Executed.",
            ]
        );
    }

    #[test]
    fn test_strings_too_long() {
        let long_username = "a".repeat(COLUMN_USERNAME_SIZE + 1);
        let long_email = "a".repeat(COLUMN_EMAIL_SIZE + 1);
        let result = run_script(vec![
            &format!("insert 1 {} {}", long_username, long_email),
            "select",
            ".exit",
        ]);

        assert_eq!(result, vec!["String is too long.", "Executed."]);
    }

    #[test]
    fn test_negative_id() {
        let result = run_script(vec![
            "insert -1 user1 person1@example.com",
            "select",
            ".exit",
        ]);

        assert_eq!(
            result,
            vec!["ID must be positive.".to_string(), "Executed.".to_string(),]
        );
    }
}
