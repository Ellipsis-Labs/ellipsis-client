pub mod banks_client;
pub mod banks_client_interface;
pub mod banks_server;
pub mod ellipsis_client;
pub mod program_test;
pub mod programs;
pub mod transaction_utils;
pub mod grpc_client;

#[macro_use]
extern crate solana_bpf_loader_program;

pub use ellipsis_client::*;
pub use program_test::*;

/// Converts a `solana-program`-style entrypoint into the runtime's entrypoint style, for
/// use with `ProgramTest::add_program`
#[macro_export]
macro_rules! processor {
    ($process_instruction:expr) => {
        Some(
            |first_instruction_account: usize,
             invoke_context: &mut solana_test_client::program_test::InvokeContext| {
                $crate::program_test::builtin_process_instruction(
                    $process_instruction,
                    first_instruction_account,
                    invoke_context,
                )
            },
        )
    };
}
