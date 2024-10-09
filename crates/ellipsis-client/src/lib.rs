pub mod ellipsis_client;

pub use ellipsis_client::*;
pub use ellipsis_transaction_utils as transaction_utils;

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
