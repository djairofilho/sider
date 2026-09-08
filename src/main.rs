#![forbid(unsafe_code)]

use std::process::ExitCode;

use sider::ServerConfig;

fn main() -> ExitCode {
    let args: Vec<_> = std::env::args_os().skip(1).collect();

    match args.as_slice() {
        [flag] if flag == "--help" || flag == "-h" => {
            println!(
                "Sider: servidor de banco de dados em memória em desenvolvimento.\n\n\
                 Uso: sider [--help | --version]\n\n\
                 SIDER_ADDR: IP e porta (padrão: 127.0.0.1:6379).\n\
                 Sem argumentos, valida a configuração e informa o estágio atual.\n\
                 O servidor TCP ainda não está implementado."
            );
            ExitCode::SUCCESS
        }
        [flag] if flag == "--version" || flag == "-V" => {
            println!("sider {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        [] => match ServerConfig::from_env() {
            Ok(config) => {
                println!(
                    "Sider {}: configuração válida para {}.\n\
                     Base de desenvolvimento pronta; servidor TCP ainda não implementado.",
                    env!("CARGO_PKG_VERSION"),
                    config.bind_addr
                );
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("erro: {error}");
                ExitCode::FAILURE
            }
        },
        _ => {
            eprintln!("argumentos inválidos; uso: sider [--help | --version]");
            ExitCode::FAILURE
        }
    }
}
