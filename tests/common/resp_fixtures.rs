//! Literal 0.1 oracle, independent of the Sider codec and encoder.
//!
//! Each case starts and ends with its keys absent; it can be repeated in a pipeline.
//! Lengths are written by hand to avoid sharing bugs with the product.

pub type Exchange = (&'static [u8], &'static [u8]);

pub struct Case {
    pub name: &'static str,
    pub exchanges: &'static [Exchange],
}

pub const CASES: &[Case] = &[
    Case {
        name: "ping",
        exchanges: &[
            (b"*1\r\n$4\r\nPING\r\n", b"+PONG\r\n"),
            (b"*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n", b"$5\r\nhello\r\n"),
            (b"*2\r\n$4\r\nPING\r\n$0\r\n\r\n", b"$0\r\n\r\n"),
            (
                b"*2\r\n$4\r\nPING\r\n$5\r\n\x00\r\n\xffA\r\n",
                b"$5\r\n\x00\r\n\xffA\r\n",
            ),
        ],
    },
    Case {
        name: "echo",
        exchanges: &[
            (b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n", b"$5\r\nhello\r\n"),
            (b"*2\r\n$4\r\nECHO\r\n$0\r\n\r\n", b"$0\r\n\r\n"),
            (
                b"*2\r\n$4\r\nECHO\r\n$5\r\n\x00\r\n\xffA\r\n",
                b"$5\r\n\x00\r\n\xffA\r\n",
            ),
        ],
    },
    Case {
        name: "strings_and_missing",
        exchanges: &[
            (b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n", b"$-1\r\n"),
            (b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nvalue\r\n", b"+OK\r\n"),
            (b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n", b"$5\r\nvalue\r\n"),
            (b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$0\r\n\r\n", b"+OK\r\n"),
            (b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n", b"$0\r\n\r\n"),
            (b"*2\r\n$3\r\nDEL\r\n$1\r\nk\r\n", b":1\r\n"),
            (b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n", b"$-1\r\n"),
            (b"*2\r\n$3\r\nDEL\r\n$1\r\nk\r\n", b":0\r\n"),
        ],
    },
    Case {
        name: "empty_key_and_binary_value",
        exchanges: &[
            (
                b"*3\r\n$3\r\nSET\r\n$0\r\n\r\n$5\r\n\x00\r\n\xffA\r\n",
                b"+OK\r\n",
            ),
            (b"*2\r\n$3\r\nGET\r\n$0\r\n\r\n", b"$5\r\n\x00\r\n\xffA\r\n"),
            (b"*2\r\n$3\r\nDEL\r\n$0\r\n\r\n", b":1\r\n"),
            (b"*2\r\n$3\r\nGET\r\n$0\r\n\r\n", b"$-1\r\n"),
        ],
    },
    Case {
        name: "binary_key",
        exchanges: &[
            (
                b"*3\r\n$3\r\nSET\r\n$4\r\n\x00\xff\r\n\r\n$1\r\nx\r\n",
                b"+OK\r\n",
            ),
            (b"*2\r\n$3\r\nGET\r\n$4\r\n\x00\xff\r\n\r\n", b"$1\r\nx\r\n"),
            (b"*2\r\n$3\r\nDEL\r\n$4\r\n\x00\xff\r\n\r\n", b":1\r\n"),
            (b"*2\r\n$3\r\nGET\r\n$4\r\n\x00\xff\r\n\r\n", b"$-1\r\n"),
        ],
    },
    Case {
        name: "del_duplicates",
        exchanges: &[
            (b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nx\r\n", b"+OK\r\n"),
            (b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\ny\r\n", b"+OK\r\n"),
            (
                b"*5\r\n$3\r\nDEL\r\n$1\r\na\r\n$1\r\na\r\n$1\r\nz\r\n$1\r\nb\r\n",
                b":2\r\n",
            ),
            (b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n", b"$-1\r\n"),
            (b"*2\r\n$3\r\nGET\r\n$1\r\nb\r\n", b"$-1\r\n"),
            (b"*3\r\n$3\r\nDEL\r\n$1\r\na\r\n$1\r\nb\r\n", b":0\r\n"),
        ],
    },
    Case {
        name: "ascii_command_case_and_distinct_keys",
        exchanges: &[
            (b"*1\r\n$4\r\npInG\r\n", b"+PONG\r\n"),
            (b"*2\r\n$4\r\neChO\r\n$1\r\nx\r\n", b"$1\r\nx\r\n"),
            (b"*3\r\n$3\r\nsEt\r\n$1\r\nK\r\n$1\r\nu\r\n", b"+OK\r\n"),
            (b"*3\r\n$3\r\nset\r\n$1\r\nk\r\n$1\r\nl\r\n", b"+OK\r\n"),
            (b"*2\r\n$3\r\ngEt\r\n$1\r\nK\r\n", b"$1\r\nu\r\n"),
            (b"*2\r\n$3\r\nget\r\n$1\r\nk\r\n", b"$1\r\nl\r\n"),
            (b"*3\r\n$3\r\ndEl\r\n$1\r\nk\r\n$1\r\nK\r\n", b":2\r\n"),
        ],
    },
    Case {
        name: "arity_errors_preserve_connection_and_state",
        exchanges: &[
            (b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4\r\nsafe\r\n", b"+OK\r\n"),
            (
                b"*3\r\n$4\r\npInG\r\n$1\r\na\r\n$1\r\nb\r\n",
                b"-ERR wrong number of arguments for 'ping' command\r\n",
            ),
            (
                b"*1\r\n$4\r\nECHO\r\n",
                b"-ERR wrong number of arguments for 'echo' command\r\n",
            ),
            (
                b"*3\r\n$4\r\nECHO\r\n$1\r\na\r\n$1\r\nb\r\n",
                b"-ERR wrong number of arguments for 'echo' command\r\n",
            ),
            (
                b"*1\r\n$3\r\nGET\r\n",
                b"-ERR wrong number of arguments for 'get' command\r\n",
            ),
            (
                b"*3\r\n$3\r\nGET\r\n$1\r\nk\r\n$1\r\nb\r\n",
                b"-ERR wrong number of arguments for 'get' command\r\n",
            ),
            (
                b"*1\r\n$3\r\nSET\r\n",
                b"-ERR wrong number of arguments for 'set' command\r\n",
            ),
            (
                b"*2\r\n$3\r\nSET\r\n$1\r\nk\r\n",
                b"-ERR wrong number of arguments for 'set' command\r\n",
            ),
            (
                b"*1\r\n$3\r\nDEL\r\n",
                b"-ERR wrong number of arguments for 'del' command\r\n",
            ),
            (b"*1\r\n$4\r\nPING\r\n", b"+PONG\r\n"),
            (b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n", b"$4\r\nsafe\r\n"),
            (b"*2\r\n$3\r\nDEL\r\n$1\r\nk\r\n", b":1\r\n"),
        ],
    },
];
