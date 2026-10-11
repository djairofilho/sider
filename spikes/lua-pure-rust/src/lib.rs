#![forbid(unsafe_code)]

#[cfg(test)]
mod tests {
    use omnilua::{Lua, LuaString, SandboxConfig, TripReason};

    fn bounded_lua() -> (Lua, omnilua::Sandbox) {
        let mut config = SandboxConfig::strict();
        config.instruction_limit = Some(100_000);
        config.memory_limit_bytes = Some(1024 * 1024);
        config.check_interval = 1;
        Lua::sandboxed(config).expect("sandbox creation")
    }

    #[test]
    fn host_capabilities_and_dynamic_loading_are_absent() {
        let (lua, _) = bounded_lua();
        let absent: bool = lua
            .load(
                "return os == nil and io == nil and package == nil and debug == nil \
                 and require == nil and load == nil and loadfile == nil and dofile == nil",
            )
            .eval()
            .expect("inspect sandbox globals");
        assert!(absent);
    }

    #[test]
    fn binary_strings_survive_the_vm_boundary() {
        let (lua, _) = bounded_lua();
        let value: LuaString = lua
            .load("return string.char(0, 255, 128, 65)")
            .eval()
            .expect("binary string result");
        assert_eq!(value.as_bytes().expect("string bytes"), [0, 255, 128, 65]);
    }

    #[test]
    fn infinite_loop_trips_instruction_budget() {
        let (lua, sandbox) = bounded_lua();
        assert!(lua.load("while true do end").exec().is_err());
        assert_eq!(sandbox.tripped(), Some(TripReason::Instructions));
    }

    #[test]
    fn large_allocation_is_rejected() {
        let (lua, sandbox) = bounded_lua();
        assert!(lua.load("return string.rep('x', 2 * 1024 * 1024)").exec().is_err());
        assert_eq!(sandbox.tripped(), Some(TripReason::Memory));
    }

    #[test]
    fn malformed_source_fails_without_executing() {
        let (lua, sandbox) = bounded_lua();
        assert!(lua.load("local =").exec().is_err());
        assert_eq!(sandbox.tripped(), None);
    }
}
