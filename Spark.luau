--[[
    Spark.luau
    ----------
    By @Cheeteau - https://www.roblox.com/users/925308243/profile

    Lightweight two-phase service framework for Roblox.
    Works identically on server and client — each VM gets its own isolated registry
    because Luau caches module results per-VM.

    ── Lifecycle:
      Phase 1 · Init:
        All registered services have their :Init() called in parallel.
        Use Init ONLY for internal setup (tables, stores, creating instances).
        Do NOT call other services' methods here — they may not be initialised yet.

      Phase 2 · Start:
        Runs only after every :Init() has finished.
        Safe to call Spark.Get() and invoke other services freely.
        Wire signals, connect players, start loops here.

    ─ API:
      Spark.Register(service)       Register a service table (must have .Name).
      Spark.LoadFolder(folder)      Auto-register every ModuleScript in a folder.
      Spark.Get(name)               Retrieve a registered service by name.
                                    Safe during :Start() and after Spark.Start().
      Spark.Start()                 Run Init phase then Start phase. Yields until
                                    both are fully complete, then returns.

    ─ Service contract:
      Each service is a plain table with:
        .Name     string     required  — unique identifier used by Spark.Get()
        :Init()   function   optional  — internal setup, no cross-service calls
        :Start()  function   optional  — connections, loops, cross-service calls OK
]]
local Spark = {}
local _order: { table } = {}
local _registry: { [string]: table } = {}

--[[
    Register a service table with Spark.

    The table must have a non-empty .Name string field.
    Optionally implements :Init() and/or :Start().

    @param table service - Service table to register
    @returns table - The same service table (for chaining convenience)
]]
function Spark.Register(service: table): table
    assert(type(service) == "table", "[Spark] Register() expects a table, got " .. type(service))
    assert(type(service.Name) == "string" and service.Name ~= "", "[Spark] Service must have a non-empty .Name string field")
    assert(not _registry[service.Name], "[Spark] A service named '" .. service.Name .. "' is already registered")

    _registry[service.Name] = service
    table.insert(_order, service)
    return service
end

--[[
    Auto-register every ModuleScript that is a direct child of a folder.

    Only registers modules whose require() result is a table with a .Name field.
    Modules that fail to require or don't match the contract are warned and skipped.

    @param Instance folder - Folder instance to scan
]]
function Spark.LoadFolder(folder: Instance)
    assert(typeof(folder) == "Instance", "[Spark] LoadFolder() expects an Instance, got " .. typeof(folder))

    for _, child in folder:GetChildren() do
        if not child:IsA("ModuleScript") then continue end

        local ok, result = pcall(require, child)
        if not ok then
            warn(("[Spark] LoadFolder: failed to require '%s' — %s"):format(child.Name, tostring(result)))
            continue
        end

        if type(result) ~= "table" then
            warn(("[Spark] LoadFolder: '%s' did not return a table, skipping"):format(child.Name))
            continue
        end

        if type(result.Name) ~= "string" or result.Name == "" then
            warn(("[Spark] LoadFolder: '%s' has no .Name field, skipping"):format(child.Name))
            continue
        end

        Spark.Register(result)
    end
end

--[[
    Retrieve a registered service by name.

    Safe to call during :Start() and at any point after Spark.Start() returns.
    Will error if the name has not been registered — this is intentional so typos
    surface immediately rather than causing silent nil-call bugs downstream.

    @param string name - Service name to look up
    @returns table - The registered service table
]]
function Spark.Get(name: string): table
    local service = _registry[name]
    assert(service, "[Spark] No service registered with name '" .. tostring(name) .. "'")
    return service
end

--[[
    Run methodName on every registered service in parallel, then yield until
    all invocations finish.

    Handles both synchronous and asynchronous methods correctly:
    - If all methods are synchronous (no yields), they complete during the spawn
      loop and we never yield at all.
    - If any method yields (e.g. waiting on a DataStore or RemoteEvent), we yield
      the caller and resume once the last one finishes.

    Errors in individual services are caught, warned, and do not block others.

    @param string methodName - "Init" or "Start"
]]
local function _runPhase(methodName: string)
    local total = 0
    for _, svc in _order do
        if type(svc[methodName]) == "function" then
            total += 1
        end
    end
    if total == 0 then return end

    local finished = 0
    local caller: thread? = nil -- set below if we need to yield

    local function onFinished()
        finished += 1
        if finished >= total and caller then
            -- Resume the yielded Spark.Start() coroutine.
            task.spawn(caller :: thread)
            caller = nil
        end
    end

    for _, svc in _order do
        if type(svc[methodName]) == "function" then
            task.spawn(function()
                local ok, err = pcall(svc[methodName], svc)
                if not ok then
                    warn(("[Spark] %s:%s() error — %s"):format(
                        svc.Name, methodName, tostring(err)))
                end
                onFinished()
            end)
        end
    end

    if finished < total then
        caller = coroutine.running()
        coroutine.yield()
    end
end

--[[
    Start all registered services.

    Blocks (yields) until every :Init() has completed, then until every :Start()
    has completed, then returns. Call this once from your server or client
    bootstrap script.
]]
function Spark.Start()
    print(("[Spark] Starting — %d service(s) registered"):format(#_order))

    _runPhase("Init")
    print("[Spark] ✓ Init phase complete")

    _runPhase("Start")
    print("[Spark] ✓ Start phase complete — all services running")
end

return Spark
