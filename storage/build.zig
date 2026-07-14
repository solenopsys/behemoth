const std = @import("std");
const Build = std.Build;
const OptimizeMode = std.builtin.OptimizeMode;
const build_utils = @import("build_utils.zig");

const sqlite_dir = "../../../../../../solenopsys/detonation/wrapers/sqlite/vendor/sqlite";
const sqlite_src = sqlite_dir ++ "/sqlite3.c";
const valkey_vendor_dir = "../../wrapers/valkey/vendor/valkey";
const valkey_wrapper_dir = "../../wrapers/valkey";

fn supportsTransport(target: Build.ResolvedTarget) bool {
    return target.result.os.tag == .linux;
}

const transport_vendor_src = "../transport/vendor/capnproto/c++/src";

const TargetParts = struct {
    arch: []const u8,
    libc: []const u8,
};

const kj_sources = [_][]const u8{
    transport_vendor_src ++ "/kj/array.c++",
    transport_vendor_src ++ "/kj/arena.c++",
    transport_vendor_src ++ "/kj/common.c++",
    transport_vendor_src ++ "/kj/debug.c++",
    transport_vendor_src ++ "/kj/encoding.c++",
    transport_vendor_src ++ "/kj/exception.c++",
    transport_vendor_src ++ "/kj/hash.c++",
    transport_vendor_src ++ "/kj/io.c++",
    transport_vendor_src ++ "/kj/list.c++",
    transport_vendor_src ++ "/kj/memory.c++",
    transport_vendor_src ++ "/kj/mutex.c++",
    transport_vendor_src ++ "/kj/refcount.c++",
    transport_vendor_src ++ "/kj/source-location.c++",
    transport_vendor_src ++ "/kj/string.c++",
    transport_vendor_src ++ "/kj/string-tree.c++",
    transport_vendor_src ++ "/kj/table.c++",
    transport_vendor_src ++ "/kj/thread.c++",
    transport_vendor_src ++ "/kj/time.c++",
    transport_vendor_src ++ "/kj/units.c++",
};

const capnp_sources = [_][]const u8{
    transport_vendor_src ++ "/capnp/any.c++",
    transport_vendor_src ++ "/capnp/arena.c++",
    transport_vendor_src ++ "/capnp/blob.c++",
    transport_vendor_src ++ "/capnp/c++.capnp.c++",
    transport_vendor_src ++ "/capnp/persistent.capnp.c++",
    transport_vendor_src ++ "/capnp/rpc.capnp.c++",
    transport_vendor_src ++ "/capnp/rpc-twoparty.capnp.c++",
    transport_vendor_src ++ "/capnp/dynamic.c++",
    transport_vendor_src ++ "/capnp/layout.c++",
    transport_vendor_src ++ "/capnp/list.c++",
    transport_vendor_src ++ "/capnp/message.c++",
    transport_vendor_src ++ "/capnp/schema.c++",
    transport_vendor_src ++ "/capnp/schema.capnp.c++",
    transport_vendor_src ++ "/capnp/schema-loader.c++",
    transport_vendor_src ++ "/capnp/stream.capnp.c++",
    transport_vendor_src ++ "/capnp/serialize.c++",
    transport_vendor_src ++ "/capnp/serialize-packed.c++",
    transport_vendor_src ++ "/capnp/stringify.c++",
};

fn getValkeyTargetParts(target: Build.ResolvedTarget) TargetParts {
    if (target.result.os.tag != .linux) {
        std.debug.panic("valkey wrapper supports linux targets only, got {s}", .{
            @tagName(target.result.os.tag),
        });
    }

    const arch = switch (target.result.cpu.arch) {
        .x86_64 => "x86_64",
        .aarch64 => "aarch64",
        else => std.debug.panic("unsupported cpu arch for valkey: {s}", .{
            @tagName(target.result.cpu.arch),
        }),
    };

    const libc = switch (target.result.abi) {
        .gnu, .gnueabi, .gnueabihf => "gnu",
        .musl, .musleabi, .musleabihf => "musl",
        else => std.debug.panic("unsupported abi for valkey: {s}", .{
            @tagName(target.result.abi),
        }),
    };

    return .{ .arch = arch, .libc = libc };
}

fn getValkeyOptimize(optimize: OptimizeMode) []const u8 {
    return switch (optimize) {
        .Debug => "-O0",
        .ReleaseSafe => "-O2",
        .ReleaseFast => "-O3",
        .ReleaseSmall => "-Os",
    };
}

fn getValkeyTargetTriple(b: *Build, target: Build.ResolvedTarget) []const u8 {
    const target_parts = getValkeyTargetParts(target);
    return b.fmt("{s}-linux-{s}", .{ target_parts.arch, target_parts.libc });
}

fn addValkeyWrapperBuild(
    b: *Build,
    target: Build.ResolvedTarget,
    optimize: OptimizeMode,
    install_dir: []const u8,
) *Build.Step.Run {
    const target_str = build_utils.getTargetString(target);
    const target_triple = getValkeyTargetTriple(b, target);
    const build_cmd = b.addSystemCommand(&[_][]const u8{
        b.graph.zig_exe,
        "build",
        b.fmt("-Dtarget={s}", .{target_triple}),
        b.fmt("-Doptimize={s}", .{@tagName(optimize)}),
        "--prefix",
        install_dir,
    });
    build_cmd.setCwd(b.path(valkey_wrapper_dir));
    build_cmd.setName(b.fmt("build valkey wrapper ({s})", .{target_str}));
    return build_cmd;
}

fn linkValkeyWrapper(
    b: *Build,
    compile: *Build.Step.Compile,
    target: Build.ResolvedTarget,
    optimize: OptimizeMode,
    install_name: []const u8,
) void {
    const target_str = build_utils.getTargetString(target);
    const install_dir = b.fmt("../../behemoth/storage/.zig-cache/valkey-wrapper/{s}", .{target_str});
    const lib_dir = b.fmt(".zig-cache/valkey-wrapper/{s}/lib", .{target_str});
    const build_cmd = addValkeyWrapperBuild(b, target, optimize, install_dir);

    compile.step.dependOn(&build_cmd.step);
    compile.root_module.addIncludePath(b.path("../../wrapers/valkey/include"));
    compile.root_module.addLibraryPath(.{ .cwd_relative = lib_dir });
    compile.root_module.addRPathSpecial("$ORIGIN/../lib");
    compile.root_module.addRPathSpecial("$ORIGIN/lib");
    compile.root_module.linkSystemLibrary("valkey", .{});

    const install_lib = b.addInstallFileWithDir(
        .{ .cwd_relative = b.fmt("{s}/libvalkey.so", .{lib_dir}) },
        .lib,
        install_name,
    );
    install_lib.step.dependOn(&build_cmd.step);
    b.getInstallStep().dependOn(&install_lib.step);
}

fn buildMdbx(b: *Build, target: Build.ResolvedTarget, optimize: OptimizeMode) *Build.Step.Compile {
    const target_str = build_utils.getTargetString(target);
    const mdbx_name = build_utils.getLibName(std.heap.page_allocator, "mdbx", target_str);

    const mdbx = b.addLibrary(.{
        .name = mdbx_name,
        .linkage = .static,
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
        }),
    });

    const cpu_arch = target.result.cpu.arch;
    const abi = target.result.abi;
    const base_flags = [_][]const u8{
        "-DMDBX_BUILD_SHARED_LIBRARY=0",
        "-DMDBX_WITHOUT_MSVC_CRT=0",
        "-DMDBX_BUILD_TOOLS=0",
        "-DMDBX_BUILD_FLAGS=\"zig\"",
        "-DMDBX_BUILD_COMPILER=\"zig-cc\"",
        "-DMDBX_BUILD_TARGET=\"cross\"",
        "-std=c11",
        "-Wno-error",
        "-Wno-expansion-to-defined",
        "-Wno-date-time",
        "-fno-sanitize=undefined",
        "-fPIC",
        "-O2",
        "-ffunction-sections",
        "-fdata-sections",
        "-fvisibility=hidden",
    };
    const x86_gnu_flags = base_flags ++ [_][]const u8{ "-DMDBX_GCC_FASTMATH_i686_SIMD_WORKAROUND=1", "-D_SYS_CACHECTL_H=1", "-march=x86-64" };
    const x86_flags = base_flags ++ [_][]const u8{ "-DMDBX_GCC_FASTMATH_i686_SIMD_WORKAROUND=1", "-march=x86-64" };
    const glibc_cross_flags = base_flags ++ [_][]const u8{"-D_SYS_CACHECTL_H=1"};
    const flags: []const []const u8 = if (cpu_arch == .x86_64 and (abi == .gnu or abi == .gnueabi or abi == .gnueabihf))
        &x86_gnu_flags
    else if (cpu_arch == .x86_64)
        &x86_flags
    else if (abi == .gnu or abi == .gnueabi or abi == .gnueabihf)
        &glibc_cross_flags
    else
        &base_flags;

    mdbx.root_module.addCSourceFile(.{
        .file = b.path("../../wrapers/lmdbx/vendor/libmdbx/src/alloy.c"),
        .flags = flags,
    });
    mdbx.root_module.addCSourceFile(.{
        .file = b.path("../../wrapers/lmdbx/version.c"),
        .flags = flags,
    });
    mdbx.root_module.addCSourceFile(.{
        .file = b.path("../../wrapers/lmdbx/cpu_stub.c"),
        .flags = &[_][]const u8{"-fPIC"},
    });
    mdbx.root_module.addIncludePath(b.path("../../wrapers/lmdbx/vendor/libmdbx"));
    mdbx.root_module.addIncludePath(b.path("../../wrapers/lmdbx/vendor/libmdbx/src"));
    mdbx.root_module.linkSystemLibrary("c", .{});

    return mdbx;
}

fn addSqliteVecObj(b: *Build, target: Build.ResolvedTarget, optimize: OptimizeMode) *Build.Step.Compile {
    const vec = b.addObject(.{
        .name = "sqlite-vec",
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
        }),
    });

    const cpu_arch = target.result.cpu.arch;
    const abi = target.result.abi;
    const is_musl = (abi == .musl or abi == .musleabi or abi == .musleabihf);

    const base_flags = [_][]const u8{ "-O3", "-fPIC", "-DSQLITE_VEC_OMIT_FS" };
    const musl_compat = [_][]const u8{ "-Du_int8_t=uint8_t", "-Du_int16_t=uint16_t", "-Du_int64_t=uint64_t" };
    const neon = [_][]const u8{"-DSQLITE_VEC_ENABLE_NEON"};

    const flags: []const []const u8 = if (cpu_arch == .x86_64 and is_musl)
        &(base_flags ++ musl_compat)
    else if (cpu_arch == .x86_64)
        &base_flags
    else if (cpu_arch == .aarch64 and is_musl)
        &(base_flags ++ musl_compat ++ neon)
    else if (cpu_arch == .aarch64)
        &(base_flags ++ neon)
    else if (is_musl)
        &(base_flags ++ musl_compat)
    else
        &base_flags;

    vec.root_module.addCSourceFile(.{
        .file = b.path("../../wrapers/sqlite-vec/vendor/sqlite-vec/sqlite-vec.c"),
        .flags = flags,
    });
    vec.root_module.addIncludePath(b.path("../../wrapers/sqlite-vec/vendor/sqlite-vec"));
    vec.root_module.addIncludePath(b.path("../../wrapers/sqlite-vec/vendor/sqlite-vec/vendor"));
    vec.root_module.linkSystemLibrary("c", .{});

    return vec;
}

fn buildSqlite3(b: *Build, target: Build.ResolvedTarget, optimize: OptimizeMode) *Build.Step.Compile {
    const target_str = build_utils.getTargetString(target);
    const sqlite_name = build_utils.getLibName(std.heap.page_allocator, "sqlite3", target_str);

    const sqlite = b.addLibrary(.{
        .name = sqlite_name,
        .linkage = .static,
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
        }),
    });

    sqlite.root_module.addCSourceFile(.{
        .file = b.path(sqlite_src),
        .flags = &[_][]const u8{
            "-std=c99",
            "-O2",
            "-fPIC",
        },
    });
    sqlite.root_module.addIncludePath(b.path(sqlite_dir));
    sqlite.root_module.linkSystemLibrary("c", .{});

    return sqlite;
}

fn addStorageExecutable(
    b: *Build,
    target: Build.ResolvedTarget,
    optimize: OptimizeMode,
    name: []const u8,
    with_transport: bool,
    valkey_install_name: []const u8,
) *Build.Step.Compile {
    const mdbx = buildMdbx(b, target, optimize);
    const sqlite_vec = addSqliteVecObj(b, target, optimize);
    const sqlite3 = buildSqlite3(b, target, optimize);

    const exe = b.addExecutable(.{
        .name = name,
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const exe_options = b.addOptions();
    exe_options.addOption(bool, "with_transport", with_transport);
    exe.root_module.addOptions("build_options", exe_options);

    exe.root_module.linkLibrary(mdbx);
    exe.root_module.linkLibrary(sqlite3);
    exe.root_module.addObject(sqlite_vec);

    // sqlite-vec header for VectorEngine to call sqlite3_vec_init
    exe.root_module.addIncludePath(b.path("../../wrapers/sqlite-vec/vendor/sqlite-vec"));

    exe.root_module.addCSourceFile(.{
        .file = b.path("../../wrapers/column/src/sqlite3/c/result-transient.c"),
        .flags = &[_][]const u8{"-std=c99"},
    });
    exe.root_module.addIncludePath(b.path("../../wrapers/column/src/sqlite3/c"));
    exe.root_module.addIncludePath(b.path(sqlite_dir));

    const stanchion_mod = b.createModule(.{
        .root_source_file = b.path("../../wrapers/column/src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    stanchion_mod.addIncludePath(b.path("../../wrapers/column/src/sqlite3/c"));
    stanchion_mod.addIncludePath(b.path(sqlite_dir));

    const stanchion_options = b.addOptions();
    stanchion_options.addOption(bool, "loadable_extension", false);
    stanchion_mod.addOptions("build_options", stanchion_options);

    exe.root_module.addImport("stanchion", stanchion_mod);

    exe.root_module.addImport("lmdbx", b.createModule(.{
        .root_source_file = b.path("../../wrapers/lmdbx/src/lmdbx.zig"),
        .target = target,
        .optimize = optimize,
    }));

    exe.root_module.addImport("lmdbx_pure", b.createModule(.{
        .root_source_file = b.path("../../wrapers/lmdbx/src/lmdbx_pure.zig"),
        .target = target,
        .optimize = optimize,
    }));

    exe.root_module.addImport("sqlite3", b.createModule(.{
        .root_source_file = b.path("../../wrapers/column/src/sqlite3.zig"),
        .target = target,
        .optimize = optimize,
    }));

    if (with_transport) {
        const zimq = b.dependency("zimq", .{ .target = target, .optimize = optimize });
        const transport_zmq = b.createModule(.{
            .root_source_file = b.path("../transport/src/socket.zig"),
            .target = target,
            .optimize = optimize,
        });
        transport_zmq.addImport("zimq", zimq.module("zimq"));
        exe.root_module.addImport("transport_zmq", transport_zmq);
        exe.root_module.linkLibrary(zimq.artifact("zimq"));
        exe.root_module.addRPathSpecial("$ORIGIN");

        const cpp_flags = &[_][]const u8{
            "-std=c++17",
            "-fPIC",
            "-fvisibility=hidden",
            "-O2",
            "-Wno-unused-parameter",
        };

        for (kj_sources) |src| {
            exe.root_module.addCSourceFile(.{ .file = b.path(src), .flags = cpp_flags });
        }
        for (capnp_sources) |src| {
            exe.root_module.addCSourceFile(.{ .file = b.path(src), .flags = cpp_flags });
        }

        exe.root_module.addCSourceFile(.{
            .file = b.path("../transport/src/generated/wire.capnp.cpp"),
            .flags = cpp_flags,
        });
        exe.root_module.addCSourceFile(.{
            .file = b.path("../transport/src/capnp_wrap.cpp"),
            .flags = cpp_flags,
        });
        exe.root_module.addIncludePath(b.path("../transport/include"));
        exe.root_module.addIncludePath(b.path("../transport/src/generated"));
        exe.root_module.addIncludePath(b.path(transport_vendor_src));
        exe.root_module.linkSystemLibrary("c++", .{});
    }

    exe.root_module.linkSystemLibrary("c", .{});
    linkValkeyWrapper(b, exe, target, optimize, valkey_install_name);

    return exe;
}

pub fn build(b: *Build) void {
    const target = b.standardTargetOptions(.{
        .default_target = .{
            .cpu_arch = .x86_64,
            .os_tag = .linux,
            .abi = .musl,
        },
    });
    const optimize = b.standardOptimizeOption(.{});
    const build_all = b.option(bool, "all", "Build for all supported targets") orelse false;
    const transport_override = b.option(bool, "transport", "Enable Cap'n Proto transport/server support");

    if (build_all) {
        for (build_utils.supported_targets) |query| {
            const resolved_target = b.resolveTargetQuery(query);
            const with_transport = transport_override orelse supportsTransport(resolved_target);
            const target_str = build_utils.getTargetString(resolved_target);
            const exe_name = build_utils.getExeName(std.heap.page_allocator, "storage", target_str);
            const valkey_name = build_utils.getLibName(std.heap.page_allocator, "valkey", target_str);
            const valkey_install_name = b.fmt("lib{s}.so", .{valkey_name});
            const exe = addStorageExecutable(b, resolved_target, optimize, exe_name, with_transport, valkey_install_name);
            b.installArtifact(exe);
        }
        return;
    }

    const with_transport = transport_override orelse supportsTransport(target);
    const exe = addStorageExecutable(b, target, optimize, "storage", with_transport, "libvalkey.so");
    b.installArtifact(exe);
    if (with_transport) {
        const zimq = b.dependency("zimq", .{ .target = target, .optimize = optimize });
        b.installArtifact(zimq.artifact("zimq"));
    }

    const mdbx = buildMdbx(b, target, optimize);
    const sqlite_vec = addSqliteVecObj(b, target, optimize);
    const sqlite3 = buildSqlite3(b, target, optimize);

    const tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const tests_options = b.addOptions();
    tests_options.addOption(bool, "with_transport", false);
    tests.root_module.addOptions("build_options", tests_options);

    tests.root_module.linkLibrary(mdbx);
    tests.root_module.linkLibrary(sqlite3);
    tests.root_module.addObject(sqlite_vec);

    // sqlite-vec header for VectorEngine
    tests.root_module.addIncludePath(b.path("../../wrapers/sqlite-vec/vendor/sqlite-vec"));

    tests.root_module.addCSourceFile(.{
        .file = b.path("../../wrapers/column/src/sqlite3/c/result-transient.c"),
        .flags = &[_][]const u8{"-std=c99"},
    });
    tests.root_module.addIncludePath(b.path("../../wrapers/column/src/sqlite3/c"));
    tests.root_module.addIncludePath(b.path(sqlite_dir));
    tests.root_module.addImport("lmdbx", b.createModule(.{
        .root_source_file = b.path("../../wrapers/lmdbx/src/lmdbx.zig"),
        .target = target,
        .optimize = optimize,
    }));
    tests.root_module.addImport("lmdbx_pure", b.createModule(.{
        .root_source_file = b.path("../../wrapers/lmdbx/src/lmdbx_pure.zig"),
        .target = target,
        .optimize = optimize,
    }));
    tests.root_module.addImport("sqlite3", b.createModule(.{
        .root_source_file = b.path("../../wrapers/column/src/sqlite3.zig"),
        .target = target,
        .optimize = optimize,
    }));
    tests.root_module.linkSystemLibrary("c", .{});
    tests.root_module.addIncludePath(b.path("../../wrapers/valkey/include"));

    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_tests.step);

    const integration_target = b.resolveTargetQuery(build_utils.supported_targets[0]);
    const integration_with_transport = transport_override orelse supportsTransport(integration_target);
    const integration_exe = addStorageExecutable(b, integration_target, optimize, "storage-test-host", integration_with_transport, "libvalkey-test-host.so");
    const install_integration_exe = b.addInstallArtifact(integration_exe, .{});

    const run_integration_tests = b.addSystemCommand(&[_][]const u8{ "bun", "test", "tests" });
    run_integration_tests.setCwd(b.path(".."));
    run_integration_tests.step.dependOn(&install_integration_exe.step);
    const integration_test_step = b.step("integration-test", "Run Bun integration/stress tests");
    integration_test_step.dependOn(&run_integration_tests.step);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_cmd.addArgs(args);
    const run_step = b.step("run", "Run storage engine");
    run_step.dependOn(&run_cmd.step);
}
