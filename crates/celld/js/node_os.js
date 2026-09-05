// node:os for celld — a port of Workerd's deterministic Workers shim.
// Copyright (c) 2017-2022 Cloudflare, Inc. (Apache-2.0).
//
// A Worker cannot observe its host operating system. These values describe
// the stable POSIX-shaped Workers environment instead. `/tmp/` names the
// Cloudflare-compatible temporary directory, but this module does not create
// a filesystem; node:fs remains a separate compatibility boundary.
//
// Injected lazily, so an isolate that never imports node:os pays nothing.
(() => {
  "use strict";

  const nodeError = (Type, code, message) => {
    const error = new Type(message);
    error.code = code;
    return error;
  };
  const validateNumber = (value, name) => {
    if (typeof value !== "number" || Number.isNaN(value)) {
      throw nodeError(
        TypeError,
        "ERR_INVALID_ARG_TYPE",
        `The "${name}" argument must be of type number`,
      );
    }
  };
  const validateObject = (value, name) => {
    if (value === null || typeof value !== "object") {
      throw nodeError(
        TypeError,
        "ERR_INVALID_ARG_TYPE",
        `The "${name}" argument must be of type object`,
      );
    }
  };

  const availableParallelism = () => 1;
  const arch = () => "x64";
  const cpus = () => [];
  const endianness = () => "LE";
  const freemem = () => 0;
  const getPriority = (pid) => {
    if (pid !== undefined) validateNumber(pid, "pid");
    return 0;
  };
  const homedir = () => "/tmp/";
  const hostname = () => "localhost";
  const loadavg = () => [0, 0, 0];
  const machine = () => "x86_64";
  const networkInterfaces = () => ({});
  const platform = () => "linux";
  const release = () => "";
  const setPriority = (pid, priority) => {
    validateNumber(pid, "pid");
    validateNumber(priority, "priority");
  };
  const tmpdir = () => "/tmp/";
  const totalmem = () => 0;
  const type = () => "Linux";
  const uptime = () => 0;
  const userInfo = (options = {}) => {
    validateObject(options, "options");
    const encoding = options.encoding ?? null;
    if (
      encoding !== null && encoding !== "buffer" &&
      !globalThis.Buffer.isEncoding(encoding)
    ) {
      throw nodeError(
        TypeError,
        "ERR_INVALID_ARG_VALUE",
        `The argument 'options.encoding' must be a valid encoding. Received ${encoding}`,
      );
    }
    return {};
  };
  const version = () => "";

  const constants = {
    __proto__: null,
    UV_UDP_REUSEADDR: 4,
    dlopen: {
      __proto__: null,
      RTLD_LAZY: 1,
      RTLD_NOW: 2,
      RTLD_GLOBAL: 256,
      RTLD_LOCAL: 0,
      RTLD_DEEPBIND: 8,
    },
    errno: {
      __proto__: null,
      E2BIG: 7,
      EACCES: 13,
      EADDRINUSE: 98,
      EADDRNOTAVAIL: 99,
      EAFNOSUPPORT: 97,
      EAGAIN: 11,
      EALREADY: 114,
      EBADF: 9,
      EBADMSG: 74,
      EBUSY: 16,
      ECANCELED: 125,
      ECHILD: 10,
      ECONNABORTED: 103,
      ECONNREFUSED: 111,
      ECONNRESET: 104,
      EDEADLK: 35,
      EDESTADDRREQ: 89,
      EDOM: 33,
      EDQUOT: 122,
      EEXIST: 17,
      EFAULT: 14,
      EFBIG: 27,
      EHOSTUNREACH: 113,
      EIDRM: 43,
      EILSEQ: 84,
      EINPROGRESS: 115,
      EINTR: 4,
      EINVAL: 22,
      EIO: 5,
      EISCONN: 106,
      EISDIR: 21,
      ELOOP: 40,
      EMFILE: 24,
      EMLINK: 31,
      EMSGSIZE: 90,
      EMULTIHOP: 72,
      ENAMETOOLONG: 36,
      ENETDOWN: 100,
      ENETRESET: 102,
      ENETUNREACH: 101,
      ENFILE: 23,
      ENOBUFS: 105,
      ENODATA: 61,
      ENODEV: 19,
      ENOENT: 2,
      ENOEXEC: 8,
      ENOLCK: 37,
      ENOLINK: 67,
      ENOMEM: 12,
      ENOMSG: 42,
      ENOPROTOOPT: 92,
      ENOSPC: 28,
      ENOSR: 63,
      ENOSTR: 60,
      ENOSYS: 38,
      ENOTCONN: 107,
      ENOTDIR: 20,
      ENOTEMPTY: 39,
      ENOTSOCK: 88,
      ENOTSUP: 95,
      ENOTTY: 25,
      ENXIO: 6,
      EOPNOTSUPP: 95,
      EOVERFLOW: 75,
      EPERM: 1,
      EPIPE: 32,
      EPROTO: 71,
      EPROTONOSUPPORT: 93,
      EPROTOTYPE: 91,
      ERANGE: 34,
      EROFS: 30,
      ESPIPE: 29,
      ESRCH: 3,
      ESTALE: 116,
      ETIME: 62,
      ETIMEDOUT: 110,
      ETXTBSY: 26,
      EWOULDBLOCK: 11,
      EXDEV: 18,
    },
    signals: {
      __proto__: null,
      SIGHUP: 1,
      SIGINT: 2,
      SIGQUIT: 3,
      SIGILL: 4,
      SIGTRAP: 5,
      SIGABRT: 6,
      SIGIOT: 6,
      SIGBUS: 7,
      SIGFPE: 8,
      SIGKILL: 9,
      SIGUSR1: 10,
      SIGSEGV: 11,
      SIGUSR2: 12,
      SIGPIPE: 13,
      SIGALRM: 14,
      SIGTERM: 15,
      SIGCHLD: 17,
      SIGSTKFLT: 16,
      SIGCONT: 18,
      SIGSTOP: 19,
      SIGTSTP: 20,
      SIGTTIN: 21,
      SIGTTOU: 22,
      SIGURG: 23,
      SIGXCPU: 24,
      SIGXFSZ: 25,
      SIGVTALRM: 26,
      SIGPROF: 27,
      SIGWINCH: 28,
      SIGIO: 29,
      SIGPOLL: 29,
      SIGPWR: 30,
      SIGSYS: 31,
    },
    priority: {
      __proto__: null,
      PRIORITY_LOW: 19,
      PRIORITY_BELOW_NORMAL: 10,
      PRIORITY_NORMAL: 0,
      PRIORITY_ABOVE_NORMAL: -7,
      PRIORITY_HIGH: -14,
      PRIORITY_HIGHEST: -20,
    },
  };
  Object.freeze(constants);
  Object.freeze(constants.dlopen);
  Object.freeze(constants.errno);
  Object.freeze(constants.signals);
  Object.freeze(constants.priority);

  globalThis.__osModule = {
    EOL: "\n",
    devNull: "/dev/null",
    constants,
    availableParallelism,
    arch,
    cpus,
    endianness,
    freemem,
    getPriority,
    homedir,
    hostname,
    loadavg,
    machine,
    networkInterfaces,
    platform,
    release,
    setPriority,
    tmpdir,
    totalmem,
    type,
    uptime,
    userInfo,
    version,
  };
})();
