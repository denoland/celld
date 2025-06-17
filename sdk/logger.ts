import * as log from "jsr:@std/log@^0.224.14";

export function setup(
  tenant: string,
  cellId: string,
  logLevel: log.LevelName = "DEBUG",
) {
  log.setup({
    handlers: {
      console: new log.ConsoleHandler(logLevel, {
        formatter: (record) =>
          `${record.datetime.toISOString()} ${record.levelName} ${record.loggerName} ${tenant}/${cellId}: ${record.msg}`,
      }),
    },

    loggers: {
      "cells-sdk": {
        level: logLevel,
        handlers: ["console"],
      },
    },
  });
}

export function logger() {
  return log.getLogger("cells-sdk");
}
