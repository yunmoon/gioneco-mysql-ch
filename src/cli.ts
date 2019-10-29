#!/usr/bin/env node

import * as yargs from "yargs";
import RunCommand from "./command/ranCommand";
yargs.usage("Usage: $0 <command> [options]")
  .command(new RunCommand())
  .recommendCommands()
  .demandCommand(1)
  // .strict()
  .alias("v", "version")
  .help("h")
  .alias("h", "help")
  .argv;