import { CommandModule, Argv } from "yargs";
import { createConnection, QueryRunner } from "typeorm";
import * as  ClickHouse from "@apla/clickhouse";
import * as times from 'async/times';
import { isNull } from "util";
import _ = require("lodash");
import moment = require("moment");
import cp = require('child_process');
let child;
async function syncData(queryRunner: QueryRunner, chclient, chDatabase, chTable, tableName, timeColumn, start, end, condition, limit) {
  console.log(`正在同步表${tableName}的数据`)
  let page = 0, conditionArray = [], conditionStr;
  if (condition) {
    Object.keys(condition).forEach(key => {
      if (isNull(condition[key]) || condition[key] === "null" || condition[key] === "NULL") {
        conditionArray.push(`\`${key}\` is null`);
      } else {
        conditionArray.push(`\`${key}\` = '${condition[key]}'`);
      }
    })
    conditionStr = conditionArray.join(" and ");
  }
  let sql = `select * from \`${tableName}\` ${((timeColumn && start && end) || conditionStr) ? 'where' : ''}${timeColumn && start && end ? `\`${timeColumn}\` between ${`'${start}'`} and ${`'${end}'`}` : ""}${conditionStr ? ` and ${conditionStr}` : ""} order by \`${timeColumn}\` asc limit ?,?`;
  let rows = await queryRunner.query(sql, [page * limit, limit]);
  while (rows.length > 0) {
    await insertDataToClickhouse(chDatabase, chclient, rows, chTable);
    page++;
    rows = await queryRunner.query(sql, [page * limit, limit]);
  }
  console.log(`表${tableName}的数据同步完成`)
}
async function insertDataToClickhouse(database, chclient, data, chTableName) {
  return new Promise((resolve, reject) => {
    const stream = chclient.query(`INSERT INTO \`${chTableName}\``, { format: "JSONEachRow", queryOptions: { database } }, async function (err, result) {
      if (err) {
        reject(err)
      } else {
        child.send(data.length);
        resolve(result);
      }
    });
    for (const item of data) {
      Object.keys(item).forEach(key => {
        if (item[key] instanceof Date) {
          item[key] = moment(item[key]).format("YYYY-MM-DD HH:mm:ss");
        }
      })
      stream.write(item);
    }
    stream.end();
  })
}
export default class RunCommand implements CommandModule {
  command = "run";
  describe = "导出mysql数据到clickhouse";
  builder(args: Argv) {
    return args.option("config", {
      demand: true,
      type: "string",
      describe: "配置文件"
    }).option("logging", {
      type: "boolean",
      describe: "是否打印日志"
    }).option("prefix", {
      demand: true,
      type: "string",
      describe: "mysql分表表名前缀，单表时则为表名"
    }).option("number", {
      type: "number",
      default: 1,
      describe: "mysql分表数量，默认不分表"
    }).option("thread", {
      type: "number",
      default: 1,
      describe: "同时执行的线程数，默认为1"
    }).option("timeColumn", {
      type: "string",
      demand: true,
      describe: "时间筛选字段"
    }).option("start", {
      type: "string",
      describe: "时间范围，开始时间"
    }).option("end", {
      type: "string",
      describe: "时间范围，结束时间"
    }).option("condition", {
      type: "string",
      describe: "同步表筛选扩展条件，请使用如下格式：key1:value1,key2:value2"
    }).option("chTable", {
      type: "string",
      demand: true,
      describe: "同步到clickhouse的表名"
    }).option("limit", {
      type: "number",
      default: 1000,
      describe: "分页查询数量"
    }).option("time", {
      type: "number",
      default: 5,
      describe: "计时输出tps时间，默认5秒"
    })
  }
  async handler(args) {
    const config = require(`${process.cwd()}/${args.config}`);
    child = cp.fork(`${__dirname}/../child`, [args.time]);
    if (!config.mysql || !config.clickhouse) {
      throw new Error("config 配置错误");
    }
    if (args.thread > args.number) {
      throw new Error("--thread 不允许大于 --number");
    }
    let condition = {}, conditionArray;
    if (args.condition) {
      conditionArray = args.condition.split(",");
      for (const item of conditionArray) {
        const data = item.split(":");
        if (data.length !== 2) {
          throw new Error("condition 格式错误");
        }
        condition[data[0]] = data[1];
      }
    }
    const chclient = new ClickHouse(config.clickhouse);
    const connection = await createConnection({
      ...config.mysql,
      logging: args.logging
    });
    let tableName = args.prefix
    if (args.number > 1) {
      let ns = 0, tables = [];
      while (ns < args.number) {
        tableName = `${args.prefix}${ns}`;
        tables.push(tableName);
        ns++
      }
      const groupTableNames = _.chunk(tables, Math.ceil(args.number / args.thread));
      // setInterval(() => {
      //   timeTotal++;
      //   console.log(`当前导入数据量：${importCount}`);
      //   if (timeTotal % args.time === 0) {
      //     console.log(`当前TPS: ${fiveTotal / args.time}`);
      //     fiveTotal = 0;
      //   }
      // }, 1000)
      await times(groupTableNames.length, async (time) => {
        const queryRunner = connection.createQueryRunner();
        const tables = groupTableNames[time];
        for (let index = 0; index < tables.length; index++) {
          const tableName = tables[index];
          await syncData(queryRunner, chclient, config.clickhouse.database, args.chTable, tableName, args.timeColumn, args.start, args.end, condition, args.limit);
        }
      });
    } else {
      const queryRunner = connection.createQueryRunner();
      await syncData(queryRunner, chclient, config.clickhouse.database, args.chTable, tableName, args.timeColumn, args.start, args.end, condition, args.limit);
    }
    console.log("执行完毕！");
    process.exit();
  }
}