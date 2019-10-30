let importCount = 0, avgTotal = 0, timeTotal = 0;
let time = parseInt(process.argv[2]);
process.on("message", msg => {
  importCount += msg || 0;
  avgTotal += msg || 0;
})
setInterval(() => {
  timeTotal++
  console.log(`当前导入数据量：${importCount}`);
  if (timeTotal % time === 0) {
    console.log(`当前TPS: ${avgTotal / time}`);
    avgTotal = 0;
  }
}, 1000)