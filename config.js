module.exports = {
  mysql: {
    type: "mysql",
    host: "10.255.50.81",
    port: "3306",
    username: "root",
    password: "shuguometro@12@cdmetro.A",
    database: "ty_metro_trip",
    extra: {
      connectionLimit: 50,
    }
  },
  clickhouse: {
    host: "10.255.50.45",
    port: "8123",
    username: "default",
    password: "",
    database: "default",
  }
}