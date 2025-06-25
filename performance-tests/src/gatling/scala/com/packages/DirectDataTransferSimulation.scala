package com.packages

import io.gatling.core.Predef._
import io.gatling.http.Predef._
import java.util.UUID

class DirectDataTransferSimulation extends Simulation {
  val httpConf = http
    .baseUrl("http://localhost:8080")
    .contentTypeHeader("application/json")

  val feeder = Iterator.continually {
    val id   = UUID.randomUUID().toString
    val name = "Name-" + id.take(8)
    Map("id" -> id, "name" -> name)
  }

  val scn = scenario("DirectDataTransfer_CRUD")
    .feed(feeder)
    .exec(
      http("Create")
        .post("/api/entity/create")
        .body(StringBody("""{"id":"#{id}","name":"#{name}"}""")).asJson
        .check(status.is(201))
    )
    .exec(
      http("Update1")
        .put("/api/entity/update/#{id}")
        .body(StringBody("""{"id":"#{id}","name":"#{name}-upd1"}""")).asJson
        .check(status.is(201))
    )
    .exec(
      http("Update2")
        .put("/api/entity/update/#{id}")
        .body(StringBody("""{"id":"#{id}","name":"#{name}-upd2"}""")).asJson
        .check(status.is(201))
    )
    .exec(
      http("Delete")
        .delete("/api/entity/delete/#{id}")
        .check(status.is(200))
    )

  setUp(
    scn.inject(atOnceUsers(100))
  ).protocols(httpConf)
}
