package com.packages

import io.gatling.core.Predef._
import io.gatling.http.Predef._
import scala.concurrent.duration._
class DirectDataTransferSimulation extends Simulation {
  val httpConf = http
    .baseUrl("http://localhost:8080")
    .contentTypeHeader("application/json")
    .shareConnections
    .maxConnectionsPerHost(1000)
  val feeder = Iterator.continually {
    val uuid= java.util.UUID.randomUUID().toString
    Map("name" -> s"Name-${uuid.take(8)}")
  }

  val scn = scenario("DirectDataTransfer_CRUD")
    .exitBlockOnFail{
    feed(feeder)
     .exec(
      http("Create")
        .post("/api/entity/stream/create")
        .body(StringBody("""{"name":"#{name}"}""")).asJson
        .check(status.is(201),jsonPath("$.id").saveAs("entityId"))
         )
     .exec(
      http("Update1")
        .put("/api/entity/stream/update/#{entityId}")
        .body(StringBody("""{"name":"#{name}-upd1"}""")).asJson
        .check(status.is(200))
    )
    .exec(
      http("Update2")
        .put("/api/entity/stream/update/#{entityId}")
        .body(StringBody("""{"name":"#{name}-upd2"}""")).asJson
        .check(status.is(200))
    )
    .exec(
      http("Delete")
        .delete("/api/entity/stream/delete/#{entityId}")
        .check(status.is(200))
    )

    .pause(10.millis)}

  setUp(
    scn.inject(
    constantUsersPerSec(10) during (100.minute) randomized)
  ).protocols(httpConf)

}

