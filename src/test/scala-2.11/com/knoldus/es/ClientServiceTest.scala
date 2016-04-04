package com.knoldus.es

import org.scalatest.FunSuite

class ClientServiceTest extends FunSuite {

  val clientService = new ClientService

  test("Case 1: Add a valid record") {
    val result = clientService.add("Pallavi", "Trainee", 10000, "1")
    assert(Integer.parseInt(result.getId) === 1)
  }

  test("Case 2: Get count of Records") {
    val result = clientService.getCount
    assert(result === 1)
  }

  test("Case 3: Get all Records") {
    clientService.add("Himani", "Trainee", 10000, "2")
    val result = clientService.searchAll
    assert(result.getHits.totalHits === 2)
  }

  test("Case 4: Update a Record") {
    val response = clientService.update("1", "amount", 12000)
    assert(response.getVersion === 1)
  }

  test("Case 5: Search") {
    val result = clientService.search("amount", "12000")
    assert(result.getHits.totalHits === 1)
  }

  test("Case 6: Delete a Record") {
    val result = clientService.delete(2)
    assert(result.getTotalDeleted === 1)
  }

  test("Case 7:Read from a json File") {
    val result = clientService.readFromJson("inputJson.json")
    assert(result != null)
  }

  test("Case 8:Write to a json File") {
    val result = clientService.readFromJson("inputJson.json")
    assert(result != null)
  }


}
