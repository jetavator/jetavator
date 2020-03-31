@fixture.tempfolder
@fixture.yaml
Feature: Testing Framework

  @setup
  @fixture.jetavator
  Scenario: Testing Framework - Setup
    Given a definition for a project:
      """
      name: example
      type: project
      version: 0.0.1
      """

    And a definition for a source:
      """
      name: airport_details
      type: source
      columns:
        "code": {"type": "char(3)", "nullable": False, "pk": True}
        "name": {"type": "varchar(max)", "nullable": False}
      """

  @fixture.jetavator
  Scenario: given only the following data is loaded
    Given we call the method .deploy()
    Given only the following data is loaded to table source_airport_details:
      | code | name                                             |
      | AMS  | Amsterdam Airport Schiphol                       |
    Then the table source_airport_details contains 1 row

  @fixture.jetavator
  Scenario: when only the following data is loaded
    Given we call the method .deploy()
    Given the following data is loaded to table source_airport_details:
      | code | name                                             |
      | AMS  | Amsterdam Airport Schiphol                       |
    When only the following data is loaded to table source_airport_details:
      | code | name                                             |
      | ATL  | Hartsfield-Jackson Atlanta International Airport |
      | PEK  | Beijing Capital International Airport            |
      | DXB  | Dubai International Airport                      |
      | HND  | Haneda Airport                                   |
      | LAX  | Los Angeles International Airport                |
      | ORD  | O'Hare International Airport                     |
      | LHR  | London Heathrow Airport                          |
      | HKG  | Hong Kong International Airport                  |
      | PVG  | Shanghai Pudong International Airport            |
      | CDG  | Paris-Charles de Gaulle Airport	                |
    Then the table source_airport_details contains 10 rows

  @fixture.jetavator
  Scenario: given the file is loaded
    Given we call the method .deploy()
    Given the following data is loaded to table source_airport_details:
      | code | name                                             |
      | AMS  | Amsterdam Airport Schiphol                       |
    Given the file airport_details.csv is loaded to table source_airport_details
    Then the table source_airport_details contains 11 rows

  @fixture.jetavator
  Scenario: when the file is loaded
    Given we call the method .deploy()
    Given the following data is loaded to table source_airport_details:
      | code | name                                             |
      | AMS  | Amsterdam Airport Schiphol                       |
    When the file airport_details.csv is loaded to table source_airport_details
    Then the table source_airport_details contains 11 rows

  @fixture.jetavator
  Scenario: given only the file is loaded
    Given we call the method .deploy()
    Given the following data is loaded to table source_airport_details:
      | code | name                                             |
      | AMS  | Amsterdam Airport Schiphol                       |
    Given only the file airport_details.csv is loaded to table source_airport_details
    Then the table source_airport_details contains 10 rows

  @fixture.jetavator
  Scenario: when only the file is loaded
    Given we call the method .deploy()
    Given the following data is loaded to table source_airport_details:
      | code | name                                             |
      | AMS  | Amsterdam Airport Schiphol                       |
    When only the file airport_details.csv is loaded to table source_airport_details
    Then the table source_airport_details contains 10 rows
