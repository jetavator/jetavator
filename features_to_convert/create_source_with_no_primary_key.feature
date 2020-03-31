@fixture.tempfolder
@fixture.yaml
Feature: Create source with no primary key

  @setup
  Scenario: Create source with no primary key - Setup

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
        "code": {"type": "char(3)", "nullable": False}
        "name": {"type": "varchar(max)", "nullable": False}
      """

    And a set of test data:
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

  @fixture.jetavator
  Scenario: Create source with no primary key and load data

    When we call the method .deploy()
    And the test data is loaded to table source_airport_details

    Then the table source_airport_details contains 10 rows

  @fixture.jetavator
  Scenario: Create source with no primary key and load data - after delta load

    When we call the method .deploy()
    And the test data is loaded to table source_airport_details
    And we call the method .run(load_type="delta")

    Then the table source_airport_details contains 0 rows
    And the table source_history.airport_details contains 10 rows
