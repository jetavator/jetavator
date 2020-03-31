@fixture.tempfolder
@fixture.yaml
Feature: Add new source to existing database

  @setup
  Scenario: Add new source to existing database - Setup

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
  Scenario: Attempt to load data before source is created

    When we call the method .deploy()
    And we load a set of test data to table aircraft_type:
      | code | name           |
      | 744  | Boeing 747-400 |
      | 319  | Airbus A319    |

    Then an exception is raised

  @fixture.jetavator
  Scenario: Add new source

    When we call the method .deploy()
    And the test data is loaded to table source_airport_details
    And we call the method .run(load_type="delta")
    And we add a new source:
      """
      name: aircraft_type
      type: source
      columns:
        "code": {"type": "char(3)", "nullable": False, "pk": True}
        "name": {"type": "varchar(max)", "nullable": False}
      """

    Then the table source_aircraft_type exists
    And the table source_aircraft_type contains 0 rows

  @fixture.jetavator
  Scenario: Add new source and load test data

    When we call the method .deploy()
    And the test data is loaded to table source_airport_details
    And we call the method .run(load_type="delta")
    And we add a new source:
      """
      name: aircraft_type
      type: source
      columns:
        "code": {"type": "char(3)", "nullable": False, "pk": True}
        "name": {"type": "varchar(max)", "nullable": False}
      """
    And we load a set of test data to table aircraft_type:
      | code | name           |
      | 744  | Boeing 747-400 |
      | 319  | Airbus A319    |

    Then the table source_aircraft_type exists
    And the table source_aircraft_type contains 2 rows
