@fixture.tempfolder
@fixture.yaml
Feature: Update Hub Satellite

  @setup
  Scenario: Update Hub Satellite - Setup

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

    And a definition for a hub:
      """
      name: airport
      type: hub
      key_length: 3
      """

    And a definition for a satellite:
      """
      name: airport_details
      type: satellite
      parent:
        name: airport
        type: hub
      columns:
        "name": {"type": "varchar(max)", "nullable": False}
      pipeline:
        type: source
      """

  @fixture.jetavator
  Scenario: Update Existing Hub Satellite Entry

     When we call the method .deploy()
      And the following data is loaded to table source_airport_details:
        | code | name                                             |
        | ATL  | Hartsfield-Jackson Atlanta International Airport |
        | PEK  | Beijing Capital International Airport            |
        | DXB  | Dubai International Airport                      |
        | HND  | Haneda Airport                                   |
        | LAX  | Los Angeles International Airport                |
        | ORD  | Orchard Field                                    |
        | LHR  | London Heathrow Airport                          |
        | HKG  | Hong Kong International Airport                  |
        | PVG  | Shanghai Pudong International Airport            |
        | CDG  | Paris-Charles de Gaulle Airport	                |
      And we call the method .run(load_type="delta")

      And the following updates are loaded to table source_airport_details:
        | code | name                                             |
        | ORD  | O'Hare International Airport                     |
      And we call the method .run(load_type="delta")

     Then the table vault.hub_airport exists
      And the table vault.sat_airport_details exists
      And the table vault.hub_airport contains 10 rows
      And the table vault.sat_airport_details contains 11 rows
      And the loading datetime in the table sat_airport_details contains 2 distinct datetimes with code ORD
      And the view vault_now.airport_details contains 10 rows:
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
      And the view vault_updates_airport_details contains 1 rows:
        | code | name                                             | jetavator_deleted_ind |
        | ORD  | O'Hare International Airport                     | 0                     |

  @fixture.jetavator
  Scenario: Add New Hub Satellite Entry

      When we call the method .deploy()
      And the following data is loaded to table source_airport_details:
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
      And we call the method .run(load_type="delta")

      And the following updates are loaded to table source_airport_details:
        | code | name                                             |
        | AMS  | Amsterdam Airport Schiphol                       |
      And we call the method .run(load_type="delta")

      Then the table vault.hub_airport exists
      And the table vault.sat_airport_details exists
      And the table vault.hub_airport contains 11 rows
      And the table vault.sat_airport_details contains 11 rows
      And the view vault_now.airport_details contains 11 rows:
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
        | AMS  | Amsterdam Airport Schiphol                       |
      And the view vault_updates_airport_details contains 1 rows:
        | code | name                                             | jetavator_deleted_ind |
        | AMS  | Amsterdam Airport Schiphol                       | 0                     |

  @fixture.jetavator
  Scenario: Delete Hub Satellite Entry

      When we call the method .deploy()
      And the following data is loaded to table source_airport_details:
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
      And we call the method .run(load_type="delta")

      And the following updates are loaded to table source_airport_details:
        | code | jetavator_deleted_ind |
        | ATL  | 1                     |
      And we call the method .run(load_type="delta")

      Then the table vault.hub_airport exists
      And the table vault.sat_airport_details exists
      And the table vault.hub_airport contains 10 rows
      And the table vault.sat_airport_details contains 11 rows
      And the loading datetime in the table sat_airport_details contains 2 distinct datetimes with code ATL
      And the view vault_now.airport_details contains 9 rows:
        | code | name                                             |
        | PEK  | Beijing Capital International Airport            |
        | DXB  | Dubai International Airport                      |
        | HND  | Haneda Airport                                   |
        | LAX  | Los Angeles International Airport                |
        | ORD  | O'Hare International Airport                     |
        | LHR  | London Heathrow Airport                          |
        | HKG  | Hong Kong International Airport                  |
        | PVG  | Shanghai Pudong International Airport            |
        | CDG  | Paris-Charles de Gaulle Airport	                |
      And the view vault_updates_airport_details contains 1 rows:
        | code | jetavator_deleted_ind |
        | ATL  | 1                     |
