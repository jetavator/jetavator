@fixture.tempfolder
@fixture.yaml
Feature: Define Hub Satellite

  @setup
  Scenario: Define Hub Satellite - Setup

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
  Scenario Outline: Define Hub Satellite from Source Table

    Given a definition for a satellite:
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

     When we call the method .deploy()
      And the test data is loaded to table source_airport_details
      And we call the method .run(load_type="<type>")

     Then the table vault.hub_airport exists
      And the table vault.sat_airport_details exists
      And the table vault.hub_airport contains <rows> rows
      And the table vault.sat_airport_details contains <rows> rows

      Examples: Load Types
        | type  | rows |
        | delta | 10   |
        | full  | 10   |

  @fixture.jetavator
  Scenario Outline: Define Hub Satellite from SQL

    Given a definition for a satellite:
      """
      name: airport_details
      type: satellite
      parent:
        name: airport
        type: hub
      columns:
        "name": {"type": "varchar(max)", "nullable": False}
      pipeline:
        type: sql
        key_columns:
          "airport": "code"
        dependencies:
          - type: source
            name: airport_details
        sql: |
          SELECT
            code,
            name
          FROM
            {{source.airport_details}} AS airport_details
      """

     When we call the method .deploy()
      And the test data is loaded to table source_airport_details
      And we call the method .run(load_type="<type>")

     Then the table vault.hub_airport exists
      And the table vault.sat_airport_details exists
      And the table vault.hub_airport contains <rows> rows
      And the table vault.sat_airport_details contains <rows> rows

      Examples: Load Types
        | type  | rows |
        | delta | 10   |
        | full  | 10   |


  @fixture.jetavator
  Scenario Outline: Define Hub Satellite from SQL, with specified load_dt and deleted_ind

    Given a definition for a satellite:
      """
      name: airport_details
      type: satellite
      parent:
        name: airport
        type: hub
      columns:
        "name": {"type": "varchar(max)", "nullable": False}
      pipeline:
        type: sql
        key_columns:
          "airport": "code"
        dependencies:
          - type: source
            name: airport_details
        load_dt: "jetavator_load_dt"
        deleted_ind: "jetavator_deleted_ind"
        sql: |
          SELECT
            code,
            name,
            jetavator_load_dt,
            jetavator_deleted_ind
          FROM
            {{source.airport_details}} AS airport_details
      """

     When we call the method .deploy()
      And the test data is loaded to table source_airport_details
      And we call the method .run(load_type="<type>")

     Then the table vault.hub_airport exists
      And the table vault.sat_airport_details exists
      And the table vault.hub_airport contains <rows> rows
      And the table vault.sat_airport_details contains <rows> rows

      Examples: Load Types
        | type  | rows |
        | delta | 10   |
        | full  | 10   |

    @fixture.jetavator
    Scenario: Deployment fails on duplicate column name

      Given a definition for a satellite:
        """
        name: airport_details
        type: satellite
        parent:
          name: airport
          type: hub
        columns:
          "name": {"type": "varchar(max)", "nullable": False}
        pipeline:
          type: sql
          key_columns:
            "airport": "code"
          dependencies:
            - type: source
              name: airport_details
          load_dt: "jetavator_load_dt"
          deleted_ind: "jetavator_deleted_ind"
          sql: |
            SELECT
              code,
              name,
              jetavator_load_dt,
              jetavator_deleted_ind
            FROM
              {{source.airport_details}} AS airport_details
        """

      And a definition for a satellite:
        """
        name: airport_details_duplicate
        type: satellite
        parent:
          name: airport
          type: hub
        columns:
          "name": {"type": "varchar(max)", "nullable": False}
        pipeline:
          type: sql
          key_columns:
            "airport": "code"
          dependencies:
            - type: source
              name: airport_details
          load_dt: "jetavator_load_dt"
          deleted_ind: "jetavator_deleted_ind"
          sql: |
            SELECT
              code,
              name,
              jetavator_load_dt,
              jetavator_deleted_ind
            FROM
              {{source.airport_details}} AS airport_details
        """

       When we try to call the method .deploy(model_dir="{tempfolder}")

       Then an exception is raised
