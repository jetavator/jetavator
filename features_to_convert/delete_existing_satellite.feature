@fixture.yaml
Feature: Drop existing satellite from existing database

  @setup
  Scenario: Drop existing satellite from existing database - Setup

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

    And a definition for a satellite:
      """
      name: airport_details_allcaps
      type: satellite
      parent:
        name: airport
        type: hub
      columns:
        "allcaps_name": {"type": "varchar(max)", "nullable": True}
      pipeline:
        type: sql
        key_columns:
          "airport": "hub_airport_key"
        dependencies:
          - type: satellite
            name: airport_details
        load_dt: "sat_load_dt"
        deleted_ind: "sat_deleted_ind"
        sql: |
          SELECT

            hub_airport_key,
            UPPER(name) AS allcaps_name,
            sat_load_dt,
            sat_deleted_ind

          FROM {{satellite.airport_details.updates}} AS airport_details
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

  @fixture.tempfolder
  @fixture.jetavator
  Scenario: Drop satellite immediately after deploy

    When all the definitions are saved to disk
     And we call the method .deploy(model_dir="{tempfolder}")
     And the test data is loaded to table source_airport_details
     And we call the method .run(load_type="delta")
     And we drop a satellite with .drop("satellite", "airport_details_allcaps")

    Then the view vault_now.airport_details_allcaps does not exist
     And the property .schema_registry.deployed.version returns "0.0.2"

  @fixture.tempfolder
  @fixture.jetavator
  Scenario: Remaining satellites still work after satellite is dropped

    When all the definitions are saved to disk
     And we call the method .deploy(model_dir="{tempfolder}")
     And we drop a satellite with .drop("satellite", "airport_details_allcaps")
     And the test data is loaded to table source_airport_details
     And we call the method .run(load_type="delta")

    Then the view vault_now.airport_details exists
     And the view vault_now.airport_details contains 10 rows

  @fixture.tempfolder
  @fixture.jetavator
  Scenario: Attempt to drop satellite that is referenced in dependencies

    When all the definitions are saved to disk
     And we call the method .deploy(model_dir="{tempfolder}")
     And the test data is loaded to table source_airport_details
     And we call the method .run(load_type="delta")
     And we try to drop a satellite with .drop("satellite", "airport_details")

    Then an exception is raised
     And the view vault_now.airport_details exists
     And the view vault_now.airport_details contains 10 rows
     And the view vault_now.airport_details_allcaps exists
     And the view vault_now.airport_details_allcaps contains 10 rows

  @fixture.tempfolder
  @fixture.jetavator
  Scenario: Drop existing satellite with fresh Client instance

    When all the definitions are saved to disk
     And we call the method .deploy(model_dir="{tempfolder}")
     And the test data is loaded to table source_airport_details
     And we call the method .run(load_type="delta")
     And we create a new Client instance with no model
     And we drop a satellite with .drop("satellite", "airport_details_allcaps")

    Then the view vault_now.airport_details_allcaps does not exist
     And the view vault_now.airport_details exists
     And the view vault_now.airport_details contains 10 rows
     And the property .schema_registry.deployed.version returns "0.0.2"

  @fixture.tempfolder
  @fixture.jetavator
  Scenario: Drop existing satellite by updating model on disk

    Given a definition for a project:
      """
      name: example
      type: project
      version: 0.0.1
      """

     When all the definitions are saved to disk
      And we call the method .deploy(model_dir="{tempfolder}")
      And the test data is loaded to table source_airport_details
      And we call the method .run(load_type="delta")
      And we write this new project definition to disk:
       """
       name: example
       type: project
       version: 0.0.2
       """
      And we delete the satellite definition airport_details_allcaps from disk
      And we call the method .update(load_full_history=False)

     Then the view vault_now.airport_details_allcaps does not exist
      And the view vault_now.airport_details exists
      And the view vault_now.airport_details contains 10 rows

  @cli
  @fixture.tempfolder
  @fixture.jetavator
  Scenario: Drop existing satellite using the CLI

   When all the definitions are saved to disk
    And we run the CLI command:
     """
     jetavator deploy --model-dir="{tempfolder}" --server="{server}" --username="{username}" --password="{password}" --db="{database_name}"
     """

   When we run the CLI command:
     """
     jetavator drop satellite airport_details_allcaps --server="{server}" --username="{username}" --password="{password}" --db="{database_name}"
     """

   Then the command returns error code 0
    And the view vault_now.airport_details_allcaps does not exist
    And the view vault_now.airport_details exists

  @cli
  @fixture.tempfolder
  @fixture.jetavator
  Scenario: Attempt to drop satellite that is referenced in dependencies using the CLI

    When all the definitions are saved to disk
     And we run the CLI command:
      """
      jetavator deploy --model-dir="{tempfolder}" --server="{server}" --username="{username}" --password="{password}" --db="{database_name}"
      """

    When we try to run the CLI command:
      """
      jetavator drop satellite airport_details --server="{server}" --username="{username}" --password="{password}" --db="{database_name}"
      """

    Then the command returns error code 1
     And the view vault_now.airport_details exists
     And the view vault_now.airport_details_allcaps exists

  @cli
  @fixture.tempfolder
  @fixture.jetavator
  Scenario: Drop existing satellite by updating whole model via CLI

    Given a definition for a project:
      """
      name: example
      type: project
      version: 0.0.1
      """

    When all the definitions are saved to disk
     And we run the CLI command:
      """
      jetavator deploy --model-dir="{tempfolder}" --server="{server}" --username="{username}" --password="{password}" --db="{database_name}"
      """

     And we write this new project definition to disk:
      """
      name: example
      type: project
      version: 0.0.2
      """
     And we delete the satellite definition airport_details_allcaps from disk

    When we run the CLI command:
      """
      jetavator update --model-dir="{tempfolder}" --server="{server}" --username="{username}" --password="{password}" --db="{database_name}"
      """

    Then the command returns error code 0
     And the view vault_now.airport_details_allcaps does not exist
     And the view vault_now.airport_details exists
