@fixture.yaml
Feature: Add new satellite to existing database

  @setup
  Scenario: Add new satellite to existing database - Setup

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

    And a definition for a new satellite that we will deploy later:
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

  @fixture.tempfolder
  @fixture.jetavator
  Scenario: Add new satellite immediately after deploy

    When all the definitions are saved to disk
     And we call the method .deploy(model_dir="{tempfolder}")
     And the test data is loaded to table source_airport_details
     And we call the method .run(load_type="delta")
     And we add a new satellite with .add(yaml_object, load_full_history=True)

   Then the table vault.sat_airport_details_allcaps exists
    And the table vault.sat_airport_details_allcaps contains 10 rows
    And the property .schema_registry.deployed.version returns "0.0.2"

  @fixture.tempfolder
  @fixture.jetavator
  Scenario: Add new satellite with fresh Client instance

   When all the definitions are saved to disk
    And we call the method .deploy(model_dir="{tempfolder}")
    And the test data is loaded to table source_airport_details
    And we call the method .run(load_type="delta")
    And we create a new Client instance with no model
    And we add a new satellite with .add(yaml_object, load_full_history=True)

   Then the table vault.sat_airport_details_allcaps exists
    And the table vault.sat_airport_details_allcaps contains 10 rows
    And the property .schema_registry.deployed.version returns "0.0.2"

  @fixture.tempfolder
  @fixture.jetavator
  Scenario: Add new satellite with fresh Client instance

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
     And we write a new satellite definition to disk
     And we reload the model in Client with .update_model_from_dir()
     And the definitions are updated with .update_database_model(load_full_history=True)

    Then the table vault.sat_airport_details_allcaps exists
     And the table vault.sat_airport_details_allcaps contains 10 rows
     And the property .schema_registry.deployed.version returns "0.0.2"

  @fixture.tempfolder
  @fixture.jetavator
  Scenario: Test that delta load works properly after the new satellite has been added

    Given a definition for a project:
      """
      name: example
      type: project
      version: 0.0.1
      """

    When all the definitions are saved to disk
     And we call the method .deploy(model_dir="{tempfolder}")
     And we write this new project definition to disk:
       """
       name: example
       type: project
       version: 0.0.2
       """
     And we write a new satellite definition to disk
     And we reload the model in Client with .update_model_from_dir()
     And the definitions are updated with .update_database_model(load_full_history=False)
     And the test data is loaded to table source_airport_details
     And we call the method .run(load_type="delta")

    Then the table vault.sat_airport_details exists
     And the table vault.sat_airport_details contains 10 rows
     And the table vault.sat_airport_details_allcaps exists
     And the table vault.sat_airport_details_allcaps contains 10 rows

   @cli
   @fixture.tempfolder
   Scenario: Update whole model via CLI

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
      And we write a new satellite definition to disk

     When we run the CLI command:
       """
       jetavator update --model-dir="{tempfolder}" --server="{server}" --username="{username}" --password="{password}" --db="{database_name}"
       """

     Then the command returns error code 0

   @fixture.tempfolder
   @fixture.jetavator
   Scenario: Deployment fails on duplicate column name - add method

     Given a definition for a new satellite that we will deploy later:
       """
       name: airport_details_allcaps_invalid
       type: satellite
       parent:
         name: airport
         type: hub
       columns:
         "name": {"type": "varchar(max)", "nullable": True}
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

           FROM {{satellite.airport_details.updates}}
       """

     When all the definitions are saved to disk
      And we call the method .deploy(model_dir="{tempfolder}")
      And we try to add a new satellite with .add(yaml_object, load_full_history=True)

     Then an exception is raised

   @fixture.tempfolder
   @fixture.jetavator
   Scenario: Deployment fails on duplicate column name - update method

     When all the definitions are saved to disk
      And we call the method .deploy(model_dir="{tempfolder}")
      And we write this new satellite definition to disk:
        """
        name: airport_details_allcaps_invalid
        type: satellite
        parent:
          name: airport
          type: hub
        columns:
          "name": {"type": "varchar(max)", "nullable": True}
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

            FROM {{satellite.airport_details.updates}}
        """
      And we reload the model in Client with .update_model_from_dir()
      And we try to update the definitions with .update_database_model(load_full_history=False)

     Then an exception is raised
