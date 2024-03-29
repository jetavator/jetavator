@fixture.tempfolder
@fixture.yaml
Feature: Drop existing database if exists

  @setup
  Scenario: Drop existing database if exists - Setup

    Given a random string in the environment variable $RANDOM_TEST_SCHEMA

    And a config file saved as config.yml

    And a definition for a project:
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
        "code": {"type": "CHAR(3)", "nullable": False, "pk": True}
        "name": {"type": "String(64)", "nullable": False}
      """

    And a definition for a source:
      """
      name: airport_pair_details
      type: source
      columns:
        "dep_airport": {"type": "CHAR(3)", "nullable": False, "pk": True}
        "arr_airport": {"type": "CHAR(3)", "nullable": False, "pk": True}
        "num_changes": {"type": "Integer", "nullable": False}
      """

    And a definition for a hub:
      """
      name: airport
      type: hub
      key_type: "CHAR(3)"
      """

    And a definition for a link:
      """
      name: airport_pair
      type: link
      link_hubs:
        dep_airport: airport
        arr_airport: airport
      """

    And a definition for a satellite:
      """
      name: airport_details
      type: satellite
      parent:
        name: airport
        type: hub
      columns:
        "name": {"type": "String(64)", "nullable": False}
      pipeline:
        type: source
        source: airport_details
        key_columns:
          "airport": "code"
      """

    And a definition for a satellite:
      """
      name: airport_details_allcaps
      type: satellite
      parent:
        name: airport
        type: hub
      columns:
        "allcaps_name": {"type": "String(64)", "nullable": True}
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

    And a definition for a satellite:
      """
      name: last_updated_airport
      type: satellite
      parent:
        name: airport
        type: hub
      columns:
        "last_updated_airport": {"type": "Boolean"}
      pipeline:
        type: sql
        key_columns:
          "airport": "hub_airport_key"
        dependencies:
          - type: satellite
            name: airport_details
            view: current
        load_dt: "sat_load_dt"
        deleted_ind: "sat_deleted_ind"
        sql: |
          SELECT COALESCE(updates.hub_airport_key,
                          now.hub_airport_key) AS hub_airport_key,
                 CASE WHEN updates.hub_airport_key IS NULL
                      THEN 0
                      ELSE 1
                    END AS last_updated_airport,
                 CURRENT_TIMESTAMP AS sat_load_dt,
                 COALESCE(updates.sat_deleted_ind,
                          now.sat_deleted_ind) AS sat_deleted_ind
            FROM {{satellite.airport_details.current}} AS now
                 FULL JOIN {{satellite.airport_details.updates}} AS updates
                        ON updates.hub_airport_key
                         = now.hub_airport_key
      """

    And a definition for a satellite:
      """
      name: airport_pair_details
      type: satellite
      parent:
        name: airport_pair
        type: link
      columns:
        "num_changes": {"type": "Integer", "nullable": False}
      pipeline:
        type: source
        source: airport_pair_details
      """

    And a definition for a satellite:
      """
      name: airport_pair_direct_flight
      type: satellite
      parent:
        name: airport_pair
        type: link
      columns:
        "direct_flight": {"type": "Boolean", "nullable": True}
      pipeline:
        type: sql
        key_columns:
          "dep_airport": "hub_dep_airport_key"
          "arr_airport": "hub_arr_airport_key"
        dependencies:
          - type: satellite
            name: airport_pair_details
        load_dt: "sat_load_dt"
        deleted_ind: "sat_deleted_ind"
        sql: |
          SELECT

            hub_dep_airport_key,
            hub_arr_airport_key,
            CASE
              WHEN num_changes = 0 THEN True
              WHEN num_changes > 0 THEN False
            END AS direct_flight,
            sat_load_dt,
            sat_deleted_ind

          FROM {{satellite.airport_pair_details.updates}} AS airport_pair_details
      """

    And a definition for a satellite:
      """
      name: last_updated_pair
      type: satellite
      parent:
        name: airport_pair
        type: link
      columns:
        "last_updated_pair": {"type": "Boolean"}
      pipeline:
        type: sql
        key_columns:
          "dep_airport": "hub_dep_airport_key"
          "arr_airport": "hub_arr_airport_key"
        dependencies:
          - type: satellite
            name: airport_pair_details
            view: current
        load_dt: "sat_load_dt"
        deleted_ind: "sat_deleted_ind"
        sql: |
          SELECT COALESCE(updates.hub_dep_airport_key,
                          now.hub_dep_airport_key) AS hub_dep_airport_key,
                 COALESCE(updates.hub_arr_airport_key,
                          now.hub_arr_airport_key) AS hub_arr_airport_key,
                 CASE WHEN updates.link_airport_pair_key IS NULL
                      THEN False
                      ELSE True
                    END AS last_updated_pair,
                 CURRENT_TIMESTAMP AS sat_load_dt,
                 COALESCE(updates.sat_deleted_ind,
                          now.sat_deleted_ind) AS sat_deleted_ind

           FROM {{satellite.airport_pair_details.current}} AS now
                FULL JOIN {{satellite.airport_pair_details.updates}} AS updates
                       ON updates.link_airport_pair_key
                        = now.link_airport_pair_key
      """

    And a CSV file airport_details.csv saved in a temporary folder:
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

    And a CSV file airport_pair_details.csv saved in a temporary folder:
      | dep_airport | arr_airport | num_changes |
      | ATL         | LHR         | 0           |
      | ATL         | ORD         | 0           |
      | ATL         | PVG         | 0           |
      | LHR         | DEN         | 0           |
      | ORD         | PVG         | 1           |
      | ORD         | LHR         | 1           |
      | ORD         | DEN         | 2           |

    And we run the CLI command:
      """
      jetavator config --config-file={tempfolder}/config.yml --set model_path={tempfolder}/definitions
      """

  @fixture.remove_database_after_scenario
  Scenario: Redeploy with -d

    When all the definitions are saved to disk
     And we run the CLI command "jetavator deploy"
     And we run the CLI command:
       """
       jetavator run delta \
         --csv airport_details="{tempfolder}/airport_details.csv" \
         --csv airport_pair_details="{tempfolder}/airport_pair_details.csv"
       """
     And we run the CLI command "jetavator deploy -d"

    Then the table star_dim_airport exists on the star datastore
     And the table star_dim_airport on the star datastore contains 0 rows
     And the table star_fact_airport_pair exists on the star datastore
     And the table star_fact_airport_pair on the star datastore contains 0 rows

  @fixture.remove_database_after_scenario
  Scenario: Redeploy with --drop-if-exists

    When all the definitions are saved to disk
     And we run the CLI command "jetavator deploy"
     And we run the CLI command:
       """
       jetavator run delta \
         --csv airport_details="{tempfolder}/airport_details.csv" \
         --csv airport_pair_details="{tempfolder}/airport_pair_details.csv"
       """
     And we run the CLI command "jetavator deploy --drop-if-exists"

    Then the table star_dim_airport exists on the star datastore
     And the table star_dim_airport on the star datastore contains 0 rows
     And the table star_fact_airport_pair exists on the star datastore
     And the table star_fact_airport_pair on the star datastore contains 0 rows
