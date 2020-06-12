@fixture.tempfolder
@fixture.yaml
Feature: Build fact from link

  @setup
  Scenario: Build fact from link - Setup

    Given a random string in the environment variable $RANDOM_TEST_SCHEMA

    And a file saved as config.yml:
      """
      services:
        spark:
          type: local_spark
      storage:
        source: spark
        vault: spark
        star: spark
        logs: azure_queue
      compute: spark
      secret_lookup: environment
      schema: $RANDOM_TEST_SCHEMA
      """

    And a definition for a project:
      """
      name: example
      type: project
      version: 0.0.1
      """

    And a definition for a source:
      """
      name: airport_pair_details
      type: source
      columns:
        "dep_airport": {"type": "char(3)", "nullable": False, "pk": True}
        "arr_airport": {"type": "char(3)", "nullable": False, "pk": True}
        "num_changes": {"type": "int", "nullable": False}
      """

    And a definition for a hub:
      """
      name: airport
      type: hub
      key_length: 3
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
      name: airport_pair_details
      type: satellite
      parent:
        name: airport_pair
        type: link
      columns:
        "num_changes": {"type": "int", "nullable": False}
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
        "direct_flight": {"type": "char(1)", "nullable": True}
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
              WHEN num_changes = 0 THEN 1
              WHEN num_changes > 0 THEN 0
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
        "last_updated_pair": {"type": "char(1)"}
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
                      THEN 0
                      ELSE 1
                    END AS last_updated_pair,
                 CURRENT_TIMESTAMP AS sat_load_dt,
                 COALESCE(updates.sat_deleted_ind,
                          now.sat_deleted_ind) AS sat_deleted_ind

           FROM {{satellite.airport_pair_details.current}} AS now
                FULL JOIN {{satellite.airport_pair_details.updates}} AS updates
                       ON updates.link_airport_pair_key
                        = now.link_airport_pair_key
      """

    And a CSV file airport_pair_details.csv saved in a temporary folder:
      | dep_airport | arr_airport | num_changes |
      | ATL         | LHR         | 0           |
      | ATL         | ORD         | 0           |
      | ATL         | PVG         | 0           |
      | LHR         | AMS         | 0           |
      | ORD         | PVG         | 1           |
      | ORD         | LHR         | 1           |
      | ORD         | AMS         | 2           |

    And we run the CLI command:
      """
      jetavator config --config-file={tempfolder}/config.yml --set model_path={tempfolder}/definitions
      """

  Scenario: Initial load

    When all the definitions are saved to disk
    And we run the CLI command "jetavator deploy -d"
    And we run the CLI command:
      """
      jetavator run delta \
        --csv airport_pair_details="{tempfolder}/airport_pair_details.csv"
      """

    Then the table star_fact_airport_pair exists on the star datastore
     And the table star_fact_airport_pair on the star datastore contains 7 rows
     And the table star_fact_airport_pair on the star datastore contains these columns with this data:
       | hub_dep_airport_key | hub_arr_airport_key | num_changes | direct_flight | last_updated_pair |
       | ATL                 | LHR                 | 0           | 1             | 1                 |
       | ATL                 | ORD                 | 0           | 1             | 1                 |
       | ATL                 | PVG                 | 0           | 1             | 1                 |
       | LHR                 | AMS                 | 0           | 1             | 1                 |
       | ORD                 | PVG                 | 1           | 0             | 1                 |
       | ORD                 | LHR                 | 1           | 0             | 1                 |
       | ORD                 | AMS                 | 2           | 0             | 1                 |

  Scenario: Delta load - Update

    When all the definitions are saved to disk
    And we run the CLI command "jetavator deploy -d"
    And we run the CLI command:
      """
      jetavator run delta \
        --csv airport_pair_details="{tempfolder}/airport_pair_details.csv"
      """
    And the following CSV file airport_pair_details_update.csv is saved in the temporary folder:
       | dep_airport | arr_airport | num_changes |
       | ORD         | PVG         | 0           |
    And we run the CLI command:
      """
      jetavator run delta \
        --csv airport_pair_details="{tempfolder}/airport_pair_details_update.csv"
      """

    Then the table star_fact_airport_pair exists on the star datastore
     And the table star_fact_airport_pair on the star datastore contains 7 rows
     And the table star_fact_airport_pair on the star datastore contains these columns with this data:
       | hub_dep_airport_key | hub_arr_airport_key | num_changes | direct_flight | last_updated_pair |
       | ATL                 | LHR                 | 0           | 1             | 0                 |
       | ATL                 | ORD                 | 0           | 1             | 0                 |
       | ATL                 | PVG                 | 0           | 1             | 0                 |
       | LHR                 | AMS                 | 0           | 1             | 0                 |
       | ORD                 | PVG                 | 0           | 1             | 1                 |
       | ORD                 | LHR                 | 1           | 0             | 0                 |
       | ORD                 | AMS                 | 2           | 0             | 0                 |

   Scenario: Delta load - Insert

      When all the definitions are saved to disk
      And we run the CLI command "jetavator deploy -d"
      And we run the CLI command:
        """
        jetavator run delta \
          --csv airport_pair_details="{tempfolder}/airport_pair_details.csv"
        """
      And the following CSV file airport_pair_details_insert.csv is saved in the temporary folder:
        | dep_airport | arr_airport | num_changes |
        | ATL         | CDG         | 0           |
        | ORD         | CDG         | 1           |
     And we run the CLI command:
       """
       jetavator run delta \
         --csv airport_pair_details="{tempfolder}/airport_pair_details_insert.csv"
       """

     Then the table star_fact_airport_pair exists on the star datastore
      And the table star_fact_airport_pair on the star datastore contains 9 rows
      And the table star_fact_airport_pair on the star datastore contains these columns with this data:
        | hub_dep_airport_key | hub_arr_airport_key | num_changes | direct_flight | last_updated_pair |
        | ATL                 | LHR                 | 0           | 1             | 0                 |
        | ATL                 | ORD                 | 0           | 1             | 0                 |
        | ATL                 | PVG                 | 0           | 1             | 0                 |
        | LHR                 | AMS                 | 0           | 1             | 0                 |
        | ORD                 | PVG                 | 1           | 0             | 0                 |
        | ORD                 | LHR                 | 1           | 0             | 0                 |
        | ORD                 | AMS                 | 2           | 0             | 0                 |
        | ATL                 | CDG                 | 0           | 1             | 1                 |
        | ORD                 | CDG                 | 1           | 0             | 1                 |

  Scenario: Delta load - Delete

     When all the definitions are saved to disk
     And we run the CLI command "jetavator deploy -d"
     And we run the CLI command:
       """
       jetavator run delta \
         --csv airport_pair_details="{tempfolder}/airport_pair_details.csv"
       """
     And the following CSV file airport_pair_details_delete.csv is saved in the temporary folder:
       | dep_airport | arr_airport | jetavator_deleted_ind |
       | ORD         | AMS         | 1                     |
    And we run the CLI command:
      """
      jetavator run delta \
        --csv airport_pair_details="{tempfolder}/airport_pair_details_delete.csv"
      """

    Then the table star_fact_airport_pair exists on the star datastore
     And the table star_fact_airport_pair on the star datastore contains 6 rows
     And the table star_fact_airport_pair on the star datastore contains these columns with this data:
       | hub_dep_airport_key | hub_arr_airport_key | num_changes | direct_flight | last_updated_pair |
       | ATL                 | LHR                 | 0           | 1             | 0                 |
       | ATL                 | ORD                 | 0           | 1             | 0                 |
       | ATL                 | PVG                 | 0           | 1             | 0                 |
       | LHR                 | AMS                 | 0           | 1             | 0                 |
       | ORD                 | PVG                 | 1           | 0             | 0                 |
       | ORD                 | LHR                 | 1           | 0             | 0                 |

   Scenario: Allow users to exclude links from the star schema

     Given a definition for a link:
       """
       name: airport_pair
       type: link
       link_hubs:
         dep_airport: airport
         arr_airport: airport
       exclude_from_star_schema: True
       """

     When all the definitions are saved to disk
      And we run the CLI command "jetavator deploy -d"

     Then the table star_fact_airport_pair does not exist on the star datastore

   Scenario: Allow users to exclude link satellites from the star schema

     Given a definition for a satellite:
       """
       name: airport_pair_details
       type: satellite
       parent:
         name: airport_pair
         type: link
       columns:
         "num_changes": {"type": "int", "nullable": False}
       pipeline:
         type: source
         source: airport_pair_details
       exclude_from_star_schema: True
       """

     And a definition for a link:
       """
       name: airport_pair
       type: link
       link_hubs:
          dep_airport: airport
          arr_airport: airport
       """

     When all the definitions are saved to disk
      And we run the CLI command "jetavator deploy -d"

     Then the column num_changes does not exist in the table star_fact_airport_pair on the star datastore
