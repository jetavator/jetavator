@fixture.tempfolder
@fixture.yaml
Feature: Build star schema from project

  @setup
  Scenario: Build star schema from project - Setup

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
        "jetavator_deleted_ind": {"type": "Integer", "nullable": False}
      deleted_indicator_column: jetavator_deleted_ind
      """

    And a definition for a source:
      """
      name: airport_pair_details
      type: source
      columns:
        "dep_airport": {"type": "CHAR(3)", "nullable": False, "pk": True}
        "arr_airport": {"type": "CHAR(3)", "nullable": False, "pk": True}
        "num_changes": {"type": "Integer", "nullable": False}
        "jetavator_deleted_ind": {"type": "Integer", "nullable": False}
      deleted_indicator_column: jetavator_deleted_ind
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
           WHERE (SELECT COUNT(*) FROM {{satellite.airport_details.updates}}) > 0
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
      | code | name                                             | jetavator_deleted_ind |
      | ATL  | Hartsfield-Jackson Atlanta International Airport | 0                     |
      | PEK  | Beijing Capital International Airport            | 0                     |
      | DXB  | Dubai International Airport                      | 0                     |
      | HND  | Haneda Airport                                   | 0                     |
      | LAX  | Los Angeles International Airport                | 0                     |
      | ORD  | Orchard Field                                    | 0                     |
      | LHR  | London Heathrow Airport                          | 0                     |
      | HKG  | Hong Kong International Airport                  | 0                     |
      | PVG  | Shanghai Pudong International Airport            | 0                     |
      | CDG  | Paris-Charles de Gaulle Airport	                | 0                     |

    And a CSV file airport_pair_details.csv saved in a temporary folder:
      | dep_airport | arr_airport | num_changes | jetavator_deleted_ind |
      | ATL         | LHR         | 0           | 0                     |
      | ATL         | ORD         | 0           | 0                     |
      | ATL         | PVG         | 0           | 0                     |
      | LHR         | DEN         | 0           | 0                     |
      | ORD         | PVG         | 1           | 0                     |
      | ORD         | LHR         | 1           | 0                     |
      | ORD         | DEN         | 2           | 0                     |

    And we run the CLI command:
      """
      jetavator config --config-file={tempfolder}/config.yml --set model_path={tempfolder}/definitions
      """

  @fixture.remove_database_after_scenario
  Scenario: Initial load

    When all the definitions are saved to disk
     And we run the CLI command "jetavator deploy -d"
     And we run the CLI command:
       """
       jetavator run \
         --csv airport_details="{tempfolder}/airport_details.csv" \
         --csv airport_pair_details="{tempfolder}/airport_pair_details.csv"
       """

    Then the table vault_hub_airport exists on the vault datastore
     And the table vault_hub_airport on the vault datastore contains 11 rows
     And the table vault_hub_airport on the vault datastore contains these columns with this data:
       | hub_airport_key |
       | ATL             |
       | PEK             |
       | DXB             |
       | HND             |
       | LAX             |
       | ORD             |
       | LHR             |
       | HKG             |
       | PVG             |
       | CDG             |
       | DEN             |
     And the table star_dim_airport exists on the star datastore
     And the table star_dim_airport on the star datastore contains 11 rows
     And the table star_dim_airport on the star datastore contains these columns with this data:
       | hub_airport_key | name                                             | allcaps_name                                     | last_updated_airport |
       | ATL             | Hartsfield-Jackson Atlanta International Airport | HARTSFIELD-JACKSON ATLANTA INTERNATIONAL AIRPORT | 1                    |
       | PEK             | Beijing Capital International Airport            | BEIJING CAPITAL INTERNATIONAL AIRPORT            | 1                    |
       | DXB             | Dubai International Airport                      | DUBAI INTERNATIONAL AIRPORT                      | 1                    |
       | HND             | Haneda Airport                                   | HANEDA AIRPORT                                   | 1                    |
       | LAX             | Los Angeles International Airport                | LOS ANGELES INTERNATIONAL AIRPORT                | 1                    |
       | ORD             | Orchard Field                                    | ORCHARD FIELD                                    | 1                    |
       | LHR             | London Heathrow Airport                          | LONDON HEATHROW AIRPORT                          | 1                    |
       | HKG             | Hong Kong International Airport                  | HONG KONG INTERNATIONAL AIRPORT                  | 1                    |
       | PVG             | Shanghai Pudong International Airport            | SHANGHAI PUDONG INTERNATIONAL AIRPORT            | 1                    |
       | CDG             | Paris-Charles de Gaulle Airport                  | PARIS-CHARLES DE GAULLE AIRPORT                  | 1                    |
       | DEN             | None                                             | None                                             | None                 |
     And the table vault_link_airport_pair exists on the vault datastore
     And the table vault_link_airport_pair on the vault datastore contains 7 rows
     And the table vault_link_airport_pair on the vault datastore contains these columns with this data:
       | hub_dep_airport_key | hub_arr_airport_key |
       | ATL                 | LHR                 |
       | ATL                 | ORD                 |
       | ATL                 | PVG                 |
       | LHR                 | DEN                 |
       | ORD                 | PVG                 |
       | ORD                 | LHR                 |
       | ORD                 | DEN                 |
     And the table star_fact_airport_pair exists on the star datastore
     And the table star_fact_airport_pair on the star datastore contains 7 rows
     And the table star_fact_airport_pair on the star datastore contains these columns with this data:
       | hub_dep_airport_key | hub_arr_airport_key | num_changes | direct_flight | last_updated_pair |
       | ATL                 | LHR                 | 0           | 1             | 1                 |
       | ATL                 | ORD                 | 0           | 1             | 1                 |
       | ATL                 | PVG                 | 0           | 1             | 1                 |
       | LHR                 | DEN                 | 0           | 1             | 1                 |
       | ORD                 | PVG                 | 1           | 0             | 1                 |
       | ORD                 | LHR                 | 1           | 0             | 1                 |
       | ORD                 | DEN                 | 2           | 0             | 1                 |

  @fixture.remove_database_after_scenario
  Scenario: Delta load - Update

    When all the definitions are saved to disk
     And we run the CLI command "jetavator deploy -d"
     And we run the CLI command:
       """
       jetavator run \
         --csv airport_details="{tempfolder}/airport_details.csv" \
         --csv airport_pair_details="{tempfolder}/airport_pair_details.csv"
       """
     And the following CSV file airport_details_update.csv is saved in the temporary folder:
       | code | name                                             | jetavator_deleted_ind |
       | ORD  | O'Hare International Airport                     | 0                     |
     And the following CSV file airport_pair_details_update.csv is saved in the temporary folder:
       | dep_airport | arr_airport | num_changes | jetavator_deleted_ind |
       | ORD         | PVG         | 0           | 0                     |
     And we run the CLI command:
       """
       jetavator run \
         --csv airport_details="{tempfolder}/airport_details_update.csv" \
         --csv airport_pair_details="{tempfolder}/airport_pair_details_update.csv"
       """

    Then the table vault_hub_airport exists on the vault datastore
     And the table vault_hub_airport on the vault datastore contains 11 rows
     And the table vault_hub_airport on the vault datastore contains these columns with this data:
       | hub_airport_key |
       | ATL             |
       | PEK             |
       | DXB             |
       | HND             |
       | LAX             |
       | ORD             |
       | LHR             |
       | HKG             |
       | PVG             |
       | CDG             |
       | DEN             |
     And the table star_dim_airport exists on the star datastore
     And the table star_dim_airport on the star datastore contains 11 rows
     And the table star_dim_airport on the star datastore contains these columns with this data:
       | hub_airport_key | name                                             | allcaps_name                                     | last_updated_airport |
       | ATL             | Hartsfield-Jackson Atlanta International Airport | HARTSFIELD-JACKSON ATLANTA INTERNATIONAL AIRPORT | 0                    |
       | PEK             | Beijing Capital International Airport            | BEIJING CAPITAL INTERNATIONAL AIRPORT            | 0                    |
       | DXB             | Dubai International Airport                      | DUBAI INTERNATIONAL AIRPORT                      | 0                    |
       | HND             | Haneda Airport                                   | HANEDA AIRPORT                                   | 0                    |
       | LAX             | Los Angeles International Airport                | LOS ANGELES INTERNATIONAL AIRPORT                | 0                    |
       | ORD             | O'Hare International Airport                     | O'HARE INTERNATIONAL AIRPORT                     | 1                    |
       | LHR             | London Heathrow Airport                          | LONDON HEATHROW AIRPORT                          | 0                    |
       | HKG             | Hong Kong International Airport                  | HONG KONG INTERNATIONAL AIRPORT                  | 0                    |
       | PVG             | Shanghai Pudong International Airport            | SHANGHAI PUDONG INTERNATIONAL AIRPORT            | 0                    |
       | CDG             | Paris-Charles de Gaulle Airport                  | PARIS-CHARLES DE GAULLE AIRPORT                  | 0                    |
       | DEN             | None                                             | None                                             | None                 |
     And the table vault_link_airport_pair exists on the vault datastore
     And the table vault_link_airport_pair on the vault datastore contains 7 rows
     And the table vault_link_airport_pair on the vault datastore contains these columns with this data:
       | hub_dep_airport_key | hub_arr_airport_key |
       | ATL                 | LHR                 |
       | ATL                 | ORD                 |
       | ATL                 | PVG                 |
       | LHR                 | DEN                 |
       | ORD                 | PVG                 |
       | ORD                 | LHR                 |
       | ORD                 | DEN                 |
     And the table star_fact_airport_pair exists on the star datastore
     And the table star_fact_airport_pair on the star datastore contains 7 rows
     And the table star_fact_airport_pair on the star datastore contains these columns with this data:
       | hub_dep_airport_key | hub_arr_airport_key | num_changes | direct_flight | last_updated_pair |
       | ATL                 | LHR                 | 0           | 1             | 0                 |
       | ATL                 | ORD                 | 0           | 1             | 0                 |
       | ATL                 | PVG                 | 0           | 1             | 0                 |
       | LHR                 | DEN                 | 0           | 1             | 0                 |
       | ORD                 | PVG                 | 0           | 1             | 1                 |
       | ORD                 | LHR                 | 1           | 0             | 0                 |
       | ORD                 | DEN                 | 2           | 0             | 0                 |

  @fixture.remove_database_after_scenario
  Scenario: Delta load - Insert

    When all the definitions are saved to disk
     And we run the CLI command "jetavator deploy -d"
     And we run the CLI command:
       """
       jetavator run \
         --csv airport_details="{tempfolder}/airport_details.csv" \
         --csv airport_pair_details="{tempfolder}/airport_pair_details.csv"
       """
     And the following CSV file airport_details_insert.csv is saved in the temporary folder:
       | code | name                                             | jetavator_deleted_ind |
       | AMS  | Amsterdam Airport Schiphol                       | 0                     |
     And the following CSV file airport_pair_details_insert.csv is saved in the temporary folder:
       | dep_airport | arr_airport | num_changes | jetavator_deleted_ind |
       | ATL         | CDG         | 0           | 0                     |
       | ORD         | CDG         | 1           | 0                     |
     And we run the CLI command:
       """
       jetavator run \
         --csv airport_details="{tempfolder}/airport_details_insert.csv" \
         --csv airport_pair_details="{tempfolder}/airport_pair_details_insert.csv"
       """

    Then the table vault_hub_airport exists on the vault datastore
     And the table vault_hub_airport on the vault datastore contains 12 rows
     And the table vault_hub_airport on the vault datastore contains these columns with this data:
       | hub_airport_key |
       | ATL             |
       | PEK             |
       | DXB             |
       | HND             |
       | LAX             |
       | ORD             |
       | LHR             |
       | HKG             |
       | PVG             |
       | CDG             |
       | DEN             |
       | AMS             |
     And the table star_dim_airport exists on the star datastore
     And the table star_dim_airport on the star datastore contains 12 rows
     And the table star_dim_airport on the star datastore contains these columns with this data:
       | hub_airport_key | name                                             | allcaps_name                                     | last_updated_airport |
       | ATL             | Hartsfield-Jackson Atlanta International Airport | HARTSFIELD-JACKSON ATLANTA INTERNATIONAL AIRPORT | 0                    |
       | PEK             | Beijing Capital International Airport            | BEIJING CAPITAL INTERNATIONAL AIRPORT            | 0                    |
       | DXB             | Dubai International Airport                      | DUBAI INTERNATIONAL AIRPORT                      | 0                    |
       | HND             | Haneda Airport                                   | HANEDA AIRPORT                                   | 0                    |
       | LAX             | Los Angeles International Airport                | LOS ANGELES INTERNATIONAL AIRPORT                | 0                    |
       | ORD             | Orchard Field                                    | ORCHARD FIELD                                    | 0                    |
       | LHR             | London Heathrow Airport                          | LONDON HEATHROW AIRPORT                          | 0                    |
       | HKG             | Hong Kong International Airport                  | HONG KONG INTERNATIONAL AIRPORT                  | 0                    |
       | PVG             | Shanghai Pudong International Airport            | SHANGHAI PUDONG INTERNATIONAL AIRPORT            | 0                    |
       | CDG             | Paris-Charles de Gaulle Airport                  | PARIS-CHARLES DE GAULLE AIRPORT                  | 0                    |
       | DEN             | None                                             | None                                             | None                 |
       | AMS             | Amsterdam Airport Schiphol                       | AMSTERDAM AIRPORT SCHIPHOL                       | 1                    |
     And the table vault_link_airport_pair exists on the vault datastore
     And the table vault_link_airport_pair on the vault datastore contains 9 rows
     And the table vault_link_airport_pair on the vault datastore contains these columns with this data:
       | hub_dep_airport_key | hub_arr_airport_key |
       | ATL                 | LHR                 |
       | ATL                 | ORD                 |
       | ATL                 | PVG                 |
       | LHR                 | DEN                 |
       | ORD                 | PVG                 |
       | ORD                 | LHR                 |
       | ORD                 | DEN                 |
       | ATL                 | CDG                 |
       | ORD                 | CDG                 |
     And the table star_fact_airport_pair exists on the star datastore
     And the table star_fact_airport_pair on the star datastore contains 9 rows
     And the table star_fact_airport_pair on the star datastore contains these columns with this data:
       | hub_dep_airport_key | hub_arr_airport_key | num_changes | direct_flight | last_updated_pair |
       | ATL                 | LHR                 | 0           | 1             | 0                 |
       | ATL                 | ORD                 | 0           | 1             | 0                 |
       | ATL                 | PVG                 | 0           | 1             | 0                 |
       | LHR                 | DEN                 | 0           | 1             | 0                 |
       | ORD                 | PVG                 | 1           | 0             | 0                 |
       | ORD                 | LHR                 | 1           | 0             | 0                 |
       | ORD                 | DEN                 | 2           | 0             | 0                 |
       | ATL                 | CDG                 | 0           | 1             | 1                 |
       | ORD                 | CDG                 | 1           | 0             | 1                 |

  @fixture.remove_database_after_scenario
  Scenario: Delta load - Delete

    When all the definitions are saved to disk
     And we run the CLI command "jetavator deploy -d"
     And we run the CLI command:
       """
       jetavator run \
         --csv airport_details="{tempfolder}/airport_details.csv" \
         --csv airport_pair_details="{tempfolder}/airport_pair_details.csv"
       """
     And the following CSV file airport_details_delete.csv is saved in the temporary folder:
       | code | name | jetavator_deleted_ind                            |
       | ATL  |      | 1                                                |
       | LAX  |      | 1                                                |
     And the following CSV file airport_pair_details_delete.csv is saved in the temporary folder:
       | dep_airport | arr_airport | num_changes | jetavator_deleted_ind |
       | ORD         | DEN         |             | 1                     |
     And we run the CLI command:
       """
       jetavator run \
         --csv airport_details="{tempfolder}/airport_details_delete.csv" \
         --csv airport_pair_details="{tempfolder}/airport_pair_details_delete.csv"
       """

    Then the table vault_hub_airport exists on the vault datastore
     And the table vault_hub_airport on the vault datastore contains 11 rows
     And the table vault_hub_airport on the vault datastore contains these columns with this data:
       | hub_airport_key |
       | ATL             |
       | PEK             |
       | DXB             |
       | HND             |
       | LAX             |
       | ORD             |
       | LHR             |
       | HKG             |
       | PVG             |
       | CDG             |
       | DEN             |
      And the table star_dim_airport exists on the star datastore
      And the table star_dim_airport on the star datastore contains 11 rows
      And the table star_dim_airport on the star datastore contains these columns with this data:
       | hub_airport_key | name                                             | allcaps_name                                     | last_updated_airport |
       | ATL             | None                                             | None                                             | None                 |
       | PEK             | Beijing Capital International Airport            | BEIJING CAPITAL INTERNATIONAL AIRPORT            | 0                    |
       | DXB             | Dubai International Airport                      | DUBAI INTERNATIONAL AIRPORT                      | 0                    |
       | HND             | Haneda Airport                                   | HANEDA AIRPORT                                   | 0                    |
       | LAX             | None                                             | None                                             | None                 |
       | ORD             | Orchard Field                                    | ORCHARD FIELD                                    | 0                    |
       | LHR             | London Heathrow Airport                          | LONDON HEATHROW AIRPORT                          | 0                    |
       | HKG             | Hong Kong International Airport                  | HONG KONG INTERNATIONAL AIRPORT                  | 0                    |
       | PVG             | Shanghai Pudong International Airport            | SHANGHAI PUDONG INTERNATIONAL AIRPORT            | 0                    |
       | CDG             | Paris-Charles de Gaulle Airport                  | PARIS-CHARLES DE GAULLE AIRPORT                  | 0                    |
       | DEN             | None                                             | None                                             | None                 |
     And the table vault_link_airport_pair exists on the vault datastore
     And the table vault_link_airport_pair on the vault datastore contains 7 rows
     And the table vault_link_airport_pair on the vault datastore contains these columns with this data:
       | hub_dep_airport_key | hub_arr_airport_key |
       | ATL                 | LHR                 |
       | ATL                 | ORD                 |
       | ATL                 | PVG                 |
       | LHR                 | DEN                 |
       | ORD                 | PVG                 |
       | ORD                 | LHR                 |
       | ORD                 | DEN                 |
     And the table star_fact_airport_pair exists on the star datastore
     And the table star_fact_airport_pair on the star datastore contains 7 rows
     And the table star_fact_airport_pair on the star datastore contains these columns with this data:
       | hub_dep_airport_key | hub_arr_airport_key | num_changes | direct_flight | last_updated_pair |
       | ATL                 | LHR                 | 0           | 1             | 0                 |
       | ATL                 | ORD                 | 0           | 1             | 0                 |
       | ATL                 | PVG                 | 0           | 1             | 0                 |
       | LHR                 | DEN                 | 0           | 1             | 0                 |
       | ORD                 | PVG                 | 1           | 0             | 0                 |
       | ORD                 | LHR                 | 1           | 0             | 0                 |
       | ORD                 | DEN                 | None        | None          | None              |

  @fixture.remove_database_after_scenario
  Scenario: Delta load - All Together

    When all the definitions are saved to disk
     And we run the CLI command "jetavator deploy -d"
     And we run the CLI command:
       """
       jetavator run \
         --csv airport_details="{tempfolder}/airport_details.csv" \
         --csv airport_pair_details="{tempfolder}/airport_pair_details.csv"
       """
     And the following CSV file airport_details_update.csv is saved in the temporary folder:
       | code | name                                             | jetavator_deleted_ind |
       | ORD  | O'Hare International Airport                     | 0                     |
     And the following CSV file airport_details_insert.csv is saved in the temporary folder:
       | code | name                                             | jetavator_deleted_ind |
       | AMS  | Amsterdam Airport Schiphol                       | 0                     |
     And the following CSV file airport_details_delete.csv is saved in the temporary folder:
       | code | name                                             | jetavator_deleted_ind |
       | ATL  |                                                  | 1                     |
       | LAX  |                                                  | 1                     |
     And the following CSV file airport_pair_details_update.csv is saved in the temporary folder:
       | dep_airport | arr_airport | num_changes | jetavator_deleted_ind |
       | ORD         | PVG         | 0           | 0                     |
     And the following CSV file airport_pair_details_insert.csv is saved in the temporary folder:
       | dep_airport | arr_airport | num_changes | jetavator_deleted_ind |
       | ATL         | CDG         | 0           | 0                     |
       | ORD         | CDG         | 1           | 0                     |
     And the following CSV file airport_pair_details_delete.csv is saved in the temporary folder:
       | dep_airport | arr_airport | num_changes | jetavator_deleted_ind |
       | ORD         | DEN         |             | 1                     |
     And we run the CLI command:
       """
       jetavator run \
         --csv airport_details="{tempfolder}/airport_details_update.csv" \
         --csv airport_details="{tempfolder}/airport_details_insert.csv" \
         --csv airport_details="{tempfolder}/airport_details_delete.csv" \
         --csv airport_pair_details="{tempfolder}/airport_pair_details_update.csv" \
         --csv airport_pair_details="{tempfolder}/airport_pair_details_insert.csv" \
         --csv airport_pair_details="{tempfolder}/airport_pair_details_delete.csv"
       """

    Then the table vault_hub_airport exists on the vault datastore
     And the table vault_hub_airport on the vault datastore contains 12 rows
     And the table vault_hub_airport on the vault datastore contains these columns with this data:
       | hub_airport_key |
       | ATL             |
       | PEK             |
       | DXB             |
       | HND             |
       | LAX             |
       | ORD             |
       | LHR             |
       | HKG             |
       | PVG             |
       | CDG             |
       | DEN             |
       | AMS             |
     And the table star_dim_airport exists on the star datastore
     And the table star_dim_airport on the star datastore contains 12 rows
     And the table star_dim_airport on the star datastore contains these columns with this data:
       | hub_airport_key | name                                             | allcaps_name                                     | last_updated_airport |
       | ATL             | None                                             | None                                             | None                 |
       | PEK             | Beijing Capital International Airport            | BEIJING CAPITAL INTERNATIONAL AIRPORT            | 0                    |
       | DXB             | Dubai International Airport                      | DUBAI INTERNATIONAL AIRPORT                      | 0                    |
       | HND             | Haneda Airport                                   | HANEDA AIRPORT                                   | 0                    |
       | LAX             | None                                             | None                                             | None                 |
       | ORD             | O'Hare International Airport                     | O'HARE INTERNATIONAL AIRPORT                     | 1                    |
       | LHR             | London Heathrow Airport                          | LONDON HEATHROW AIRPORT                          | 0                    |
       | HKG             | Hong Kong International Airport                  | HONG KONG INTERNATIONAL AIRPORT                  | 0                    |
       | PVG             | Shanghai Pudong International Airport            | SHANGHAI PUDONG INTERNATIONAL AIRPORT            | 0                    |
       | CDG             | Paris-Charles de Gaulle Airport                  | PARIS-CHARLES DE GAULLE AIRPORT                  | 0                    |
       | DEN             | None                                             | None                                             | None                 |
       | AMS             | Amsterdam Airport Schiphol                       | AMSTERDAM AIRPORT SCHIPHOL                       | 1                    |
     And the table vault_link_airport_pair exists on the vault datastore
     And the table vault_link_airport_pair on the vault datastore contains 9 rows
     And the table vault_link_airport_pair on the vault datastore contains these columns with this data:
       | hub_dep_airport_key | hub_arr_airport_key |
       | ATL                 | LHR                 |
       | ATL                 | ORD                 |
       | ATL                 | PVG                 |
       | LHR                 | DEN                 |
       | ORD                 | PVG                 |
       | ORD                 | LHR                 |
       | ORD                 | DEN                 |
       | ATL                 | CDG                 |
       | ORD                 | CDG                 |
     And the table star_fact_airport_pair exists on the star datastore
     And the table star_fact_airport_pair on the star datastore contains 9 rows
     And the table star_fact_airport_pair on the star datastore contains these columns with this data:
       | hub_dep_airport_key | hub_arr_airport_key | num_changes | direct_flight | last_updated_pair |
       | ATL                 | LHR                 | 0           | 1             | 0                 |
       | ATL                 | ORD                 | 0           | 1             | 0                 |
       | ATL                 | PVG                 | 0           | 1             | 0                 |
       | LHR                 | DEN                 | 0           | 1             | 0                 |
       | ORD                 | PVG                 | 0           | 1             | 1                 |
       | ORD                 | LHR                 | 1           | 0             | 0                 |
       | ORD                 | DEN                 | None        | None          | None              |
       | ATL                 | CDG                 | 0           | 1             | 1                 |
       | ORD                 | CDG                 | 1           | 0             | 1                 |

  @fixture.remove_database_after_scenario
  Scenario: Delta load - All Individually

    When all the definitions are saved to disk
     And we run the CLI command "jetavator deploy -d"
     And we run the CLI command:
       """
       jetavator run \
         --csv airport_details="{tempfolder}/airport_details.csv"
       """
     And we run the CLI command:
       """
       jetavator run \
         --csv airport_pair_details="{tempfolder}/airport_pair_details.csv"
       """
     And the following CSV file airport_details_update.csv is saved in the temporary folder:
       | code | name                                             | jetavator_deleted_ind |
       | ORD  | O'Hare International Airport                     | 0                     |
     And the following CSV file airport_details_insert.csv is saved in the temporary folder:
       | code | name                                             | jetavator_deleted_ind |
       | AMS  | Amsterdam Airport Schiphol                       | 0                     |
     And the following CSV file airport_details_delete.csv is saved in the temporary folder:
       | code | name                                             | jetavator_deleted_ind |
       | ATL  |                                                  | 1                     |
       | LAX  |                                                  | 1                     |
     And the following CSV file airport_pair_details_update.csv is saved in the temporary folder:
       | dep_airport | arr_airport | num_changes | jetavator_deleted_ind |
       | ORD         | PVG         | 0           | 0                     |
     And the following CSV file airport_pair_details_insert.csv is saved in the temporary folder:
       | dep_airport | arr_airport | num_changes | jetavator_deleted_ind |
       | ATL         | CDG         | 0           | 0                     |
       | ORD         | CDG         | 1           | 0                     |
     And the following CSV file airport_pair_details_delete.csv is saved in the temporary folder:
       | dep_airport | arr_airport | num_changes | jetavator_deleted_ind |
       | ORD         | DEN         |             | 1                     |
     And we run the CLI command:
       """
       jetavator run \
         --csv airport_details="{tempfolder}/airport_details_update.csv"
       """
     And we run the CLI command:
       """
       jetavator run \
         --csv airport_details="{tempfolder}/airport_details_delete.csv"
       """
     And we run the CLI command:
       """
       jetavator run \
         --csv airport_details="{tempfolder}/airport_details_insert.csv"
       """
     And we run the CLI command:
       """
       jetavator run \
         --csv airport_pair_details="{tempfolder}/airport_pair_details_update.csv"
       """
     And we run the CLI command:
       """
       jetavator run \
         --csv airport_pair_details="{tempfolder}/airport_pair_details_delete.csv"
       """
     And we run the CLI command:
       """
       jetavator run \
         --csv airport_pair_details="{tempfolder}/airport_pair_details_insert.csv"
       """

    Then the table vault_hub_airport exists on the vault datastore
     And the table vault_hub_airport on the vault datastore contains 12 rows
     And the table vault_hub_airport on the vault datastore contains these columns with this data:
       | hub_airport_key |
       | ATL             |
       | PEK             |
       | DXB             |
       | HND             |
       | LAX             |
       | ORD             |
       | LHR             |
       | HKG             |
       | PVG             |
       | CDG             |
       | DEN             |
       | AMS             |
     And the table star_dim_airport exists on the star datastore
     And the table star_dim_airport on the star datastore contains 12 rows
     And the table star_dim_airport on the star datastore contains these columns with this data:
       | hub_airport_key | name                                             | allcaps_name                                     | last_updated_airport |
       | ATL             | None                                             | None                                             | None                 |
       | PEK             | Beijing Capital International Airport            | BEIJING CAPITAL INTERNATIONAL AIRPORT            | 0                    |
       | DXB             | Dubai International Airport                      | DUBAI INTERNATIONAL AIRPORT                      | 0                    |
       | HND             | Haneda Airport                                   | HANEDA AIRPORT                                   | 0                    |
       | LAX             | None                                             | None                                             | None                 |
       | ORD             | O'Hare International Airport                     | O'HARE INTERNATIONAL AIRPORT                     | 0                    |
       | LHR             | London Heathrow Airport                          | LONDON HEATHROW AIRPORT                          | 0                    |
       | HKG             | Hong Kong International Airport                  | HONG KONG INTERNATIONAL AIRPORT                  | 0                    |
       | PVG             | Shanghai Pudong International Airport            | SHANGHAI PUDONG INTERNATIONAL AIRPORT            | 0                    |
       | CDG             | Paris-Charles de Gaulle Airport                  | PARIS-CHARLES DE GAULLE AIRPORT                  | 0                    |
       | DEN             | None                                             | None                                             | None                 |
       | AMS             | Amsterdam Airport Schiphol                       | AMSTERDAM AIRPORT SCHIPHOL                       | 1                    |
     And the table vault_link_airport_pair exists on the vault datastore
     And the table vault_link_airport_pair on the vault datastore contains 9 rows
     And the table vault_link_airport_pair on the vault datastore contains these columns with this data:
       | hub_dep_airport_key | hub_arr_airport_key |
       | ATL                 | LHR                 |
       | ATL                 | ORD                 |
       | ATL                 | PVG                 |
       | LHR                 | DEN                 |
       | ORD                 | PVG                 |
       | ORD                 | LHR                 |
       | ORD                 | DEN                 |
       | ATL                 | CDG                 |
       | ORD                 | CDG                 |
     And the table star_fact_airport_pair exists on the star datastore
     And the table star_fact_airport_pair on the star datastore contains 9 rows
     And the table star_fact_airport_pair on the star datastore contains these columns with this data:
       | hub_dep_airport_key | hub_arr_airport_key | num_changes | direct_flight | last_updated_pair |
       | ATL                 | LHR                 | 0           | 1             | 0                 |
       | ATL                 | ORD                 | 0           | 1             | 0                 |
       | ATL                 | PVG                 | 0           | 1             | 0                 |
       | LHR                 | DEN                 | 0           | 1             | 0                 |
       | ORD                 | PVG                 | 0           | 1             | 0                 |
       | ORD                 | LHR                 | 1           | 0             | 0                 |
       | ORD                 | DEN                 | None        | None          | None              |
       | ATL                 | CDG                 | 0           | 1             | 1                 |
       | ORD                 | CDG                 | 1           | 0             | 1                 |

  @fixture.remove_database_after_scenario
  Scenario: Allow users to exclude hubs from the star schema

    Given a definition for a hub:
      """
      name: airport
      type: hub
      key_type: "CHAR(3)"
      exclude_from_star_schema: True
      """

    When all the definitions are saved to disk
     And we run the CLI command "jetavator deploy -d"

    Then the table star_dim_airport does not exist on the star datastore

  @fixture.remove_database_after_scenario
  Scenario: Allow users to exclude hub satellites from the star schema

    Given a definition for a satellite:
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
      exclude_from_star_schema: True
      """

    And a definition for a hub:
      """
      name: airport
      type: hub
      key_type: "CHAR(3)"
      """

    When all the definitions are saved to disk
     And we run the CLI command "jetavator deploy -d"

    Then the column name does not exist in the table star_dim_airport on the star datastore

  @fixture.remove_database_after_scenario
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
        "num_changes": {"type": "Integer", "nullable": False}
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


